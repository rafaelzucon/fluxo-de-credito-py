from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from comum.circuit_breaker import CircuitBreaker
from comum.config import ConfigCircuitBreaker, brokers
from comum.http import Saude, registrar_saude
from comum.kafka import ConsumidorComRetry, OpcoesConsumidorComRetry, ProdutorKafka, garantir_topicos, topicos_com_retry
from comum.oracle import BancoOracle, migrar
from comum.processo import rodar
from fastapi import FastAPI

from solicitacao.api import registrar_rotas
from solicitacao.aplicacao.processador_decisao import ProcessadorDecisao
from solicitacao.aplicacao.republicador import RepublicadorSolicitacoesPendentes
from solicitacao.aplicacao.solicitacao_service import SolicitacaoService
from solicitacao.config import Config
from solicitacao.infra.controle_consumidor import reagir_ao_circuit_breaker
from solicitacao.infra.idempotencia import RegistroIdempotenciaOracle
from solicitacao.infra.publicadores import PublicadorDecisaoSns, PublicadorSolicitacaoCriadaKafka
from solicitacao.infra.repositorio import RepositorioSolicitacaoOracle

MIGRACOES = Path(__file__).parent / "migrations"


def criar_aplicacao(config: Config | None = None) -> FastAPI:
    cfg = config or Config()
    saude = Saude()

    @asynccontextmanager
    async def ciclo_de_vida(app: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as pilha:
            banco = await BancoOracle.conectar(cfg.db_user, cfg.db_password, cfg.db_connect_string)
            pilha.push_async_callback(banco.fechar)
            saude.verificacoes["oracle"] = banco.saudavel
            await migrar(banco, MIGRACOES)

            lista_brokers = brokers(cfg.kafka_bootstrap_servers)
            await garantir_topicos(
                lista_brokers,
                [*topicos_com_retry(cfg.topico_solicitacao_criada), *topicos_com_retry(cfg.topico_decisao_tomada)],
            )

            cb_kafka = CircuitBreaker(
                "kafka-producer", ConfigCircuitBreaker(timeout_ms=cfg.kafka_timeout_publicacao_ms)
            )
            cb_sns = CircuitBreaker("sns-publisher")
            saude.circuit_breakers += [cb_kafka, cb_sns]
            pilha.callback(cb_kafka.desligar)
            pilha.callback(cb_sns.desligar)

            produtor = ProdutorKafka(lista_brokers, "solicitacao-service", cfg.kafka_timeout_publicacao_ms / 1000)
            pilha.push_async_callback(produtor.fechar)
            sns = await pilha.enter_async_context(
                get_session().create_client(
                    "sns",
                    region_name=cfg.aws_region,
                    endpoint_url=cfg.aws_endpoint,
                    config=AioConfig(connect_timeout=2, read_timeout=3, retries={"total_max_attempts": 1}),
                )
            )

            repositorio = RepositorioSolicitacaoOracle()
            publicador = PublicadorSolicitacaoCriadaKafka(produtor, cb_kafka, cfg.topico_solicitacao_criada)
            app.state.servico = SolicitacaoService(banco, repositorio, publicador)
            processador = ProcessadorDecisao(
                banco,
                RegistroIdempotenciaOracle(),
                repositorio,
                PublicadorDecisaoSns(sns, cb_sns, cfg.sns_topico_decisao_arn),
            )

            consumidor = ConsumidorComRetry(
                OpcoesConsumidorComRetry(
                    brokers=lista_brokers,
                    grupo=cfg.grupo,
                    topico=cfg.topico_decisao_tomada,
                    processar=processador.processar,
                    publicador=produtor,
                    tentativas=cfg.kafka_tentativas,
                    intervalo_retry_s=cfg.kafka_intervalo_retry_ms / 1000,
                    intervalo_circuito_aberto_s=cfg.kafka_intervalo_circuito_aberto_ms / 1000,
                )
            )
            cb_sns.ao_mudar_estado(reagir_ao_circuit_breaker(consumidor))
            consumidor.iniciar()
            pilha.push_async_callback(consumidor.parar)

            republicador = RepublicadorSolicitacoesPendentes(banco, repositorio, publicador, cb_kafka)
            republicador.agendar(cfg.republicacao_intervalo_ms / 1000)
            pilha.push_async_callback(republicador.parar)
            yield

    app = FastAPI(title="solicitacao-service", version="1.0.0", lifespan=ciclo_de_vida)
    registrar_saude(app, saude)
    registrar_rotas(app)
    return app


def main() -> None:
    rodar(criar_aplicacao(), porta_padrao=8080)


if __name__ == "__main__":
    main()
