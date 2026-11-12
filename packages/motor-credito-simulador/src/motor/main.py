from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager

from comum.config import brokers
from comum.http import Saude, registrar_saude
from comum.kafka import ConsumidorComRetry, OpcoesConsumidorComRetry, ProdutorKafka, garantir_topicos, topicos_com_retry
from comum.processo import rodar
from fastapi import FastAPI
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from motor.processador_solicitacao import criar_processador


class Config(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    porta: int = Field(default=8082, validation_alias="PORT")
    kafka_bootstrap_servers: str = "localhost:29092"
    topico_solicitacao_criada: str = "solicitacao-criada"
    topico_decisao_tomada: str = "decisao-tomada"
    kafka_intervalo_retry_ms: int = 5_000


def criar_aplicacao(config: Config | None = None) -> FastAPI:
    cfg = config or Config()

    @asynccontextmanager
    async def ciclo_de_vida(_: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as pilha:
            lista_brokers = brokers(cfg.kafka_bootstrap_servers)
            # Quem subir primeiro cria os tópicos; todos usam o mesmo número de partições.
            await garantir_topicos(
                lista_brokers,
                [*topicos_com_retry(cfg.topico_solicitacao_criada), *topicos_com_retry(cfg.topico_decisao_tomada)],
            )
            produtor = ProdutorKafka(lista_brokers, "motor-credito")
            pilha.push_async_callback(produtor.fechar)
            consumidor = ConsumidorComRetry(
                OpcoesConsumidorComRetry(
                    brokers=lista_brokers,
                    grupo="motor-credito",
                    topico=cfg.topico_solicitacao_criada,
                    processar=criar_processador(produtor, cfg.topico_decisao_tomada),
                    publicador=produtor,
                    tentativas=4,
                    intervalo_retry_s=cfg.kafka_intervalo_retry_ms / 1000,
                )
            )
            consumidor.iniciar()
            pilha.push_async_callback(consumidor.parar)
            yield

    app = FastAPI(title="motor-credito-simulador", version="1.0.0", lifespan=ciclo_de_vida)
    registrar_saude(app, Saude())
    return app


def main() -> None:
    rodar(criar_aplicacao(), porta_padrao=8082)


if __name__ == "__main__":
    main()
