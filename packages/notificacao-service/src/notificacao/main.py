from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import httpx
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from comum.circuit_breaker import CircuitBreaker
from comum.http import Saude, registrar_saude
from comum.oracle import BancoOracle, migrar
from comum.processo import rodar
from fastapi import FastAPI

from notificacao.config import Config
from notificacao.dominio import CIRCUIT_BREAKER_DO_CANAL, Canal
from notificacao.infra.consumidor_fila import ConsumidorFila, reagir_ao_circuit_breaker
from notificacao.infra.enviadores import EnviadorEmail, EnviadorPush, smtp
from notificacao.infra.registro_envio import RegistroEnvioOracle
from notificacao.processador import ProcessadorNotificacao

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient

MIGRACOES = Path(__file__).parent / "migrations"


async def url_da_fila(sqs: SQSClient, fila: str, endpoint: str) -> str:
    """URL da fila apontando para o endpoint configurado (o LocalStack devolve *.localhost.localstack.cloud)."""
    url = urlsplit((await sqs.get_queue_url(QueueName=fila))["QueueUrl"])
    return endpoint.rstrip("/") + url.path


def criar_aplicacao(config: Config | None = None) -> FastAPI:
    cfg = config or Config()
    saude = Saude()

    @asynccontextmanager
    async def ciclo_de_vida(_: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as pilha:
            banco = await BancoOracle.conectar(cfg.db_user, cfg.db_password, cfg.db_connect_string)
            pilha.push_async_callback(banco.fechar)
            saude.verificacoes["oracle"] = banco.saudavel
            await migrar(banco, MIGRACOES)

            sqs = await pilha.enter_async_context(
                get_session().create_client(
                    "sqs",
                    region_name=cfg.aws_region,
                    endpoint_url=cfg.aws_endpoint,
                    config=AioConfig(connect_timeout=2, read_timeout=20, retries={"total_max_attempts": 2}),
                )
            )
            http = await pilha.enter_async_context(httpx.AsyncClient(timeout=cfg.push_timeout_ms / 1000))
            cb_email = CircuitBreaker(CIRCUIT_BREAKER_DO_CANAL[Canal.EMAIL])
            cb_push = CircuitBreaker(CIRCUIT_BREAKER_DO_CANAL[Canal.PUSH])
            saude.circuit_breakers += [cb_email, cb_push]
            pilha.callback(cb_email.desligar)
            pilha.callback(cb_push.desligar)

            processador = ProcessadorNotificacao(
                banco,
                RegistroEnvioOracle(),
                [
                    EnviadorEmail(smtp(cfg.smtp_host, cfg.smtp_port), cb_email, cfg.email_remetente),
                    EnviadorPush(cfg.push_url, cb_push, http),
                ],
            )
            for canal, fila, cb in ((Canal.EMAIL, cfg.fila_email, cb_email), (Canal.PUSH, cfg.fila_push, cb_push)):
                consumidor = ConsumidorFila(
                    canal,
                    await url_da_fila(sqs, fila, cfg.aws_endpoint),
                    sqs,
                    processador,
                    cfg.visibilidade_circuito_aberto_s,
                )
                cb.ao_mudar_estado(reagir_ao_circuit_breaker(consumidor))
                consumidor.iniciar()
                pilha.push_async_callback(consumidor.encerrar)
            yield

    app = FastAPI(title="notificacao-service", version="1.0.0", lifespan=ciclo_de_vida)
    registrar_saude(app, saude)
    return app


def main() -> None:
    rodar(criar_aplicacao(), porta_padrao=8081)


if __name__ == "__main__":
    main()
