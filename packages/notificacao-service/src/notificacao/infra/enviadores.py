from collections.abc import Awaitable, Callable
from email.message import EmailMessage
from typing import Any

import aiosmtplib
import httpx
from comum.circuit_breaker import CircuitBreaker

from notificacao.dominio import Canal, DecisaoCredito, mensagem_cliente

CAMINHO_PUSH = "/v1/push"

type EnviarSmtp = Callable[[EmailMessage], Awaitable[Any]]


def smtp(host: str, porta: int, timeout_s: float = 3) -> EnviarSmtp:
    """Envio SMTP simples (sem TLS, como o Mailpit local)."""

    async def enviar(mensagem: EmailMessage) -> Any:
        return await aiosmtplib.send(
            mensagem, hostname=host, port=porta, timeout=timeout_s, use_tls=False, start_tls=False
        )

    return enviar


class EnviadorEmail:
    canal = Canal.EMAIL

    def __init__(self, enviar_smtp: EnviarSmtp, circuit_breaker: CircuitBreaker, remetente: str) -> None:
        self._smtp = enviar_smtp
        self._cb = circuit_breaker
        self._remetente = remetente

    async def enviar(self, decisao: DecisaoCredito) -> None:
        texto = mensagem_cliente(decisao)
        email = EmailMessage()
        email["From"] = self._remetente
        email["To"] = decisao.email
        email["Subject"] = texto.titulo
        email.set_content(texto.corpo)
        await self._cb.executar(lambda: self._smtp(email))


class FalhaProvedorPushError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"Provedor de push respondeu HTTP {status}")
        self.status = status


class EnviadorPush:
    canal = Canal.PUSH

    def __init__(self, url: str, circuit_breaker: CircuitBreaker, http: httpx.AsyncClient) -> None:
        self._url = url.rstrip("/") + CAMINHO_PUSH
        self._cb = circuit_breaker
        self._http = http

    async def enviar(self, decisao: DecisaoCredito) -> None:
        texto = mensagem_cliente(decisao)
        push = {
            "token": decisao.token_dispositivo,
            "titulo": texto.titulo,
            "mensagem": texto.corpo,
            "dados": {"solicitacaoId": decisao.solicitacao_id, "decisao": decisao.decisao},
        }

        async def enviar() -> None:
            resposta = await self._http.post(self._url, json=push)
            if not resposta.is_success:
                raise FalhaProvedorPushError(resposta.status_code)

        await self._cb.executar(enviar)
