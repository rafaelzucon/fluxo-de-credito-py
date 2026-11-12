import json
from email.message import EmailMessage

import httpx2
import pytest
from apoio_notificacao import decisao
from comum.circuit_breaker import CircuitBreaker, CircuitoAbertoError, EstadoCircuito
from notificacao.infra.enviadores import EnviadorEmail, EnviadorPush, FalhaProvedorPushError


async def test_envia_o_email_com_remetente_destinatario_e_texto_da_decisao() -> None:
    enviados: list[EmailMessage] = []

    async def smtp(m: EmailMessage) -> None:
        enviados.append(m)

    await EnviadorEmail(smtp, CircuitBreaker("email-provider"), "credito@banco.com.br").enviar(decisao())
    [m] = enviados
    assert (m["From"], m["To"], m["Subject"]) == (
        "credito@banco.com.br",
        "cliente@exemplo.com.br",
        "Sua solicitação de crédito foi aprovada",
    )
    assert "R$ 1.234,50" in m.get_content()


async def test_falhas_do_smtp_abrem_o_circuito_e_as_chamadas_seguintes_sao_rejeitadas() -> None:
    async def smtp(_: EmailMessage) -> None:
        raise ConnectionRefusedError("smtp fora")

    cb = CircuitBreaker("email-provider")
    enviador = EnviadorEmail(smtp, cb, "credito@banco.com.br")
    while cb.estado is EstadoCircuito.FECHADO:
        with pytest.raises(ConnectionRefusedError):
            await enviador.enviar(decisao())
    with pytest.raises(CircuitoAbertoError):
        await enviador.enviar(decisao())


def push(status: int) -> tuple[EnviadorPush, list[httpx2.Request], CircuitBreaker]:
    requisicoes: list[httpx2.Request] = []

    def responder(r: httpx2.Request) -> httpx2.Response:
        requisicoes.append(r)
        return httpx2.Response(status)

    cb = CircuitBreaker("push-provider")
    http = httpx2.AsyncClient(transport=httpx2.MockTransport(responder))
    return EnviadorPush("http://wiremock:8080/", cb, http), requisicoes, cb


async def test_envia_o_push_para_o_provedor() -> None:
    enviador, requisicoes, _ = push(200)
    await enviador.enviar(decisao(decisao="NEGADA", motivo="Score insuficiente"))
    [r] = requisicoes
    assert (r.method, str(r.url)) == ("POST", "http://wiremock:8080/v1/push")
    corpo = json.loads(r.content)
    assert corpo["token"] == "token-abc"
    assert corpo["titulo"] == "Resposta da sua solicitação de crédito"
    assert corpo["dados"] == {"solicitacaoId": "sol-1", "decisao": "NEGADA"}


async def test_respostas_de_erro_do_provedor_de_push_contam_como_falha_e_abrem_o_circuito() -> None:
    enviador, _, cb = push(503)
    while cb.estado is EstadoCircuito.FECHADO:
        with pytest.raises(FalhaProvedorPushError, match="HTTP 503"):
            await enviador.enviar(decisao())
    assert cb.estado is EstadoCircuito.ABERTO
