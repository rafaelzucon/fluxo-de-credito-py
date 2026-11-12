import json
from typing import Any

import pytest
from apoio_notificacao import DECISAO_JSON
from comum.circuit_breaker import CircuitoAbertoError, EstadoCircuito
from comum.erros import MensagemInvalidaError
from notificacao.dominio import Canal, DecisaoCredito
from notificacao.infra.consumidor_fila import ConsumidorFila, reagir_ao_circuit_breaker

URL = "http://localstack:4566/000000000000/fila-email"
MENSAGEM = {"MessageId": "m-1", "ReceiptHandle": "rh-1", "Body": json.dumps(DECISAO_JSON)}


class SqsFake:
    def __init__(self) -> None:
        self.chamadas: list[tuple[str, dict[str, Any]]] = []

    async def delete_message(self, **kwargs: Any) -> None:
        self.chamadas.append(("delete_message", kwargs))

    async def change_message_visibility(self, **kwargs: Any) -> None:
        self.chamadas.append(("change_message_visibility", kwargs))


class ProcessadorFake:
    def __init__(self, erro: BaseException | None = None) -> None:
        self.erro = erro
        self.processadas: list[tuple[Canal, DecisaoCredito]] = []

    async def processar(self, canal: Canal, decisao: DecisaoCredito) -> bool:
        if self.erro is not None:
            raise self.erro
        self.processadas.append((canal, decisao))
        return True


def criar(erro: BaseException | None = None) -> tuple[ConsumidorFila, SqsFake, ProcessadorFake]:
    sqs, processador = SqsFake(), ProcessadorFake(erro)
    return ConsumidorFila(Canal.EMAIL, URL, sqs, processador, 30), sqs, processador  # type: ignore[arg-type]


async def test_processa_e_apaga_a_mensagem() -> None:
    consumidor, sqs, processador = criar()
    await consumidor.tratar(MENSAGEM)
    assert [(c, d.solicitacao_id) for c, d in processador.processadas] == [(Canal.EMAIL, "sol-1")]
    assert sqs.chamadas == [("delete_message", {"QueueUrl": URL, "ReceiptHandle": "rh-1"})]


async def test_com_o_circuito_aberto_adia_a_reentrega_e_nao_apaga() -> None:
    consumidor, sqs, _ = criar(CircuitoAbertoError("email-provider"))
    with pytest.raises(CircuitoAbertoError):
        await consumidor.tratar(MENSAGEM)
    assert sqs.chamadas == [
        ("change_message_visibility", {"QueueUrl": URL, "ReceiptHandle": "rh-1", "VisibilityTimeout": 30})
    ]


async def test_outras_falhas_mantem_a_mensagem_na_fila_ate_o_visibility_timeout() -> None:
    consumidor, sqs, _ = criar(ConnectionError("banco fora"))
    with pytest.raises(ConnectionError):
        await consumidor.tratar(MENSAGEM)
    assert sqs.chamadas == []


async def test_mensagem_invalida_nao_e_apagada_e_segue_para_a_dlq_apos_3_recebimentos() -> None:
    consumidor, sqs, processador = criar()
    with pytest.raises(MensagemInvalidaError):
        await consumidor.tratar(MENSAGEM | {"Body": "{}"})
    assert (sqs.chamadas, processador.processadas) == ([], [])


def test_para_o_consumidor_com_o_circuito_aberto_e_reinicia_com_meio_aberto_ou_fechado() -> None:
    eventos: list[str] = []

    class Consumidor:
        def iniciar(self) -> None:
            eventos.append("iniciar")

        def parar(self) -> None:
            eventos.append("parar")

    reagir = reagir_ao_circuit_breaker(Consumidor())
    for estado in (EstadoCircuito.ABERTO, EstadoCircuito.MEIO_ABERTO, EstadoCircuito.FECHADO):
        reagir(estado)
    assert eventos == ["parar", "iniciar", "iniciar"]


async def test_parar_cancela_a_espera_do_long_polling_e_encerra_o_laco() -> None:
    import asyncio

    recebendo = asyncio.Event()

    class SqsLento(SqsFake):
        async def receive_message(self, **kwargs: Any) -> dict[str, Any]:
            recebendo.set()
            await asyncio.sleep(3600)
            return {}

    consumidor = ConsumidorFila(Canal.PUSH, URL, SqsLento(), ProcessadorFake(), 30)  # type: ignore[arg-type]
    consumidor.iniciar()
    await asyncio.wait_for(recebendo.wait(), 1)
    assert consumidor.rodando
    await asyncio.wait_for(consumidor.encerrar(), 1)
    assert not consumidor.rodando
