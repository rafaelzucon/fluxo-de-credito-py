import json
from decimal import Decimal

import pytest
from apoio_notificacao import DECISAO_JSON, decisao
from comum.erros import MensagemInvalidaError
from notificacao.dominio import DecisaoCredito, formatar_real, mensagem_cliente


@pytest.mark.parametrize(
    ("valor", "texto"),
    [("0.5", "R$ 0,50"), ("1234.5", "R$ 1.234,50"), ("1500000", "R$ 1.500.000,00"), ("10.1", "R$ 10,10")],
)
def test_formata_reais_no_padrao_brasileiro(valor: str, texto: str) -> None:
    assert formatar_real(Decimal(valor)) == texto


def test_mensagem_de_aprovacao() -> None:
    m = mensagem_cliente(decisao())
    assert m.titulo == "Sua solicitação de crédito foi aprovada"
    assert m.corpo == (
        "Boa notícia! Sua solicitação do produto CARTAO-GOLD no valor de R$ 1.234,50 foi aprovada. Protocolo: sol-1."
    )


def test_mensagem_de_negativa_com_o_motivo() -> None:
    m = mensagem_cliente(decisao(decisao="NEGADA", motivo="Score insuficiente"))
    assert m.titulo == "Resposta da sua solicitação de crédito"
    assert "não foi aprovada neste momento. Motivo: Score insuficiente. Protocolo: sol-1." in m.corpo


def test_le_o_valor_sem_perder_precisao() -> None:
    assert decisao(valorSolicitado=100.1).valor_solicitado == Decimal("100.1")


@pytest.mark.parametrize(
    "payload",
    [
        "nao-json",
        "42",
        json.dumps({k: v for k, v in DECISAO_JSON.items() if k != "email"}),
        json.dumps(DECISAO_JSON | {"decisao": "TALVEZ"}),
        json.dumps(DECISAO_JSON | {"valorSolicitado": "muito"}),
    ],
)
def test_rejeita_decisao_invalida(payload: str) -> None:
    with pytest.raises(MensagemInvalidaError):
        DecisaoCredito.ler(payload)
