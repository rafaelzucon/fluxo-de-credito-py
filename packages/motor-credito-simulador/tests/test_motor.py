import json
from decimal import Decimal

import pytest
from comum.erros import MensagemInvalidaError
from comum.testes import INSTANTE, PublicadorKafkaFake
from motor.fatores_elegibilidade import FatoresElegibilidade, consultar_fatores, hash_code_java
from motor.motor_decisao import SolicitacaoCriada, decidir
from motor.processador_solicitacao import criar_processador

SOLICITACAO = {"eventoId": "e", "solicitacaoId": "sol-1", "clienteId": "cliente-123", "valorSolicitado": 1500.5}


@pytest.mark.parametrize(
    ("texto", "hash"),
    [("", 0), ("abc", 96354), ("hello world", 1794106052), ("cliente-com-nome-bem-comprido", -2011136055)],
)
def test_hash_code_igual_ao_string_hash_code_do_java(texto: str, hash: int) -> None:
    assert hash_code_java(texto) == hash


def test_hash_code_usa_unidades_utf16_como_o_java() -> None:
    assert hash_code_java("ção") == 31 * (31 * 0xE7 + 0xE3) + ord("o")
    assert hash_code_java("😀") == 31 * 0xD83D + 0xDE00


def test_fatores_deterministicos_iguais_as_versoes_java_e_node() -> None:
    assert consultar_fatores("cliente-123") == FatoresElegibilidade(771, 34_000, False)
    assert consultar_fatores("maria.silva") == FatoresElegibilidade(677, 48_000, False)
    assert consultar_fatores("abc") == FatoresElegibilidade(617, 5_000, False)


def test_cliente_restrito_independe_de_maiusculas() -> None:
    assert consultar_fatores("RESTRITO-1").restricao_cadastral
    assert consultar_fatores("restrito-2").restricao_cadastral
    assert not consultar_fatores("cliente-restrito").restricao_cadastral


@pytest.mark.parametrize(
    ("fatores", "valor", "decisao", "motivo"),
    [
        (FatoresElegibilidade(900, 50_000, True), "100", "NEGADA", "Restrição cadastral"),
        (FatoresElegibilidade(499, 50_000, False), "100", "NEGADA", "Score insuficiente"),
        (FatoresElegibilidade(500, 1_000, False), "1000.01", "NEGADA", "Valor acima do limite disponível"),
        (FatoresElegibilidade(500, 1_000, False), "1000", "APROVADA", "Cliente elegível"),
    ],
)
def test_regras_de_decisao(fatores: FatoresElegibilidade, valor: str, decisao: str, motivo: str) -> None:
    s = SolicitacaoCriada.ler(json.dumps(SOLICITACAO | {"valorSolicitado": float(valor)}))
    assert s.valor_solicitado == Decimal(valor)
    d = decidir(s, lambda _: fatores, lambda: INSTANTE)
    assert (d.solicitacao_id, d.decisao, d.motivo, d.decidido_em) == ("sol-1", decisao, motivo, INSTANTE)


async def test_publica_a_decisao_com_a_chave_igual_ao_id_da_solicitacao() -> None:
    kafka = PublicadorKafkaFake()
    await criar_processador(kafka, "decisao-tomada")(json.dumps(SOLICITACAO))
    [p] = kafka.publicacoes
    assert (p.topico, p.chave) == ("decisao-tomada", "sol-1")
    corpo = json.loads(p.valor)
    assert (corpo["solicitacaoId"], corpo["decisao"], corpo["motivo"]) == ("sol-1", "APROVADA", "Cliente elegível")
    assert set(corpo) == {"eventoId", "solicitacaoId", "decisao", "motivo", "decididoEm"}


@pytest.mark.parametrize(
    "payload",
    ["{", "null", json.dumps({"solicitacaoId": "s"}), json.dumps(SOLICITACAO | {"valorSolicitado": "abc"})],
)
async def test_rejeita_solicitacao_criada_invalida_sem_publicar(payload: str) -> None:
    kafka = PublicadorKafkaFake()
    with pytest.raises(MensagemInvalidaError):
        await criar_processador(kafka, "decisao-tomada")(payload)
    assert kafka.publicacoes == []
