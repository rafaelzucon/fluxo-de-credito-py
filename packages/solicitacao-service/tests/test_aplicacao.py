import json

import pytest
from apoio_solicitacao import (
    DADOS,
    PublicadorDecisaoFake,
    PublicadorSolicitacaoFake,
    RepositorioMemoria,
    abrir,
    solicitacao,
)
from comum.circuit_breaker import CircuitBreaker, CircuitoAbertoError
from comum.erros import MensagemInvalidaError
from comum.testes import INSTANTE, BancoFake, ConexaoFake, erro_unique
from solicitacao.aplicacao.processador_decisao import ProcessadorDecisao
from solicitacao.aplicacao.republicador import RepublicadorSolicitacoesPendentes
from solicitacao.aplicacao.solicitacao_service import SolicitacaoService
from solicitacao.dominio import Decisao, SolicitacaoNaoEncontradaError, StatusSolicitacao
from solicitacao.infra.idempotencia import RegistroIdempotenciaOracle

DECISAO = json.dumps(
    {
        "eventoId": "evt-1",
        "solicitacaoId": "sol-1",
        "decisao": "APROVADA",
        "motivo": "Cliente elegível",
        "decididoEm": "2025-01-02T03:04:06Z",
    }
)


# SolicitacaoService


async def test_grava_e_publica_solicitacao_criada_com_chave_igual_ao_id() -> None:
    repositorio, publicador = RepositorioMemoria(), PublicadorSolicitacaoFake()
    s = await SolicitacaoService(BancoFake(), repositorio, publicador, lambda: INSTANTE).criar(DADOS)
    [evento] = publicador.eventos
    assert (evento.solicitacao_id, evento.cliente_id, evento.valor_solicitado) == (
        s.id,
        "cliente-123",
        DADOS.valor_solicitado,
    )
    assert repositorio.solicitacoes[s.id].status is StatusSolicitacao.PENDENTE


@pytest.mark.parametrize("erro", [ConnectionError("kafka fora"), CircuitoAbertoError("kafka-producer")])
async def test_fica_pendente_de_publicacao_quando_o_kafka_falha_ou_o_circuito_esta_aberto(erro: Exception) -> None:
    repositorio = RepositorioMemoria()
    servico = SolicitacaoService(BancoFake(), repositorio, PublicadorSolicitacaoFake(erros=[erro]))
    s = await servico.criar(DADOS)
    assert s.status is StatusSolicitacao.PENDENTE_PUBLICACAO
    assert repositorio.solicitacoes[s.id].status is StatusSolicitacao.PENDENTE_PUBLICACAO


async def test_consulta_a_solicitacao_ou_falha_quando_nao_existe() -> None:
    repositorio = RepositorioMemoria({"sol-1": solicitacao()})
    servico = SolicitacaoService(BancoFake(), repositorio, PublicadorSolicitacaoFake())
    assert (await servico.consultar("sol-1")).id == "sol-1"
    with pytest.raises(SolicitacaoNaoEncontradaError):
        await servico.consultar("outra")


# ProcessadorDecisao


def processador(
    conexao: ConexaoFake | None = None, sns: PublicadorDecisaoFake | None = None
) -> tuple[ProcessadorDecisao, BancoFake, RepositorioMemoria, PublicadorDecisaoFake]:
    banco = BancoFake(conexao or ConexaoFake())
    repositorio = RepositorioMemoria({"sol-1": solicitacao()})
    sns = sns or PublicadorDecisaoFake()
    return (
        ProcessadorDecisao(banco, RegistroIdempotenciaOracle(), repositorio, sns, lambda: INSTANTE),
        banco,
        repositorio,
        sns,
    )


async def test_registra_a_chave_atualiza_o_status_e_publica_a_decisao_no_sns() -> None:
    p, banco, repositorio, sns = processador()
    await p.processar(DECISAO)
    assert repositorio.solicitacoes["sol-1"].status is StatusSolicitacao.APROVADA
    [d] = sns.decisoes
    assert (d.evento_id, d.email, d.token_dispositivo, d.decisao) == (
        "evt-1",
        "cliente@exemplo.com.br",
        "token-abc",
        Decisao.APROVADA,
    )
    assert banco.commits == 1
    assert "decisao-tomada:sol-1" in str(banco.conexao.comandos[0][1])  # type: ignore[attr-defined]


async def test_ignora_decisao_duplicada_sem_atualizar_nem_publicar() -> None:
    p, _, repositorio, sns = processador(ConexaoFake(erros={"chave_idempotencia": erro_unique()}))
    await p.processar(DECISAO)
    assert repositorio.atualizacoes == []
    assert sns.decisoes == []


@pytest.mark.parametrize("erro", [ConnectionError("sns fora"), CircuitoAbertoError("sns-publisher")])
async def test_faz_rollback_quando_o_sns_falha_para_liberar_a_chave(erro: Exception) -> None:
    p, banco, _, _ = processador(sns=PublicadorDecisaoFake(erro=erro))
    with pytest.raises(type(erro)):
        await p.processar(DECISAO)
    assert (banco.commits, banco.rollbacks) == (0, 1)


async def test_falha_quando_a_solicitacao_nao_existe() -> None:
    p, banco, _, _ = processador()
    with pytest.raises(SolicitacaoNaoEncontradaError):
        await p.processar(DECISAO.replace("sol-1", "outra"))
    assert banco.rollbacks == 1


@pytest.mark.parametrize(
    "payload",
    ["{nao é json", "[]", '{"eventoId":"e","solicitacaoId":"s","motivo":"m"}', DECISAO.replace("APROVADA", "TALVEZ")],
)
async def test_rejeita_payload_invalido_como_mensagem_invalida(payload: str) -> None:
    p, banco, _, _ = processador()
    with pytest.raises(MensagemInvalidaError):
        await p.processar(payload)
    assert banco.commits == banco.rollbacks == 0


# RepublicadorSolicitacoesPendentes


def republicador(
    publicador: PublicadorSolicitacaoFake, cb: CircuitBreaker | None = None
) -> tuple[RepublicadorSolicitacoesPendentes, RepositorioMemoria]:
    repositorio = RepositorioMemoria(
        {
            "a": solicitacao(id="a", status=StatusSolicitacao.PENDENTE_PUBLICACAO),
            "b": solicitacao(id="b", status=StatusSolicitacao.PENDENTE_PUBLICACAO),
            "c": solicitacao(id="c"),
        }
    )
    cb = cb or CircuitBreaker("kafka-producer")
    return RepublicadorSolicitacoesPendentes(BancoFake(), repositorio, publicador, cb, lambda: INSTANTE), repositorio


async def test_republica_as_pendentes_e_as_marca_como_pendentes_de_decisao() -> None:
    publicador = PublicadorSolicitacaoFake()
    r, repositorio = republicador(publicador)
    assert await r.republicar() == 2
    assert [e.solicitacao_id for e in publicador.eventos] == ["a", "b"]
    assert {s.status for s in repositorio.solicitacoes.values()} == {StatusSolicitacao.PENDENTE}


async def test_nao_republica_com_o_circuito_aberto() -> None:
    cb = CircuitBreaker("kafka-producer")
    await abrir(cb)
    publicador = PublicadorSolicitacaoFake()
    r, _ = republicador(publicador, cb)
    assert await r.republicar() == 0
    assert publicador.eventos == []


async def test_interrompe_o_lote_quando_o_circuito_abre_e_segue_quando_uma_publicacao_falha() -> None:
    r, repositorio = republicador(PublicadorSolicitacaoFake(erros=[CircuitoAbertoError("kafka-producer")]))
    assert await r.republicar() == 0
    assert repositorio.solicitacoes["b"].status is StatusSolicitacao.PENDENTE_PUBLICACAO

    r, repositorio = republicador(PublicadorSolicitacaoFake(erros=[ConnectionError("kafka fora")]))
    assert await r.republicar() == 1
    assert repositorio.solicitacoes["a"].status is StatusSolicitacao.PENDENTE_PUBLICACAO
    assert repositorio.solicitacoes["b"].status is StatusSolicitacao.PENDENTE
