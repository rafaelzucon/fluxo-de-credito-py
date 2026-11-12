from decimal import Decimal

import pytest
from apoio_solicitacao import solicitacao
from comum.circuit_breaker import EstadoCircuito
from comum.testes import INSTANTE, ConexaoFake, erro_banco, erro_unique
from solicitacao.dominio import StatusSolicitacao
from solicitacao.infra.controle_consumidor import reagir_ao_circuit_breaker
from solicitacao.infra.idempotencia import RegistroIdempotenciaOracle
from solicitacao.infra.repositorio import ConflitoDeVersaoError, RepositorioSolicitacaoOracle


async def test_registro_devolve_true_para_chave_nova_e_false_quando_a_constraint_unique_e_violada() -> None:
    registro = RegistroIdempotenciaOracle()
    conexao = ConexaoFake()
    assert await registro.registrar(conexao, "decisao-tomada:s-1")
    assert conexao.comandos == [(RegistroIdempotenciaOracle.INSERT, {"chave": "decisao-tomada:s-1"})]

    conexao.erros["chave_idempotencia"] = erro_unique()
    assert not await registro.registrar(conexao, "decisao-tomada:s-1")


async def test_registro_propaga_outros_erros_do_banco() -> None:
    conexao = ConexaoFake(erros={"chave_idempotencia": erro_banco()})
    with pytest.raises(Exception, match="falha"):
        await RegistroIdempotenciaOracle().registrar(conexao, "decisao-tomada:s-1")


async def test_insere_com_os_instantes_em_utc() -> None:
    conexao = ConexaoFake()
    await RepositorioSolicitacaoOracle().inserir(conexao, solicitacao())
    [(sql, binds)] = conexao.comandos
    assert "TO_TIMESTAMP_TZ(:criado_em" in sql
    assert binds["criado_em"] == "2025-01-02T03:04:05.678000+00:00"
    assert (binds["status"], binds["valor_solicitado"], binds["versao"]) == ("PENDENTE", Decimal("1500.50"), 0)


async def test_atualiza_com_controle_otimista_e_incrementa_a_versao() -> None:
    conexao = ConexaoFake(linhas_afetadas=1)
    s = solicitacao(status=StatusSolicitacao.APROVADA, motivo_decisao="Cliente elegível", versao=3)
    await RepositorioSolicitacaoOracle().atualizar(conexao, s)
    [(sql, binds)] = conexao.comandos
    assert "WHERE id = :id AND versao = :versao" in sql
    assert (binds["versao"], binds["status"], binds["motivo_decisao"]) == (3, "APROVADA", "Cliente elegível")
    assert s.versao == 4


async def test_falha_com_conflito_de_versao_quando_nenhuma_linha_e_atualizada() -> None:
    s = solicitacao()
    with pytest.raises(ConflitoDeVersaoError, match="sol-1"):
        await RepositorioSolicitacaoOracle().atualizar(ConexaoFake(linhas_afetadas=0), s)
    assert s.versao == 0


async def test_converte_a_linha_do_banco_em_solicitacao() -> None:
    linha = {
        "id": "sol-1",
        "cliente_id": "cliente-123",
        "produto_codigo": "CARTAO-GOLD",
        "valor_solicitado": Decimal("1500.50"),
        "email": "cliente@exemplo.com.br",
        "token_dispositivo": "token-abc",
        "status": "NEGADA",
        "motivo_decisao": "Score insuficiente",
        "criado_em": INSTANTE.replace(tzinfo=None),
        "atualizado_em": INSTANTE.replace(tzinfo=None),
        "versao": Decimal(2),
    }
    conexao = ConexaoFake(consultas={"WHERE id = :id": [linha]})
    s = await RepositorioSolicitacaoOracle().buscar(conexao, "sol-1")
    assert s == solicitacao(status=StatusSolicitacao.NEGADA, motivo_decisao="Score insuficiente", versao=2)
    assert await RepositorioSolicitacaoOracle().buscar(ConexaoFake(), "outra") is None


async def test_busca_por_status_em_ordem_de_criacao_com_limite() -> None:
    conexao = ConexaoFake()
    assert (
        await RepositorioSolicitacaoOracle().buscar_por_status(conexao, StatusSolicitacao.PENDENTE_PUBLICACAO, 50) == []
    )
    [(sql, binds)] = conexao.comandos
    assert "ORDER BY criado_em FETCH FIRST :limite ROWS ONLY" in sql
    assert binds == {"status": "PENDENTE_PUBLICACAO", "limite": 50}


def test_pausa_com_o_circuito_aberto_e_retoma_com_meio_aberto_ou_fechado() -> None:
    eventos: list[str] = []

    class Consumidor:
        def pausar(self) -> None:
            eventos.append("pausar")

        def retomar(self) -> None:
            eventos.append("retomar")

    reagir = reagir_ao_circuit_breaker(Consumidor())
    for estado in (EstadoCircuito.ABERTO, EstadoCircuito.MEIO_ABERTO, EstadoCircuito.ABERTO, EstadoCircuito.FECHADO):
        reagir(estado)
    assert eventos == ["pausar", "retomar", "pausar", "retomar"]
