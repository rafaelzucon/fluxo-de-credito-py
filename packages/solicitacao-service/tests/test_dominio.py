from comum.testes import INSTANTE, RelogioFake
from solicitacao.dominio import Decisao, Solicitacao, StatusSolicitacao

from .apoio import DADOS


def test_nasce_pendente_com_id_versao_0_e_sem_motivo() -> None:
    s = Solicitacao.nova(DADOS, INSTANTE)
    assert len(s.id) == 36
    assert (s.status, s.versao, s.motivo_decisao) == (StatusSolicitacao.PENDENTE, 0, None)
    assert s.criado_em == s.atualizado_em == INSTANTE
    assert s.valor_solicitado == DADOS.valor_solicitado


def test_so_vai_para_pendente_publicacao_a_partir_de_pendente() -> None:
    relogio = RelogioFake()
    s = Solicitacao.nova(DADOS, relogio())
    relogio.avancar(1)
    s.marcar_pendente_publicacao(relogio())
    assert (s.status, s.atualizado_em) == (StatusSolicitacao.PENDENTE_PUBLICACAO, relogio())

    aprovada = Solicitacao.nova(DADOS, INSTANTE)
    aprovada.registrar_decisao(Decisao.APROVADA, "Cliente elegível", INSTANTE)
    aprovada.marcar_pendente_publicacao(relogio())
    assert aprovada.status is StatusSolicitacao.APROVADA


def test_volta_para_pendente_ao_ser_republicada_sem_sobrescrever_uma_decisao() -> None:
    s = Solicitacao.nova(DADOS, INSTANTE)
    s.marcar_pendente_publicacao(INSTANTE)
    s.marcar_publicada(INSTANTE)
    assert s.status is StatusSolicitacao.PENDENTE

    s.registrar_decisao(Decisao.NEGADA, "Score insuficiente", INSTANTE)
    s.marcar_publicada(INSTANTE)
    assert s.status is StatusSolicitacao.NEGADA


def test_registra_a_decisao_e_o_motivo() -> None:
    s = Solicitacao.nova(DADOS, INSTANTE)
    s.registrar_decisao(Decisao.NEGADA, "Restrição cadastral", INSTANTE)
    assert (s.status, s.motivo_decisao) == (StatusSolicitacao.NEGADA, "Restrição cadastral")
