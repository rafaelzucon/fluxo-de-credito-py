import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Literal

from comum.erros import MensagemInvalidaError, ler_json
from comum.eventos import ModeloJson, Valor
from comum.tempo import Relogio, agora
from pydantic import ValidationError

from motor.fatores_elegibilidade import FatoresElegibilidade, consultar_fatores

SCORE_MINIMO = 500


class SolicitacaoCriada(ModeloJson):
    evento_id: str | None = None
    solicitacao_id: str
    cliente_id: str
    produto_codigo: str | None = None
    valor_solicitado: Valor
    criado_em: datetime | None = None

    @classmethod
    def ler(cls, payload: str) -> SolicitacaoCriada:
        dados = ler_json(payload, "SolicitacaoCriada", ("solicitacaoId", "clienteId", "valorSolicitado"))
        try:
            return cls.model_validate(dados)
        except ValidationError as erro:
            raise MensagemInvalidaError(f"SolicitacaoCriada inválida ({erro.error_count()} erros): {payload}") from erro


class DecisaoTomada(ModeloJson):
    evento_id: str
    solicitacao_id: str
    decisao: Literal["APROVADA", "NEGADA"]
    motivo: str
    decidido_em: datetime


def decidir(
    s: SolicitacaoCriada,
    fatores: Callable[[str], FatoresElegibilidade] = consultar_fatores,
    relogio: Relogio = agora,
) -> DecisaoTomada:
    """Regras de decisão do simulador do motor de crédito."""
    f = fatores(s.cliente_id)
    if f.restricao_cadastral:
        motivo = "Restrição cadastral"
    elif f.score < SCORE_MINIMO:
        motivo = "Score insuficiente"
    elif s.valor_solicitado > f.limite_disponivel:
        motivo = "Valor acima do limite disponível"
    else:
        return _decisao(s, "APROVADA", "Cliente elegível", relogio)
    return _decisao(s, "NEGADA", motivo, relogio)


def _decisao(
    s: SolicitacaoCriada, resultado: Literal["APROVADA", "NEGADA"], motivo: str, relogio: Relogio
) -> DecisaoTomada:
    return DecisaoTomada(
        evento_id=str(uuid.uuid4()),
        solicitacao_id=s.solicitacao_id,
        decisao=resultado,
        motivo=motivo,
        decidido_em=relogio(),
    )
