import uuid
from datetime import datetime

from comum.erros import MensagemInvalidaError, ler_json
from comum.eventos import ModeloJson, Valor
from pydantic import ValidationError

from solicitacao.dominio import Decisao, Solicitacao


class SolicitacaoCriada(ModeloJson):
    """Evento publicado no tópico solicitacao-criada e consumido pelo motor de crédito."""

    evento_id: str
    solicitacao_id: str
    cliente_id: str
    produto_codigo: str
    valor_solicitado: Valor
    criado_em: datetime

    @classmethod
    def de(cls, s: Solicitacao) -> SolicitacaoCriada:
        return cls(
            evento_id=str(uuid.uuid4()),
            solicitacao_id=s.id,
            cliente_id=s.cliente_id,
            produto_codigo=s.produto_codigo,
            valor_solicitado=s.valor_solicitado,
            criado_em=s.criado_em,
        )


class DecisaoTomada(ModeloJson):
    """Evento publicado pelo motor de crédito no tópico decisao-tomada."""

    evento_id: str
    solicitacao_id: str
    decisao: Decisao
    motivo: str
    decidido_em: datetime | None = None

    @classmethod
    def ler(cls, payload: str) -> DecisaoTomada:
        dados = ler_json(payload, "DecisaoTomada", ("eventoId", "solicitacaoId", "decisao", "motivo"))
        try:
            return cls.model_validate(dados)
        except ValidationError as erro:
            raise MensagemInvalidaError(f"DecisaoTomada inválida ({erro.error_count()} erros): {payload}") from erro


class DecisaoCredito(ModeloJson):
    """Mensagem publicada no SNS e entregue às filas SQS de e-mail e push."""

    evento_id: str
    solicitacao_id: str
    cliente_id: str
    email: str
    token_dispositivo: str
    produto_codigo: str
    valor_solicitado: Valor
    decisao: Decisao
    motivo: str
    decidido_em: datetime | None

    @classmethod
    def de(cls, s: Solicitacao, d: DecisaoTomada) -> DecisaoCredito:
        return cls(
            evento_id=d.evento_id,
            solicitacao_id=s.id,
            cliente_id=s.cliente_id,
            email=s.email,
            token_dispositivo=s.token_dispositivo,
            produto_codigo=s.produto_codigo,
            valor_solicitado=s.valor_solicitado,
            decisao=d.decisao,
            motivo=d.motivo,
            decidido_em=d.decidido_em,
        )
