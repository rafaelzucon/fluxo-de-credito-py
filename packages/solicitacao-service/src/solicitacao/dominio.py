import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class StatusSolicitacao(StrEnum):
    PENDENTE = "PENDENTE"
    PENDENTE_PUBLICACAO = "PENDENTE_PUBLICACAO"
    APROVADA = "APROVADA"
    NEGADA = "NEGADA"


class Decisao(StrEnum):
    APROVADA = "APROVADA"
    NEGADA = "NEGADA"


@dataclass(frozen=True, slots=True)
class DadosNovaSolicitacao:
    cliente_id: str
    produto_codigo: str
    valor_solicitado: Decimal
    email: str
    token_dispositivo: str


@dataclass(slots=True, kw_only=True)
class Solicitacao:
    id: str
    cliente_id: str
    produto_codigo: str
    valor_solicitado: Decimal
    email: str
    token_dispositivo: str
    status: StatusSolicitacao
    motivo_decisao: str | None
    criado_em: datetime
    atualizado_em: datetime
    versao: int
    """Controle de concorrência otimista."""

    @classmethod
    def nova(cls, dados: DadosNovaSolicitacao, agora: datetime) -> Solicitacao:
        return cls(
            id=str(uuid.uuid4()),
            cliente_id=dados.cliente_id,
            produto_codigo=dados.produto_codigo,
            valor_solicitado=dados.valor_solicitado,
            email=dados.email,
            token_dispositivo=dados.token_dispositivo,
            status=StatusSolicitacao.PENDENTE,
            motivo_decisao=None,
            criado_em=agora,
            atualizado_em=agora,
            versao=0,
        )

    def marcar_pendente_publicacao(self, agora: datetime) -> None:
        """Kafka indisponível ou circuit breaker kafka-producer aberto: o republicador refará a publicação."""
        if self.status is StatusSolicitacao.PENDENTE:
            self.status = StatusSolicitacao.PENDENTE_PUBLICACAO
            self.atualizado_em = agora

    def marcar_publicada(self, agora: datetime) -> None:
        if self.status is StatusSolicitacao.PENDENTE_PUBLICACAO:
            self.status = StatusSolicitacao.PENDENTE
            self.atualizado_em = agora

    def registrar_decisao(self, decisao: Decisao, motivo: str, agora: datetime) -> None:
        self.status = StatusSolicitacao(decisao.value)
        self.motivo_decisao = motivo
        self.atualizado_em = agora


class SolicitacaoNaoEncontradaError(Exception):
    def __init__(self, id: str) -> None:
        super().__init__(f"Solicitação {id} não encontrada")
