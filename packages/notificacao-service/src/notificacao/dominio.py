from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from comum.erros import MensagemInvalidaError, ler_json
from comum.eventos import ModeloJson, Valor
from pydantic import ValidationError


class Canal(StrEnum):
    EMAIL = "EMAIL"
    PUSH = "PUSH"


CIRCUIT_BREAKER_DO_CANAL: dict[Canal, str] = {Canal.EMAIL: "email-provider", Canal.PUSH: "push-provider"}


class DecisaoCredito(ModeloJson):
    """Mensagem publicada no SNS pelo serviço de solicitações e entregue às filas SQS de e-mail e push."""

    evento_id: str | None = None
    solicitacao_id: str
    cliente_id: str | None = None
    email: str
    token_dispositivo: str
    produto_codigo: str
    valor_solicitado: Valor
    decisao: Literal["APROVADA", "NEGADA"]
    motivo: str = ""
    decidido_em: datetime | None = None

    @classmethod
    def ler(cls, payload: str) -> DecisaoCredito:
        dados = ler_json(
            payload,
            "DecisaoCredito",
            ("solicitacaoId", "email", "tokenDispositivo", "produtoCodigo", "valorSolicitado", "decisao"),
        )
        try:
            return cls.model_validate(dados)
        except ValidationError as erro:
            raise MensagemInvalidaError(f"DecisaoCredito inválida ({erro.error_count()} erros): {payload}") from erro


@dataclass(frozen=True, slots=True)
class MensagemCliente:
    titulo: str
    corpo: str


def formatar_real(valor: Decimal) -> str:
    """R$ 1.234,50 (formato pt-BR, independente do locale do sistema)."""
    texto = f"{valor:,.2f}"
    return "R$ " + texto.replace(",", "_").replace(".", ",").replace("_", ".")


def mensagem_cliente(d: DecisaoCredito) -> MensagemCliente:
    """Texto enviado ao cliente por e-mail e push."""
    valor = formatar_real(d.valor_solicitado)
    if d.decisao == "APROVADA":
        return MensagemCliente(
            titulo="Sua solicitação de crédito foi aprovada",
            corpo=f"Boa notícia! Sua solicitação do produto {d.produto_codigo} no valor de {valor} foi aprovada. "
            f"Protocolo: {d.solicitacao_id}.",
        )
    return MensagemCliente(
        titulo="Resposta da sua solicitação de crédito",
        corpo=f"Sua solicitação do produto {d.produto_codigo} no valor de {valor} não foi aprovada neste momento. "
        f"Motivo: {d.motivo}. Protocolo: {d.solicitacao_id}.",
    )
