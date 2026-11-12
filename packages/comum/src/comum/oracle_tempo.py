"""Conversão de instantes para colunas TIMESTAMP WITH TIME ZONE sem depender do fuso da sessão."""

from datetime import UTC, datetime

FORMATO_ORACLE = """'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM'"""


def para_bind(instante: datetime) -> str:
    """Usar no SQL como TO_TIMESTAMP_TZ(:bind, FORMATO_ORACLE)."""
    return instante.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def coluna_utc(coluna: str) -> str:
    """Expressão SELECT que devolve a coluna em UTC (TIMESTAMP sem fuso)."""
    return f"SYS_EXTRACT_UTC({coluna}) AS {coluna}"


def de_coluna(valor: datetime) -> datetime:
    return valor.replace(tzinfo=UTC) if valor.tzinfo is None else valor.astimezone(UTC)
