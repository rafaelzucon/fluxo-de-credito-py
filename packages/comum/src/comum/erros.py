import json
from typing import Any


class MensagemInvalidaError(Exception):
    """Mensagem que nunca será processada com sucesso (JSON inválido, campos ausentes): não adianta tentar de novo."""


def ler_json(payload: str, tipo: str, campos_obrigatorios: tuple[str, ...]) -> dict[str, Any]:
    try:
        objeto = json.loads(payload)
    except (json.JSONDecodeError, TypeError) as erro:
        raise MensagemInvalidaError(f"{tipo} inválida: {payload}") from erro
    if not isinstance(objeto, dict):
        raise MensagemInvalidaError(f"{tipo} inválida: {payload}")
    ausentes = [c for c in campos_obrigatorios if objeto.get(c) is None]
    if ausentes:
        raise MensagemInvalidaError(f"{tipo} sem os campos {', '.join(ausentes)}: {payload}")
    return objeto


def descrever(erro: BaseException) -> str:
    """'NomeDoErro: mensagem' (equivalente ao `${erro.name}: ${erro.message}` do Node)."""
    return f"{type(erro).__name__}: {erro}"
