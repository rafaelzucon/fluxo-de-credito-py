from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FatoresElegibilidade:
    score: int
    limite_disponivel: int
    restricao_cadastral: bool


def hash_code_java(texto: str) -> int:
    """Mesmo algoritmo de String.hashCode() do Java (sobre unidades UTF-16, com overflow de 32 bits), para que as
    versões Java, Node e Python da solução decidam igual."""
    dados = texto.encode("utf-16-be")
    h = 0
    for i in range(0, len(dados), 2):
        h = (31 * h + int.from_bytes(dados[i : i + 2])) & 0xFFFFFFFF
    return h - (1 << 32) if h >= 1 << 31 else h


def consultar_fatores(cliente_id: str) -> FatoresElegibilidade:
    """Simula a consulta aos fatores de elegibilidade. Convenções para testes manuais: clienteId que começa com
    "restrito" tem restrição cadastral; os demais têm score e limite derivados do hash."""
    h = abs(hash_code_java(cliente_id))
    return FatoresElegibilidade(
        score=300 + h % 701,
        limite_disponivel=1_000 + (h % 50) * 1_000,
        restricao_cadastral=cliente_id.lower().startswith("restrito"),
    )
