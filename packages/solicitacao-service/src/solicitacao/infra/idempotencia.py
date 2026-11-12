from typing import Protocol

from comum.oracle import Conexao, eh_violacao_unique


class RegistroIdempotencia(Protocol):
    async def registrar(self, c: Conexao, chave: str) -> bool:
        """:returns: True se a chave é nova; False se a mensagem já foi processada."""
        ...


class RegistroIdempotenciaOracle:
    """Grava a chave na tabela chave_idempotencia; a constraint UNIQUE impede processamento duplicado."""

    INSERT = "INSERT INTO chave_idempotencia (chave) VALUES (:chave)"

    async def registrar(self, c: Conexao, chave: str) -> bool:
        try:
            await c.executar(self.INSERT, {"chave": chave})
            return True
        except Exception as erro:
            if eh_violacao_unique(erro):
                return False
            raise
