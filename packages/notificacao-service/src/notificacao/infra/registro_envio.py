from typing import Protocol

from comum.oracle import Conexao, eh_violacao_unique

from notificacao.dominio import Canal


class RegistroEnvio(Protocol):
    async def registrar(self, c: Conexao, canal: Canal, solicitacao_id: str) -> bool:
        """:returns: True se a chave é nova; False se o envio já foi feito."""
        ...


def chave_envio(canal: Canal, solicitacao_id: str) -> str:
    return f"{canal.value.lower()}:{solicitacao_id}"


class RegistroEnvioOracle:
    """Registra o envio na tabela envio_notificacao; a constraint UNIQUE da chave impede envio duplicado."""

    INSERT = "INSERT INTO envio_notificacao (chave, canal, solicitacao_id) VALUES (:chave, :canal, :solicitacao_id)"

    async def registrar(self, c: Conexao, canal: Canal, solicitacao_id: str) -> bool:
        try:
            await c.executar(
                self.INSERT,
                {"chave": chave_envio(canal, solicitacao_id), "canal": canal.value, "solicitacao_id": solicitacao_id},
            )
            return True
        except Exception as erro:
            if eh_violacao_unique(erro):
                return False
            raise
