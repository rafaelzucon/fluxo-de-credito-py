from collections.abc import Iterable
from typing import Protocol

from comum.logs import criar_logger
from comum.oracle import Conexao, Transacoes

from notificacao.dominio import Canal, DecisaoCredito
from notificacao.infra.registro_envio import RegistroEnvio

log = criar_logger("ProcessadorNotificacao")


class EnviadorNotificacao(Protocol):
    """Envia a decisão ao cliente por um canal. Implementações protegem a chamada ao provedor com um circuit breaker."""

    @property
    def canal(self) -> Canal: ...

    async def enviar(self, decisao: DecisaoCredito) -> None:
        """:raises CircuitoAbertoError: se o circuito do provedor estiver aberto."""
        ...


class ProcessadorNotificacao:
    """Envia a notificação numa única transação: grava a chave de idempotência (UNIQUE) e chama o provedor pelo
    circuit breaker do canal. Se o envio falhar ou o circuito estiver aberto, o rollback libera a chave para a
    próxima entrega da mensagem SQS."""

    def __init__(self, banco: Transacoes, registro: RegistroEnvio, enviadores: Iterable[EnviadorNotificacao]) -> None:
        self._banco = banco
        self._registro = registro
        self._enviadores = {e.canal: e for e in enviadores}

    async def processar(self, canal: Canal, decisao: DecisaoCredito) -> bool:
        """:returns: True se enviou; False se a mensagem era duplicada."""
        enviador = self._enviadores.get(canal)
        if enviador is None:
            raise LookupError(f"Nenhum enviador configurado para o canal {canal}")

        async def trabalho(c: Conexao) -> bool:
            if not await self._registro.registrar(c, canal, decisao.solicitacao_id):
                log.info(
                    "Notificação já enviada; mensagem duplicada ignorada",
                    canal=canal,
                    solicitacao_id=decisao.solicitacao_id,
                )
                return False
            await enviador.enviar(decisao)
            log.info("Notificação enviada", canal=canal, solicitacao_id=decisao.solicitacao_id, decisao=decisao.decisao)
            return True

        return await self._banco.em_transacao(trabalho)
