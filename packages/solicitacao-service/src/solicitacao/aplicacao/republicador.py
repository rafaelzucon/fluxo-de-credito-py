import asyncio
import contextlib

from comum.circuit_breaker import CircuitBreaker, CircuitoAbertoError, EstadoCircuito
from comum.erros import descrever
from comum.logs import criar_logger
from comum.oracle import Transacoes
from comum.tempo import Relogio, agora

from solicitacao.dominio import Solicitacao, StatusSolicitacao
from solicitacao.eventos import SolicitacaoCriada
from solicitacao.infra.publicadores import PublicadorSolicitacaoCriada
from solicitacao.infra.repositorio import RepositorioSolicitacao

log = criar_logger("RepublicadorSolicitacoesPendentes")

LOTE = 50


class RepublicadorSolicitacoesPendentes:
    """Fallback do circuit breaker kafka-producer: republica as solicitações PENDENTE_PUBLICACAO quando o circuito
    não está aberto. Uma publicação duplicada é absorvida pelas chaves de idempotência."""

    def __init__(
        self,
        banco: Transacoes,
        repositorio: RepositorioSolicitacao,
        publicador: PublicadorSolicitacaoCriada,
        circuit_breaker: CircuitBreaker,
        relogio: Relogio = agora,
    ) -> None:
        self._banco = banco
        self._repositorio = repositorio
        self._publicador = publicador
        self._cb = circuit_breaker
        self._relogio = relogio
        self._tarefa: asyncio.Task[None] | None = None

    async def republicar(self) -> int:
        """:returns: quantidade de solicitações republicadas."""
        if self._cb.estado is EstadoCircuito.ABERTO:
            log.debug("Circuit breaker kafka-producer aberto; republicação adiada")
            return 0
        pendentes = await self._banco.em_transacao(
            lambda c: self._repositorio.buscar_por_status(c, StatusSolicitacao.PENDENTE_PUBLICACAO, LOTE)
        )
        republicadas = 0
        for solicitacao in pendentes:
            try:
                await self._publicador.publicar(SolicitacaoCriada.de(solicitacao))
                solicitacao.marcar_publicada(self._relogio())
                await self._atualizar(solicitacao)
                republicadas += 1
                log.info("Solicitação republicada", solicitacao_id=solicitacao.id)
            except CircuitoAbertoError:
                log.warning("Circuit breaker kafka-producer abriu durante a republicação; retomando no próximo ciclo")
                break
            except Exception as erro:
                log.warning("Falha ao republicar a solicitação", solicitacao_id=solicitacao.id, erro=descrever(erro))
        return republicadas

    async def _atualizar(self, solicitacao: Solicitacao) -> None:
        await self._banco.em_transacao(lambda c: self._repositorio.atualizar(c, solicitacao))

    def agendar(self, intervalo_s: float) -> None:
        """Executa republicar() a cada intervalo, sem sobreposição (equivalente a @Scheduled(fixedDelay))."""

        async def ciclo() -> None:
            while True:
                await asyncio.sleep(intervalo_s)
                try:
                    await self.republicar()
                except Exception as erro:
                    log.warning("Falha no ciclo de republicação", erro=descrever(erro))

        self._tarefa = asyncio.create_task(ciclo())

    async def parar(self) -> None:
        if self._tarefa is not None:
            self._tarefa.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tarefa
            self._tarefa = None
