from comum.circuit_breaker import CircuitoAbertoError
from comum.erros import descrever
from comum.logs import criar_logger
from comum.oracle import Transacoes
from comum.tempo import Relogio, agora

from solicitacao.dominio import DadosNovaSolicitacao, Solicitacao, SolicitacaoNaoEncontradaError
from solicitacao.eventos import SolicitacaoCriada
from solicitacao.infra.publicadores import PublicadorSolicitacaoCriada
from solicitacao.infra.repositorio import RepositorioSolicitacao

log = criar_logger("SolicitacaoService")


class SolicitacaoService:
    """Grava a solicitação e publica SolicitacaoCriada.

    A gravação é confirmada antes da publicação para que a decisão nunca chegue antes de a solicitação existir no
    banco. Se a publicação falhar ou o circuit breaker kafka-producer estiver aberto, a solicitação fica
    PENDENTE_PUBLICACAO e o cliente recebe a mesma resposta.
    """

    def __init__(
        self,
        banco: Transacoes,
        repositorio: RepositorioSolicitacao,
        publicador: PublicadorSolicitacaoCriada,
        relogio: Relogio = agora,
    ) -> None:
        self._banco = banco
        self._repositorio = repositorio
        self._publicador = publicador
        self._relogio = relogio

    async def criar(self, dados: DadosNovaSolicitacao) -> Solicitacao:
        solicitacao = Solicitacao.nova(dados, self._relogio())
        await self._banco.em_transacao(lambda c: self._repositorio.inserir(c, solicitacao))
        try:
            await self._publicador.publicar(SolicitacaoCriada.de(solicitacao))
            log.info("Solicitação criada e publicada", solicitacao_id=solicitacao.id)
        except Exception as erro:
            if isinstance(erro, CircuitoAbertoError):
                log.warning(
                    "Circuit breaker kafka-producer aberto; solicitação ficará pendente de publicação",
                    solicitacao_id=solicitacao.id,
                )
            else:
                log.error(
                    "Falha ao publicar a solicitação; ficará pendente de publicação",
                    solicitacao_id=solicitacao.id,
                    erro=descrever(erro),
                )
            solicitacao.marcar_pendente_publicacao(self._relogio())
            await self._banco.em_transacao(lambda c: self._repositorio.atualizar(c, solicitacao))
        return solicitacao

    async def consultar(self, id: str) -> Solicitacao:
        solicitacao = await self._banco.em_transacao(lambda c: self._repositorio.buscar(c, id))
        if solicitacao is None:
            raise SolicitacaoNaoEncontradaError(id)
        return solicitacao
