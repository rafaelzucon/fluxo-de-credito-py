from comum.logs import criar_logger
from comum.oracle import Conexao, Transacoes
from comum.tempo import Relogio, agora

from solicitacao.dominio import SolicitacaoNaoEncontradaError
from solicitacao.eventos import DecisaoCredito, DecisaoTomada
from solicitacao.infra.idempotencia import RegistroIdempotencia
from solicitacao.infra.publicadores import PublicadorDecisao
from solicitacao.infra.repositorio import RepositorioSolicitacao

log = criar_logger("ProcessadorDecisao")


class ProcessadorDecisao:
    """Processa a DecisaoTomada numa única transação.

    Grava a chave de idempotência (UNIQUE), atualiza o status e publica no SNS pelo circuit breaker sns-publisher.
    Se a publicação falhar ou o circuito estiver aberto, o rollback libera a chave para a próxima tentativa.
    """

    def __init__(
        self,
        banco: Transacoes,
        idempotencia: RegistroIdempotencia,
        repositorio: RepositorioSolicitacao,
        sns: PublicadorDecisao,
        relogio: Relogio = agora,
    ) -> None:
        self._banco = banco
        self._idempotencia = idempotencia
        self._repositorio = repositorio
        self._sns = sns
        self._relogio = relogio

    async def processar(self, payload: str) -> None:
        """Recebe o payload do tópico decisao-tomada."""
        decisao = DecisaoTomada.ler(payload)

        async def trabalho(c: Conexao) -> None:
            if not await self._idempotencia.registrar(c, f"decisao-tomada:{decisao.solicitacao_id}"):
                log.info("Decisão já processada; mensagem duplicada ignorada", solicitacao_id=decisao.solicitacao_id)
                return
            solicitacao = await self._repositorio.buscar(c, decisao.solicitacao_id)
            if solicitacao is None:
                raise SolicitacaoNaoEncontradaError(decisao.solicitacao_id)
            solicitacao.registrar_decisao(decisao.decisao, decisao.motivo, self._relogio())
            await self._repositorio.atualizar(c, solicitacao)
            await self._sns.publicar(DecisaoCredito.de(solicitacao, decisao))
            log.info(
                "Solicitação atualizada e decisão publicada no SNS",
                solicitacao_id=solicitacao.id,
                status=solicitacao.status,
            )

        await self._banco.em_transacao(trabalho)
