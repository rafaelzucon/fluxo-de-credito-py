import asyncio
import contextlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from comum.circuit_breaker import CircuitoAbertoError, EstadoCircuito
from comum.erros import descrever
from comum.logs import criar_logger

from notificacao.dominio import CIRCUIT_BREAKER_DO_CANAL, Canal, DecisaoCredito

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient

log = criar_logger("ConsumidorFila")

type Mensagem = dict[str, Any]


class Processador(Protocol):
    async def processar(self, canal: Canal, decisao: DecisaoCredito) -> bool: ...


class ConsumidorFila:
    """Consome a fila SQS de um canal (long polling).

    A mensagem só é apagada se o processamento terminar com sucesso. Em caso de falha ela reaparece quando o
    visibility timeout expira e, depois de 3 recebimentos, a redrive policy a move para a DLQ.
    """

    def __init__(
        self,
        canal: Canal,
        queue_url: str,
        sqs: SQSClient,
        processador: Processador,
        visibilidade_circuito_aberto_s: int,
        *,
        espera_receber_s: int = 10,
        espera_apos_erro_s: float = 5,
    ) -> None:
        self.canal = canal
        self._queue_url = queue_url
        self._sqs = sqs
        self._processador = processador
        self._visibilidade_circuito_aberto_s = visibilidade_circuito_aberto_s
        """Deve ser >= tempo em que o circuito fica aberto, para a mensagem não reaparecer com o circuito aberto."""
        self._espera_receber_s = espera_receber_s
        self._espera_apos_erro_s = espera_apos_erro_s
        self._rodando = False
        self._tarefa: asyncio.Task[None] | None = None
        self._recebendo: asyncio.Task[Any] | None = None

    @property
    def rodando(self) -> bool:
        return self._rodando

    async def tratar(self, mensagem: Mensagem) -> None:
        """Processa a mensagem e a apaga da fila. Qualquer erro mantém a mensagem na fila."""
        decisao = DecisaoCredito.ler(mensagem.get("Body", ""))
        try:
            await self._processador.processar(self.canal, decisao)
        except CircuitoAbertoError:
            # A mensagem fica invisível pelo tempo em que o circuito permanece aberto; o consumidor é parado e
            # reiniciado quando o circuito passar para MEIO_ABERTO (ver reagir_ao_circuit_breaker).
            await self._sqs.change_message_visibility(
                QueueUrl=self._queue_url,
                ReceiptHandle=mensagem["ReceiptHandle"],
                VisibilityTimeout=self._visibilidade_circuito_aberto_s,
            )
            log.warning(
                f"Circuit breaker {CIRCUIT_BREAKER_DO_CANAL[self.canal]} aberto; mensagem será reentregue em "
                f"{self._visibilidade_circuito_aberto_s}s",
                canal=self.canal,
                solicitacao_id=decisao.solicitacao_id,
            )
            raise
        await self._sqs.delete_message(QueueUrl=self._queue_url, ReceiptHandle=mensagem["ReceiptHandle"])

    def iniciar(self) -> None:
        if self._rodando:
            return
        self._rodando = True
        if self._tarefa is None or self._tarefa.done():
            self._tarefa = asyncio.create_task(self._consumir(), name=f"consumidor-sqs-{self.canal}")
        log.info("Consumidor SQS iniciado", canal=self.canal)

    def parar(self) -> None:
        """Para de buscar mensagens. A mensagem em processamento termina normalmente."""
        if not self._rodando:
            return
        self._rodando = False
        if self._recebendo is not None:
            self._recebendo.cancel()
        log.warning("Consumidor SQS parado", canal=self.canal)

    async def encerrar(self) -> None:
        self.parar()
        if self._tarefa is not None:
            with contextlib.suppress(asyncio.CancelledError, TimeoutError):
                await asyncio.wait_for(self._tarefa, timeout=10)

    async def _consumir(self) -> None:
        while self._rodando:
            self._recebendo = asyncio.create_task(
                self._sqs.receive_message(
                    QueueUrl=self._queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=self._espera_receber_s,
                    MessageSystemAttributeNames=["ApproximateReceiveCount"],
                )
            )
            try:
                resposta = await self._recebendo
            except asyncio.CancelledError:
                if self._rodando:
                    raise  # cancelamento externo (encerramento)
                return  # parar() cancelou a espera pela próxima mensagem
            except Exception as erro:
                log.error("Erro no consumidor SQS", canal=self.canal, erro=descrever(erro))
                await asyncio.sleep(self._espera_apos_erro_s)
                continue
            finally:
                self._recebendo = None
            for mensagem in resposta.get("Messages", []):
                await self._tratar_registrando(mensagem)

    async def _tratar_registrando(self, mensagem: Mensagem) -> None:
        try:
            await self.tratar(mensagem)
        except CircuitoAbertoError:
            pass  # já registrado em tratar()
        except Exception as erro:
            log.error(
                "Falha ao processar; a mensagem reaparecerá quando o visibility timeout expirar",
                canal=self.canal,
                message_id=mensagem.get("MessageId"),
                recebimentos=mensagem.get("Attributes", {}).get("ApproximateReceiveCount"),
                erro=descrever(erro),
            )


class Controlavel(Protocol):
    def iniciar(self) -> None: ...

    def parar(self) -> None: ...


def reagir_ao_circuit_breaker(consumidor: Controlavel) -> Callable[[EstadoCircuito], None]:
    """Para o consumidor SQS de um canal quando o circuit breaker do seu provedor abre e o reinicia quando o
    circuito passa para MEIO_ABERTO ou FECHADO. Cada canal é controlado de forma independente."""

    def reagir(estado: EstadoCircuito) -> None:
        if estado is EstadoCircuito.ABERTO:
            consumidor.parar()
        else:
            consumidor.iniciar()

    return reagir
