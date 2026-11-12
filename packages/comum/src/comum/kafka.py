"""Produtor e consumidor Kafka (aiokafka) com retry não bloqueante e dead letter topic."""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from comum.circuit_breaker import CircuitoAbertoError
from comum.erros import MensagemInvalidaError, descrever
from comum.logs import criar_logger
from comum.tempo import Esperar, Relogio, agora, epoch_ms, esperar

log = criar_logger("kafka")

SUFIXO_RETRY = "-retry"
SUFIXO_DLT = "-dlt"
HEADER_TENTATIVA = "tentativa"
HEADER_PROCESSAR_APOS = "processar-apos"
HEADER_ERRO = "erro"
HEADER_TOPICO_ORIGINAL = "topico-original"
_TOPIC_ALREADY_EXISTS = 36


class PublicadorKafka(Protocol):
    async def publicar(self, topico: str, chave: str, valor: str, headers: Mapping[str, str] | None = None) -> None: ...


class ProdutorKafka:
    """Produtor com acks=all e idempotência habilitada. Conecta na primeira publicação (e reconecta após falha)."""

    def __init__(self, brokers: list[str], client_id: str, timeout_s: float = 10) -> None:
        self._brokers = brokers
        self._client_id = client_id
        self._timeout_s = timeout_s
        self._produtor: AIOKafkaProducer | None = None
        self._trava = asyncio.Lock()

    async def publicar(self, topico: str, chave: str, valor: str, headers: Mapping[str, str] | None = None) -> None:
        produtor = await self._iniciado()
        cabecalhos = [(k, v.encode()) for k, v in (headers or {}).items()]
        async with asyncio.timeout(self._timeout_s):
            await produtor.send_and_wait(topico, value=valor.encode(), key=chave.encode(), headers=cabecalhos)

    async def _iniciado(self) -> AIOKafkaProducer:
        async with self._trava:
            if self._produtor is None:
                produtor = AIOKafkaProducer(
                    bootstrap_servers=self._brokers,
                    client_id=self._client_id,
                    acks="all",
                    enable_idempotence=True,
                    request_timeout_ms=int(self._timeout_s * 1000),
                )
                try:
                    async with asyncio.timeout(self._timeout_s):
                        await produtor.start()
                except BaseException:
                    with contextlib.suppress(Exception):
                        await produtor.stop()
                    raise
                self._produtor = produtor
            return self._produtor

    async def fechar(self) -> None:
        if self._produtor is not None:
            await self._produtor.stop()
            self._produtor = None


def topicos_com_retry(topico: str) -> list[str]:
    return [topico, topico + SUFIXO_RETRY, topico + SUFIXO_DLT]


async def garantir_topicos(
    brokers: list[str], topicos: list[str], particoes: int = 3, tentativas: int = 30, intervalo_s: float = 2
) -> None:
    """Cria os tópicos que ainda não existem (todos com o mesmo número de partições)."""
    for tentativa in range(1, tentativas + 1):
        admin = AIOKafkaAdminClient(bootstrap_servers=brokers, client_id="criador-topicos")
        try:
            await admin.start()
            existentes = set(await admin.list_topics())
            faltantes = [t for t in topicos if t not in existentes]
            if faltantes:
                resposta: Any = await admin.create_topics(
                    [NewTopic(name=t, num_partitions=particoes, replication_factor=1) for t in faltantes]
                )
                erros = [e for e in resposta.topic_errors if e[1] not in (0, _TOPIC_ALREADY_EXISTS)]
                if erros:
                    raise RuntimeError(f"Falha ao criar tópicos: {erros}")
                log.info("Tópicos criados", topicos=faltantes)
            return
        except Exception as erro:
            if tentativa >= tentativas:
                raise
            log.warning("Kafka indisponível; tentando de novo", tentativa=tentativa, erro=descrever(erro))
            await asyncio.sleep(intervalo_s)
        finally:
            with contextlib.suppress(Exception):
                await admin.close()


@dataclass(frozen=True)
class MensagemRecebida:
    topico: str
    chave: str | None
    valor: str
    headers: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class OpcoesConsumidorComRetry:
    brokers: list[str]
    grupo: str
    topico: str
    processar: Callable[[str], Awaitable[None]]
    publicador: PublicadorKafka
    tentativas: int = 4
    """Total de tentativas: 1 no tópico principal + (tentativas - 1) no tópico de retry. Depois, DLT."""
    intervalo_retry_s: float = 5
    intervalo_circuito_aberto_s: float = 5
    """Com o circuit breaker aberto, a mensagem é reprocessada neste intervalo sem gastar tentativas."""
    esperar: Esperar = esperar
    relogio: Relogio = agora


class ConsumidorComRetry:
    """Consumidor com retry não bloqueante (equivalente ao @RetryableTopic do Spring Kafka).

    Falha → ``<topico>-retry`` (com o número da tentativa no header) → após esgotar as tentativas, ``<topico>-dlt``.
    Mensagem inválida vai direto para o DLT. Com o circuit breaker aberto o retry é bloqueante: a mensagem não vai
    para o tópico de retry, não gasta tentativas e o offset não é confirmado até ela ser processada.
    """

    def __init__(self, opcoes: OpcoesConsumidorComRetry) -> None:
        self._o = opcoes
        self._tarefas: list[asyncio.Task[None]] = []
        self._pausado = False

    @property
    def topico_retry(self) -> str:
        return self._o.topico + SUFIXO_RETRY

    @property
    def topico_dlt(self) -> str:
        return self._o.topico + SUFIXO_DLT

    @property
    def pausado(self) -> bool:
        return self._pausado

    def pausar(self) -> None:
        """Para de buscar mensagens (circuit breaker ABERTO)."""
        self._pausado = True

    def retomar(self) -> None:
        """Volta a buscar mensagens (circuit breaker MEIO_ABERTO ou FECHADO)."""
        self._pausado = False

    async def tratar(self, mensagem: MensagemRecebida) -> None:
        """Trata uma mensagem do tópico principal ou de retry. Só lança erro se não conseguir publicar no retry/DLT."""
        tentativa = int(mensagem.headers.get(HEADER_TENTATIVA, "1"))
        atraso_ms = int(mensagem.headers.get(HEADER_PROCESSAR_APOS, "0")) - epoch_ms(self._o.relogio())
        if atraso_ms > 0:
            await self._o.esperar(atraso_ms / 1000)
        while True:
            try:
                await self._o.processar(mensagem.valor)
                return
            except CircuitoAbertoError as erro:
                log.warning(
                    "Circuit breaker aberto; a mensagem será reprocessada sem gastar tentativas",
                    topico=mensagem.topico,
                    chave=mensagem.chave,
                    circuit_breaker=erro.circuit_breaker,
                )
                await self._o.esperar(self._o.intervalo_circuito_aberto_s)
            except Exception as erro:
                await self._encaminhar(mensagem, tentativa, erro)
                return

    async def _encaminhar(self, mensagem: MensagemRecebida, tentativa: int, erro: Exception) -> None:
        chave = mensagem.chave or ""
        headers = {HEADER_TOPICO_ORIGINAL: self._o.topico, HEADER_ERRO: descrever(erro)[:500]}
        if isinstance(erro, MensagemInvalidaError) or tentativa >= self._o.tentativas:
            await self._o.publicador.publicar(
                self.topico_dlt, chave, mensagem.valor, {**headers, HEADER_TENTATIVA: str(tentativa)}
            )
            log.error(
                f"Mensagem enviada para o dead letter topic {self.topico_dlt}",
                chave=chave,
                tentativa=tentativa,
                erro=str(erro),
            )
            return
        processar_apos = epoch_ms(self._o.relogio()) + int(self._o.intervalo_retry_s * 1000)
        await self._o.publicador.publicar(
            self.topico_retry,
            chave,
            mensagem.valor,
            {**headers, HEADER_TENTATIVA: str(tentativa + 1), HEADER_PROCESSAR_APOS: str(processar_apos)},
        )
        log.warning(
            f"Falha no processamento; mensagem enviada para {self.topico_retry}",
            chave=chave,
            tentativa=tentativa,
            erro=str(erro),
        )

    async def tratar_ate_conseguir(self, mensagem: MensagemRecebida) -> None:
        while True:
            try:
                await self.tratar(mensagem)
                return
            except Exception as erro:
                log.error(
                    "Não foi possível publicar no tópico de retry/DLT; tentando de novo sem confirmar o offset",
                    chave=mensagem.chave,
                    erro=descrever(erro),
                )
                await self._o.esperar(self._o.intervalo_circuito_aberto_s)

    def iniciar(self) -> None:
        self._tarefas = [
            asyncio.create_task(self._consumir(self._o.grupo, self._o.topico, com_retry=True)),
            asyncio.create_task(self._consumir(self._o.grupo + SUFIXO_RETRY, self.topico_retry, com_retry=True)),
            asyncio.create_task(self._consumir(self._o.grupo + SUFIXO_DLT, self.topico_dlt, com_retry=False)),
        ]

    async def parar(self) -> None:
        for tarefa in self._tarefas:
            tarefa.cancel()
        await asyncio.gather(*self._tarefas, return_exceptions=True)
        self._tarefas = []

    async def _consumir(self, grupo: str, topico: str, *, com_retry: bool) -> None:
        while True:
            consumidor = AIOKafkaConsumer(
                topico,
                bootstrap_servers=self._o.brokers,
                group_id=grupo,
                client_id=grupo,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_interval_ms=600_000,
            )
            try:
                await consumidor.start()
                log.info("Consumidor Kafka iniciado", grupo=grupo, topico=topico)
                while True:
                    if com_retry and self._pausado:
                        # Mantém o consumidor no grupo (getmany continua sendo chamado), mas sem receber mensagens.
                        consumidor.pause(*consumidor.assignment())
                    elif consumidor.paused():
                        consumidor.resume(*consumidor.paused())
                    lotes = await consumidor.getmany(timeout_ms=1_000, max_records=10)
                    for particao, registros in lotes.items():
                        for r in registros:
                            mensagem = MensagemRecebida(
                                topico=r.topic,
                                chave=r.key.decode() if r.key is not None else None,
                                valor=r.value.decode() if r.value is not None else "",
                                headers={k: v.decode() for k, v in (r.headers or ())},
                            )
                            if com_retry:
                                await self.tratar_ate_conseguir(mensagem)
                            else:
                                log.error(
                                    "Mensagem no dead letter topic",
                                    topico=topico,
                                    chave=mensagem.chave,
                                    headers=dict(mensagem.headers),
                                    payload=mensagem.valor,
                                )
                            await consumidor.commit({TopicPartition(particao.topic, particao.partition): r.offset + 1})
            except asyncio.CancelledError:
                raise
            except Exception as erro:
                log.error(
                    "Erro no consumidor Kafka; reiniciando em 5s", grupo=grupo, topico=topico, erro=descrever(erro)
                )
                await asyncio.sleep(5)
            finally:
                with contextlib.suppress(Exception):
                    await consumidor.stop()
