from typing import TYPE_CHECKING, Protocol

from comum.circuit_breaker import CircuitBreaker
from comum.kafka import PublicadorKafka

from solicitacao.eventos import DecisaoCredito, SolicitacaoCriada

if TYPE_CHECKING:
    from types_aiobotocore_sns import SNSClient


class PublicadorSolicitacaoCriada(Protocol):
    async def publicar(self, evento: SolicitacaoCriada) -> None:
        """:raises CircuitoAbertoError: se o circuito estiver aberto."""
        ...


class PublicadorSolicitacaoCriadaKafka:
    """Publica SolicitacaoCriada no Kafka protegido pelo circuit breaker kafka-producer."""

    def __init__(self, kafka: PublicadorKafka, circuit_breaker: CircuitBreaker, topico: str) -> None:
        self._kafka = kafka
        self._cb = circuit_breaker
        self._topico = topico

    async def publicar(self, evento: SolicitacaoCriada) -> None:
        await self._cb.executar(lambda: self._kafka.publicar(self._topico, evento.solicitacao_id, evento.para_json()))


class PublicadorDecisao(Protocol):
    async def publicar(self, decisao: DecisaoCredito) -> None: ...


class PublicadorDecisaoSns:
    """Publica a decisão no tópico SNS (fan-out para as filas de e-mail e push) pelo circuit breaker sns-publisher."""

    def __init__(self, sns: SNSClient, circuit_breaker: CircuitBreaker, topico_arn: str) -> None:
        self._sns = sns
        self._cb = circuit_breaker
        self._topico_arn = topico_arn

    async def publicar(self, decisao: DecisaoCredito) -> None:
        async def publicar() -> None:
            await self._sns.publish(TopicArn=self._topico_arn, Message=decisao.para_json())

        await self._cb.executar(publicar)
