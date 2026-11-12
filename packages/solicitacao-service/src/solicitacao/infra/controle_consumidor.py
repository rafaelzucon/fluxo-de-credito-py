from collections.abc import Callable
from typing import Protocol

from comum.circuit_breaker import EstadoCircuito
from comum.logs import criar_logger

log = criar_logger("ControleConsumidorDecisao")


class Pausavel(Protocol):
    def pausar(self) -> None: ...

    def retomar(self) -> None: ...


def reagir_ao_circuit_breaker(consumidor: Pausavel) -> Callable[[EstadoCircuito], None]:
    """Pausa o consumo de decisao-tomada (principal e retry) quando o circuit breaker sns-publisher abre e o
    retoma quando ele passa para MEIO_ABERTO ou FECHADO. O consumo do DLT nunca é pausado."""

    def reagir(estado: EstadoCircuito) -> None:
        if estado is EstadoCircuito.ABERTO:
            log.warning("Circuit breaker sns-publisher ABERTO: pausando o consumo de decisao-tomada")
            consumidor.pausar()
        else:
            log.info("Circuit breaker sns-publisher: retomando o consumo de decisao-tomada", estado=estado)
            consumidor.retomar()

    return reagir
