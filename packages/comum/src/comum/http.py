"""Endpoints de saúde (equivalentes ao actuator/health) e de estado dos circuit breakers."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from comum.circuit_breaker import CircuitBreaker, ResumoCircuitBreaker


@dataclass
class Saude:
    """Preenchida durante o ciclo de vida da aplicação, à medida que os recursos são criados."""

    verificacoes: dict[str, Callable[[], Awaitable[bool]]] = field(default_factory=dict)
    circuit_breakers: list[CircuitBreaker] = field(default_factory=list)


def registrar_saude(app: FastAPI, saude: Saude) -> None:
    @app.get("/health", tags=["operação"])
    async def health() -> JSONResponse:
        componentes = {nome: "UP" if await v() else "DOWN" for nome, v in saude.verificacoes.items()}
        status = "UP" if all(s == "UP" for s in componentes.values()) else "DOWN"
        corpo: dict[str, Any] = {
            "status": status,
            "componentes": componentes,
            "circuitBreakers": {cb.nome: cb.estado for cb in saude.circuit_breakers},
        }
        return JSONResponse(corpo, status_code=200 if status == "UP" else 503)

    @app.get("/circuit-breakers", tags=["operação"])
    async def circuit_breakers() -> list[ResumoCircuitBreaker]:
        return [cb.resumo() for cb in saude.circuit_breakers]
