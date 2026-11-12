import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

type Relogio = Callable[[], datetime]
type Esperar = Callable[[float], Awaitable[None]]
type Monotonico = Callable[[], float]


def agora() -> datetime:
    return datetime.now(UTC)


async def esperar(segundos: float) -> None:
    await asyncio.sleep(segundos)


monotonico: Monotonico = time.monotonic


def epoch_ms(instante: datetime) -> int:
    return int(instante.timestamp() * 1000)
