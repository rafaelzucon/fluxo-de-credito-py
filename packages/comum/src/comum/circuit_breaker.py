"""Circuit breaker assíncrono no modelo do Resilience4j (janela deslizante por contagem).

FECHADO: as chamadas passam e o resultado de cada uma entra numa janela das últimas N chamadas. Quando a janela
tem pelo menos ``minimo_chamadas`` e o percentual de falhas atinge ``limite_falhas_percentual``, o circuito ABRE.
ABERTO: as chamadas são rejeitadas na hora com ``CircuitoAbertoError``. Depois de ``tempo_aberto_ms`` o circuito
passa sozinho (timer) para MEIO_ABERTO e avisa os ouvintes — assim consumidores pausados voltam a consumir.
MEIO_ABERTO: só ``chamadas_meio_aberto`` chamadas de teste são liberadas; terminadas todas, se o percentual de
falhas for menor que o limite o circuito FECHA, senão volta a ABRIR. Chamadas mais lentas que ``timeout_ms`` são
interrompidas e contam como falha.
"""

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import TypedDict

from comum.config import ConfigCircuitBreaker
from comum.logs import criar_logger
from comum.tempo import Monotonico, monotonico

log = criar_logger("circuit-breaker")


class EstadoCircuito(StrEnum):
    FECHADO = "FECHADO"
    ABERTO = "ABERTO"
    MEIO_ABERTO = "MEIO_ABERTO"


class CircuitoAbertoError(Exception):
    def __init__(self, circuit_breaker: str) -> None:
        super().__init__(f"Circuit breaker {circuit_breaker} aberto; chamada não permitida")
        self.circuit_breaker = circuit_breaker


class ResumoCircuitBreaker(TypedDict):
    nome: str
    estado: EstadoCircuito
    falhas: int
    sucessos: int
    rejeitadas: int


type OuvinteEstado = Callable[[EstadoCircuito], None]


class CircuitBreaker:
    def __init__(
        self,
        nome: str,
        config: ConfigCircuitBreaker | None = None,
        *,
        ignorar: tuple[type[BaseException], ...] = (),
        relogio: Monotonico = monotonico,
    ) -> None:
        self.nome = nome
        self._c = config or ConfigCircuitBreaker()
        self._ignorar = ignorar
        self._relogio = relogio
        self._estado = EstadoCircuito.FECHADO
        self._janela: deque[bool] = deque(maxlen=self._c.janela_chamadas)  # True = falha
        self._geracao = 0
        self._aberto_em = 0.0
        self._testes_em_andamento = 0
        self._resultados_teste: list[bool] = []
        self._timer: asyncio.TimerHandle | None = None
        self._ouvintes: list[OuvinteEstado] = []
        self._falhas = 0
        self._sucessos = 0
        self._rejeitadas = 0

    @property
    def estado(self) -> EstadoCircuito:
        self._verificar_tempo_aberto()
        return self._estado

    def ao_mudar_estado(self, ouvinte: OuvinteEstado) -> None:
        self._ouvintes.append(ouvinte)

    async def executar[T](self, acao: Callable[[], Awaitable[T]]) -> T:
        """Executa a ação protegida. :raises CircuitoAbertoError: se a chamada não for permitida."""
        self._adquirir_permissao()
        geracao = self._geracao
        try:
            async with asyncio.timeout(self._c.timeout_ms / 1000):
                resultado = await acao()
        except asyncio.CancelledError:
            self._liberar_permissao(geracao)
            raise
        except TimeoutError as erro:
            self._registrar(geracao, falha=True)
            raise TimeoutError(f"Chamada protegida por {self.nome} excedeu {self._c.timeout_ms} ms") from erro
        except Exception as erro:
            if isinstance(erro, self._ignorar):
                self._liberar_permissao(geracao)
            else:
                self._registrar(geracao, falha=True)
            raise
        self._registrar(geracao, falha=False)
        return resultado

    def resumo(self) -> ResumoCircuitBreaker:
        return {
            "nome": self.nome,
            "estado": self.estado,
            "falhas": self._falhas,
            "sucessos": self._sucessos,
            "rejeitadas": self._rejeitadas,
        }

    def desligar(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _adquirir_permissao(self) -> None:
        estado = self.estado
        if estado is EstadoCircuito.ABERTO or (
            estado is EstadoCircuito.MEIO_ABERTO and self._testes_em_andamento >= self._c.chamadas_meio_aberto
        ):
            self._rejeitadas += 1
            raise CircuitoAbertoError(self.nome)
        if estado is EstadoCircuito.MEIO_ABERTO:
            self._testes_em_andamento += 1

    def _liberar_permissao(self, geracao: int) -> None:
        if geracao == self._geracao and self._estado is EstadoCircuito.MEIO_ABERTO:
            self._testes_em_andamento -= 1

    def _registrar(self, geracao: int, *, falha: bool) -> None:
        if falha:
            self._falhas += 1
        else:
            self._sucessos += 1
        if geracao != self._geracao:
            return  # chamada iniciada num estado anterior: não influencia o estado atual
        if self._estado is EstadoCircuito.FECHADO:
            self._janela.append(falha)
            if len(self._janela) >= self._c.minimo_chamadas and self._percentual(self._janela) >= self._limite:
                self._transicionar(EstadoCircuito.ABERTO)
        elif self._estado is EstadoCircuito.MEIO_ABERTO:
            self._resultados_teste.append(falha)
            if len(self._resultados_teste) >= self._c.chamadas_meio_aberto:
                abrir = self._percentual(self._resultados_teste) >= self._limite
                self._transicionar(EstadoCircuito.ABERTO if abrir else EstadoCircuito.FECHADO)

    @property
    def _limite(self) -> float:
        return self._c.limite_falhas_percentual

    @staticmethod
    def _percentual(resultados: deque[bool] | list[bool]) -> float:
        return 100 * sum(resultados) / len(resultados)

    def _verificar_tempo_aberto(self) -> None:
        if (
            self._estado is EstadoCircuito.ABERTO
            and self._relogio() - self._aberto_em >= self._c.tempo_aberto_ms / 1000
        ):
            self._transicionar(EstadoCircuito.MEIO_ABERTO)

    def _transicionar(self, novo: EstadoCircuito) -> None:
        anterior, self._estado = self._estado, novo
        self._geracao += 1
        self._janela.clear()
        self._resultados_teste.clear()
        self._testes_em_andamento = 0
        self.desligar()
        if novo is EstadoCircuito.ABERTO:
            self._aberto_em = self._relogio()
            self._agendar_meio_aberto()
        nivel = log.warning if novo is EstadoCircuito.ABERTO else log.info
        nivel(f"Circuit breaker {self.nome} {novo}", circuit_breaker=self.nome, estado=novo, anterior=anterior)
        for ouvinte in self._ouvintes:
            try:
                ouvinte(novo)
            except Exception as erro:
                log.exception("Falha no ouvinte do circuit breaker", circuit_breaker=self.nome, erro=str(erro))

    def _agendar_meio_aberto(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # sem event loop: a transição acontece na próxima consulta ao estado
        self._timer = loop.call_later(self._c.tempo_aberto_ms / 1000, self._ao_expirar_tempo_aberto)

    def _ao_expirar_tempo_aberto(self) -> None:
        self._timer = None
        if self._estado is EstadoCircuito.ABERTO:
            self._transicionar(EstadoCircuito.MEIO_ABERTO)
