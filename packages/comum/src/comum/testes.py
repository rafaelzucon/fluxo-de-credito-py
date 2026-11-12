"""Dublês de teste compartilhados pelos pacotes (banco, Kafka, relógio)."""

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import oracledb

from comum.oracle import Binds, Conexao, Linha


def erro_unique() -> oracledb.IntegrityError:
    """Erro equivalente ao ORA-00001 (violação de constraint UNIQUE)."""
    return oracledb.IntegrityError(SimpleNamespace(code=1, full_code="ORA-00001", message="ORA-00001: unique"))


def erro_banco(codigo: int = 3113) -> oracledb.DatabaseError:
    return oracledb.DatabaseError(SimpleNamespace(code=codigo, full_code=f"ORA-{codigo:05d}", message="falha"))


@dataclass
class ConexaoFake:
    """Registra os comandos; as respostas podem ser programadas por trecho do SQL."""

    linhas_afetadas: int = 1
    consultas: dict[str, list[Linha]] = field(default_factory=dict)
    erros: dict[str, BaseException] = field(default_factory=dict)
    comandos: list[tuple[str, dict[str, Any]]] = field(default_factory=list)

    async def executar(self, sql: str, binds: Binds | None = None) -> int:
        self.comandos.append((sql, dict(binds or {})))
        self._falhar_se_programado(sql)
        return self.linhas_afetadas

    async def consultar(self, sql: str, binds: Binds | None = None) -> list[Linha]:
        self.comandos.append((sql, dict(binds or {})))
        self._falhar_se_programado(sql)
        return next((linhas for trecho, linhas in self.consultas.items() if trecho in sql), [])

    def _falhar_se_programado(self, sql: str) -> None:
        for trecho, erro in self.erros.items():
            if trecho in sql:
                raise erro


@dataclass
class BancoFake:
    """Transacoes em memória: conta commits e rollbacks."""

    conexao: Conexao = field(default_factory=ConexaoFake)
    commits: int = 0
    rollbacks: int = 0

    async def em_transacao[T](self, trabalho: Callable[[Conexao], Awaitable[T]]) -> T:
        try:
            resultado = await trabalho(self.conexao)
        except BaseException:
            self.rollbacks += 1
            raise
        self.commits += 1
        return resultado


@dataclass
class PublicacaoKafka:
    topico: str
    chave: str
    valor: str
    headers: dict[str, str]


@dataclass
class PublicadorKafkaFake:
    publicacoes: list[PublicacaoKafka] = field(default_factory=list)
    erro: BaseException | None = None

    async def publicar(self, topico: str, chave: str, valor: str, headers: Mapping[str, str] | None = None) -> None:
        if self.erro is not None:
            raise self.erro
        self.publicacoes.append(PublicacaoKafka(topico, chave, valor, dict(headers or {})))


INSTANTE = datetime(2025, 1, 2, 3, 4, 5, 678000, tzinfo=UTC)


class RelogioFake:
    """Relógio de parede controlável."""

    def __init__(self, inicio: datetime = INSTANTE) -> None:
        self.agora = inicio

    def __call__(self) -> datetime:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += timedelta(seconds=segundos)


class MonotonicoFake:
    """Relógio monotônico controlável (circuit breaker)."""

    def __init__(self) -> None:
        self.agora = 1_000.0

    def __call__(self) -> float:
        return self.agora

    def avancar(self, segundos: float) -> None:
        self.agora += segundos


class EsperaFake:
    """Registra as esperas sem dormir."""

    def __init__(self, relogio: RelogioFake | None = None) -> None:
        self.esperas: list[float] = []
        self._relogio = relogio

    async def __call__(self, segundos: float) -> None:
        self.esperas.append(segundos)
        if self._relogio is not None:
            self._relogio.avancar(segundos)
