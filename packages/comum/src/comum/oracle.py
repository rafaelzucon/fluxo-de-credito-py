"""Acesso ao Oracle com python-oracledb em modo Thin assíncrono (sem Oracle Instant Client)."""

import asyncio
import contextlib
import re
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any, Protocol

import oracledb

from comum.logs import criar_logger

log = criar_logger("oracle")

# NUMBER(15,2) volta como Decimal (e não float), preservando os centavos.
oracledb.defaults.fetch_decimals = True

type Binds = Mapping[str, Any]
type Linha = dict[str, Any]


class Conexao(Protocol):
    """Subconjunto da conexão usado pelos repositórios; facilita os testes."""

    async def executar(self, sql: str, binds: Binds | None = None) -> int:
        """Executa um comando e devolve o número de linhas afetadas."""
        ...

    async def consultar(self, sql: str, binds: Binds | None = None) -> list[Linha]:
        """Executa uma consulta e devolve as linhas com o nome das colunas em minúsculas."""
        ...


class Transacoes(Protocol):
    """Executa uma unidade de trabalho numa transação: commit no sucesso, rollback em qualquer erro."""

    async def em_transacao[T](self, trabalho: Callable[[Conexao], Awaitable[T]]) -> T: ...


class _ConexaoOracle:
    def __init__(self, conexao: oracledb.AsyncConnection) -> None:
        self._conexao = conexao

    async def executar(self, sql: str, binds: Binds | None = None) -> int:
        with self._conexao.cursor() as cursor:
            await cursor.execute(sql, dict(binds or {}))
            return int(cursor.rowcount)

    async def consultar(self, sql: str, binds: Binds | None = None) -> list[Linha]:
        with self._conexao.cursor() as cursor:
            await cursor.execute(sql, dict(binds or {}))
            colunas = [d[0].lower() for d in cursor.description or []]
            return [dict(zip(colunas, linha, strict=True)) for linha in await cursor.fetchall()]


class BancoOracle:
    def __init__(self, pool: oracledb.AsyncConnectionPool) -> None:
        self._pool = pool

    @classmethod
    async def conectar(
        cls,
        usuario: str,
        senha: str,
        dsn: str,
        *,
        pool_max: int = 10,
        tentativas: int = 30,
        intervalo_s: float = 5,
    ) -> BancoOracle:
        """Cria o pool tentando de novo enquanto o banco não aceita conexões."""
        tentativa = 1
        while True:
            pool = oracledb.create_pool_async(user=usuario, password=senha, dsn=dsn, min=1, max=pool_max, increment=1)
            try:
                async with pool.acquire() as conexao:
                    await conexao.ping()
                return cls(pool)
            except oracledb.Error as erro:
                await pool.close(force=True)
                if tentativa >= tentativas:
                    raise
                log.warning("Oracle indisponível; tentando de novo", tentativa=tentativa, erro=str(erro))
                tentativa += 1
                await asyncio.sleep(intervalo_s)

    async def em_transacao[T](self, trabalho: Callable[[Conexao], Awaitable[T]]) -> T:
        async with self._pool.acquire() as conexao:
            try:
                resultado = await trabalho(_ConexaoOracle(conexao))
                await conexao.commit()
                return resultado
            except BaseException:
                with contextlib.suppress(oracledb.Error):
                    await conexao.rollback()
                raise

    async def saudavel(self) -> bool:
        try:
            await self.em_transacao(lambda c: c.consultar("SELECT 1 FROM dual"))
            return True
        except Exception:
            return False

    async def fechar(self) -> None:
        await self._pool.close(force=True)


def eh_violacao_unique(erro: BaseException) -> bool:
    """ORA-00001: violação de constraint UNIQUE."""
    return isinstance(erro, oracledb.IntegrityError) and getattr(erro.args[0] if erro.args else None, "code", 0) == 1


_VERSAO = re.compile(r"^V(\d+)__.+\.sql$")


async def migrar(banco: Transacoes, diretorio: Path) -> list[str]:
    """Migrador simples no estilo Flyway.

    Aplica, em ordem, os arquivos ``V<n>__<descricao>.sql`` ainda não registrados na tabela ``historico_migracao``.
    Cada arquivo pode ter vários comandos separados por ';' no fim da linha.
    """
    await banco.em_transacao(
        lambda c: c.executar(
            """CREATE TABLE IF NOT EXISTS historico_migracao (
                 versao       VARCHAR2(100) NOT NULL,
                 aplicado_em  TIMESTAMP WITH TIME ZONE DEFAULT SYSTIMESTAMP NOT NULL,
                 CONSTRAINT pk_historico_migracao PRIMARY KEY (versao))"""
        )
    )
    aplicadas: list[str] = []
    for arquivo, script in await asyncio.to_thread(ler_migracoes, diretorio):

        async def aplicar(c: Conexao, arquivo: str = arquivo, script: str = script) -> bool:
            linhas = await c.consultar(
                "SELECT COUNT(*) AS qtd FROM historico_migracao WHERE versao = :versao", {"versao": arquivo}
            )
            if linhas and linhas[0]["qtd"] > 0:
                return False
            for comando in separar_comandos(script):
                await c.executar(comando)
            await c.executar("INSERT INTO historico_migracao (versao) VALUES (:versao)", {"versao": arquivo})
            return True

        if await banco.em_transacao(aplicar):
            log.info("Migração aplicada", arquivo=arquivo)
            aplicadas.append(arquivo)
    return aplicadas


def ler_migracoes(diretorio: Path) -> list[tuple[str, str]]:
    """Arquivos de migração em ordem de versão, com o conteúdo de cada um."""
    versoes = {p.name: int(m.group(1)) for p in diretorio.iterdir() if (m := _VERSAO.match(p.name))}
    return [(a, (diretorio / a).read_text(encoding="utf-8")) for a in sorted(versoes, key=versoes.__getitem__)]


def separar_comandos(script: str) -> list[str]:
    sem_comentarios = "\n".join(linha for linha in script.split("\n") if not linha.strip().startswith("--"))
    return [c.strip() for c in re.split(r";\s*(?:\n|$)", sem_comentarios) if c.strip()]
