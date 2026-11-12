from typing import Protocol

from comum.oracle import Conexao, Linha
from comum.oracle_tempo import FORMATO_ORACLE, coluna_utc, de_coluna, para_bind

from solicitacao.dominio import Solicitacao, StatusSolicitacao


class RepositorioSolicitacao(Protocol):
    async def inserir(self, c: Conexao, s: Solicitacao) -> None: ...

    async def atualizar(self, c: Conexao, s: Solicitacao) -> None:
        """Atualiza com controle otimista pela coluna versao."""
        ...

    async def buscar(self, c: Conexao, id: str) -> Solicitacao | None: ...

    async def buscar_por_status(self, c: Conexao, status: StatusSolicitacao, limite: int) -> list[Solicitacao]: ...


class ConflitoDeVersaoError(Exception):
    def __init__(self, id: str) -> None:
        super().__init__(f"Solicitação {id} foi alterada por outra transação")


_SELECT = f"""SELECT id, cliente_id, produto_codigo, valor_solicitado, email, token_dispositivo, status,
    motivo_decisao, {coluna_utc("criado_em")}, {coluna_utc("atualizado_em")}, versao FROM solicitacao"""


class RepositorioSolicitacaoOracle:
    INSERT = f"""INSERT INTO solicitacao (id, cliente_id, produto_codigo, valor_solicitado, email, token_dispositivo,
        status, motivo_decisao, criado_em, atualizado_em, versao)
        VALUES (:id, :cliente_id, :produto_codigo, :valor_solicitado, :email, :token_dispositivo, :status,
        :motivo_decisao, TO_TIMESTAMP_TZ(:criado_em, {FORMATO_ORACLE}),
        TO_TIMESTAMP_TZ(:atualizado_em, {FORMATO_ORACLE}), :versao)"""
    UPDATE = f"""UPDATE solicitacao SET status = :status, motivo_decisao = :motivo_decisao,
        atualizado_em = TO_TIMESTAMP_TZ(:atualizado_em, {FORMATO_ORACLE}), versao = versao + 1
        WHERE id = :id AND versao = :versao"""
    SELECT_POR_ID = f"{_SELECT} WHERE id = :id"
    SELECT_POR_STATUS = f"{_SELECT} WHERE status = :status ORDER BY criado_em FETCH FIRST :limite ROWS ONLY"

    async def inserir(self, c: Conexao, s: Solicitacao) -> None:
        await c.executar(
            self.INSERT,
            {
                "id": s.id,
                "cliente_id": s.cliente_id,
                "produto_codigo": s.produto_codigo,
                "valor_solicitado": s.valor_solicitado,
                "email": s.email,
                "token_dispositivo": s.token_dispositivo,
                "status": s.status.value,
                "motivo_decisao": s.motivo_decisao,
                "criado_em": para_bind(s.criado_em),
                "atualizado_em": para_bind(s.atualizado_em),
                "versao": s.versao,
            },
        )

    async def atualizar(self, c: Conexao, s: Solicitacao) -> None:
        linhas = await c.executar(
            self.UPDATE,
            {
                "status": s.status.value,
                "motivo_decisao": s.motivo_decisao,
                "atualizado_em": para_bind(s.atualizado_em),
                "id": s.id,
                "versao": s.versao,
            },
        )
        if linhas != 1:
            raise ConflitoDeVersaoError(s.id)
        s.versao += 1

    async def buscar(self, c: Conexao, id: str) -> Solicitacao | None:
        linhas = await c.consultar(self.SELECT_POR_ID, {"id": id})
        return _para_solicitacao(linhas[0]) if linhas else None

    async def buscar_por_status(self, c: Conexao, status: StatusSolicitacao, limite: int) -> list[Solicitacao]:
        linhas = await c.consultar(self.SELECT_POR_STATUS, {"status": status.value, "limite": limite})
        return [_para_solicitacao(linha) for linha in linhas]


def _para_solicitacao(linha: Linha) -> Solicitacao:
    return Solicitacao(
        id=linha["id"],
        cliente_id=linha["cliente_id"],
        produto_codigo=linha["produto_codigo"],
        valor_solicitado=linha["valor_solicitado"],
        email=linha["email"],
        token_dispositivo=linha["token_dispositivo"],
        status=StatusSolicitacao(linha["status"]),
        motivo_decisao=linha["motivo_decisao"],
        criado_em=de_coluna(linha["criado_em"]),
        atualizado_em=de_coluna(linha["atualizado_em"]),
        versao=int(linha["versao"]),
    )
