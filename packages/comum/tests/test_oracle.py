from pathlib import Path

import oracledb
from comum.oracle import eh_violacao_unique, migrar, separar_comandos
from comum.oracle_tempo import de_coluna, para_bind
from comum.testes import INSTANTE, BancoFake, ConexaoFake, erro_banco, erro_unique


def test_separa_comandos_terminados_em_ponto_e_virgula_e_ignora_comentarios() -> None:
    script = """-- comentário; com ponto e vírgula
CREATE TABLE a (id NUMBER);

CREATE INDEX ix ON a (id);
-- fim
INSERT INTO a VALUES (1)"""
    assert separar_comandos(script) == [
        "CREATE TABLE a (id NUMBER)",
        "CREATE INDEX ix ON a (id)",
        "INSERT INTO a VALUES (1)",
    ]


def test_reconhece_o_ora_00001() -> None:
    assert eh_violacao_unique(erro_unique())
    assert not eh_violacao_unique(erro_banco())
    assert not eh_violacao_unique(oracledb.IntegrityError("sem detalhes"))
    assert not eh_violacao_unique(ValueError("outro"))


async def test_aplica_em_ordem_de_versao_so_as_migracoes_ainda_nao_registradas(tmp_path: Path) -> None:
    (tmp_path / "V10__dez.sql").write_text("CREATE TABLE dez (id NUMBER);")
    (tmp_path / "V2__dois.sql").write_text("CREATE TABLE dois (id NUMBER);\nCREATE INDEX ix_dois ON dois (id);")
    (tmp_path / "V1__um.sql").write_text("CREATE TABLE um (id NUMBER);")
    (tmp_path / "LEIAME.txt").write_text("ignorado")

    class ConexaoComHistorico(ConexaoFake):
        async def consultar(self, sql: str, binds: object = None) -> list[dict[str, object]]:
            await super().consultar(sql, binds)  # type: ignore[arg-type]
            return [{"qtd": 1 if binds == {"versao": "V1__um.sql"} else 0}]

    conexao = ConexaoComHistorico()
    banco = BancoFake(conexao)

    assert await migrar(banco, tmp_path) == ["V2__dois.sql", "V10__dez.sql"]
    ddl = [sql for sql, _ in conexao.comandos if sql.startswith(("CREATE TABLE d", "CREATE INDEX"))]
    assert ddl == ["CREATE TABLE dois (id NUMBER)", "CREATE INDEX ix_dois ON dois (id)", "CREATE TABLE dez (id NUMBER)"]
    assert "CREATE TABLE IF NOT EXISTS historico_migracao" in conexao.comandos[0][0]


def test_instantes_sao_gravados_e_lidos_em_utc() -> None:
    assert para_bind(INSTANTE) == "2025-01-02T03:04:05.678000+00:00"
    assert de_coluna(INSTANTE.replace(tzinfo=None)) == INSTANTE
