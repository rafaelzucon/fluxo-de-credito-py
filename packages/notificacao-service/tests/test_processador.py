from dataclasses import dataclass, field

import pytest
from apoio_notificacao import decisao
from comum.circuit_breaker import CircuitoAbertoError
from comum.testes import BancoFake, ConexaoFake, erro_banco, erro_unique
from notificacao.dominio import Canal, DecisaoCredito
from notificacao.infra.registro_envio import RegistroEnvioOracle, chave_envio
from notificacao.processador import ProcessadorNotificacao


@dataclass
class EnviadorFake:
    canal: Canal
    enviadas: list[DecisaoCredito] = field(default_factory=list)
    erro: BaseException | None = None

    async def enviar(self, d: DecisaoCredito) -> None:
        if self.erro is not None:
            raise self.erro
        self.enviadas.append(d)


class ConexaoComChaves(ConexaoFake):
    """Simula a constraint UNIQUE da tabela envio_notificacao."""

    def __init__(self) -> None:
        super().__init__()
        self.chaves: set[str] = set()

    async def executar(self, sql: str, binds: object = None) -> int:
        chave = binds["chave"]  # type: ignore[index]
        if chave in self.chaves:
            raise erro_unique()
        self.chaves.add(chave)
        return await super().executar(sql, binds)  # type: ignore[arg-type]


def criar(
    erro_email: BaseException | None = None,
) -> tuple[ProcessadorNotificacao, BancoFake, EnviadorFake, EnviadorFake]:
    banco = BancoFake(ConexaoComChaves())
    email, push = EnviadorFake(Canal.EMAIL, erro=erro_email), EnviadorFake(Canal.PUSH)
    return ProcessadorNotificacao(banco, RegistroEnvioOracle(), [email, push]), banco, email, push


async def test_registra_a_chave_e_envia_pelo_canal() -> None:
    p, banco, email, push = criar()
    assert await p.processar(Canal.EMAIL, decisao())
    assert [d.solicitacao_id for d in email.enviadas] == ["sol-1"]
    assert push.enviadas == []
    assert banco.conexao.chaves == {"email:sol-1"}  # type: ignore[attr-defined]


async def test_ignora_duplicada_no_mesmo_canal_mas_envia_no_outro() -> None:
    p, _, email, push = criar()
    assert await p.processar(Canal.EMAIL, decisao())
    assert not await p.processar(Canal.EMAIL, decisao())
    assert await p.processar(Canal.PUSH, decisao())
    assert (len(email.enviadas), len(push.enviadas)) == (1, 1)


@pytest.mark.parametrize("erro", [ConnectionError("smtp fora"), CircuitoAbertoError("email-provider")])
async def test_faz_rollback_quando_o_provedor_falha(erro: Exception) -> None:
    p, banco, _, _ = criar(erro_email=erro)
    with pytest.raises(type(erro)):
        await p.processar(Canal.EMAIL, decisao())
    assert (banco.commits, banco.rollbacks) == (0, 1)


async def test_falha_quando_nao_ha_enviador_para_o_canal() -> None:
    p = ProcessadorNotificacao(BancoFake(), RegistroEnvioOracle(), [EnviadorFake(Canal.EMAIL)])
    with pytest.raises(LookupError, match="PUSH"):
        await p.processar(Canal.PUSH, decisao())


async def test_registro_de_envio_usa_chave_por_canal_e_propaga_outros_erros() -> None:
    assert chave_envio(Canal.PUSH, "sol-1") == "push:sol-1"
    conexao = ConexaoFake()
    assert await RegistroEnvioOracle().registrar(conexao, Canal.PUSH, "sol-1")
    assert conexao.comandos[0][1] == {"chave": "push:sol-1", "canal": "PUSH", "solicitacao_id": "sol-1"}

    assert not await RegistroEnvioOracle().registrar(
        ConexaoFake(erros={"envio_notificacao": erro_unique()}), Canal.PUSH, "sol-1"
    )
    with pytest.raises(Exception, match="falha"):
        await RegistroEnvioOracle().registrar(ConexaoFake(erros={"envio_notificacao": erro_banco()}), Canal.PUSH, "s")
