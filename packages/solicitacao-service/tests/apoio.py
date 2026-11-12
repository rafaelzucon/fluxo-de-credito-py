import contextlib
from dataclasses import dataclass, field, replace
from decimal import Decimal

from comum.circuit_breaker import CircuitBreaker, EstadoCircuito
from comum.oracle import Conexao
from comum.testes import INSTANTE
from solicitacao.dominio import DadosNovaSolicitacao, Solicitacao, StatusSolicitacao
from solicitacao.eventos import DecisaoCredito, SolicitacaoCriada

DADOS = DadosNovaSolicitacao(
    cliente_id="cliente-123",
    produto_codigo="CARTAO-GOLD",
    valor_solicitado=Decimal("1500.50"),
    email="cliente@exemplo.com.br",
    token_dispositivo="token-abc",
)


def solicitacao(**alteracoes: object) -> Solicitacao:
    return replace(Solicitacao.nova(DADOS, INSTANTE), **({"id": "sol-1"} | alteracoes))  # type: ignore[arg-type]


@dataclass
class RepositorioMemoria:
    """Repositório em memória com controle otimista de versão."""

    solicitacoes: dict[str, Solicitacao] = field(default_factory=dict)
    atualizacoes: list[tuple[str, StatusSolicitacao]] = field(default_factory=list)

    async def inserir(self, c: Conexao, s: Solicitacao) -> None:
        self.solicitacoes[s.id] = replace(s)

    async def atualizar(self, c: Conexao, s: Solicitacao) -> None:
        self.atualizacoes.append((s.id, s.status))
        s.versao += 1
        self.solicitacoes[s.id] = replace(s)

    async def buscar(self, c: Conexao, id: str) -> Solicitacao | None:
        s = self.solicitacoes.get(id)
        return replace(s) if s else None

    async def buscar_por_status(self, c: Conexao, status: StatusSolicitacao, limite: int) -> list[Solicitacao]:
        return [replace(s) for s in self.solicitacoes.values() if s.status is status][:limite]


@dataclass
class PublicadorSolicitacaoFake:
    eventos: list[SolicitacaoCriada] = field(default_factory=list)
    erros: list[BaseException] = field(default_factory=list)

    async def publicar(self, evento: SolicitacaoCriada) -> None:
        if self.erros:
            raise self.erros.pop(0)
        self.eventos.append(evento)


@dataclass
class PublicadorDecisaoFake:
    decisoes: list[DecisaoCredito] = field(default_factory=list)
    erro: BaseException | None = None

    async def publicar(self, decisao: DecisaoCredito) -> None:
        if self.erro is not None:
            raise self.erro
        self.decisoes.append(decisao)


async def abrir(cb: CircuitBreaker) -> None:
    """Abre o circuito com falhas consecutivas."""

    async def falha() -> None:
        raise ConnectionError("fora")

    while cb.estado is not EstadoCircuito.ABERTO:
        with contextlib.suppress(ConnectionError):
            await cb.executar(falha)
    assert cb.estado is EstadoCircuito.ABERTO
