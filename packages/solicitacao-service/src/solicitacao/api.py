from datetime import datetime
from decimal import Decimal
from typing import Annotated

from comum.eventos import ModeloJson, Valor
from comum.logs import criar_logger
from fastapi import APIRouter, Depends, FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field, StringConstraints
from pydantic.alias_generators import to_camel

from solicitacao.aplicacao.solicitacao_service import SolicitacaoService
from solicitacao.dominio import DadosNovaSolicitacao, Solicitacao, SolicitacaoNaoEncontradaError, StatusSolicitacao

log = criar_logger("api")

MENSAGEM_PADRAO = (
    "Recebemos sua solicitação de crédito. Em breve você receberá a resposta por e-mail e por notificação no app."
)
BASE = "/api/v1/solicitacoes"


type Texto64 = Annotated[str, StringConstraints(min_length=1, max_length=64, pattern=r"\S")]
type Texto512 = Annotated[str, StringConstraints(min_length=1, max_length=512, pattern=r"\S")]


class NovaSolicitacaoRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, extra="forbid", frozen=True)

    cliente_id: Texto64
    produto_codigo: Texto64
    valor_solicitado: Annotated[Decimal, Field(gt=0, max_digits=15, decimal_places=2)]
    """No máximo 13 dígitos inteiros e 2 casas decimais (NUMBER(15,2))."""
    email: Annotated[EmailStr, Field(max_length=254)]
    token_dispositivo: Texto512

    def para_dados(self) -> DadosNovaSolicitacao:
        return DadosNovaSolicitacao(
            cliente_id=self.cliente_id,
            produto_codigo=self.produto_codigo,
            valor_solicitado=self.valor_solicitado,
            email=self.email,
            token_dispositivo=self.token_dispositivo,
        )


class SolicitacaoRecebidaResponse(ModeloJson):
    solicitacao_id: str
    mensagem: str


class SolicitacaoResponse(ModeloJson):
    solicitacao_id: str
    cliente_id: str
    produto_codigo: str
    valor_solicitado: Valor
    status: StatusSolicitacao
    motivo_decisao: str | None
    criado_em: datetime
    atualizado_em: datetime

    @classmethod
    def de(cls, s: Solicitacao) -> SolicitacaoResponse:
        return cls(
            solicitacao_id=s.id,
            cliente_id=s.cliente_id,
            produto_codigo=s.produto_codigo,
            valor_solicitado=s.valor_solicitado,
            status=s.status,
            motivo_decisao=s.motivo_decisao,
            criado_em=s.criado_em,
            atualizado_em=s.atualizado_em,
        )


def _servico(request: Request) -> SolicitacaoService:
    servico: SolicitacaoService = request.app.state.servico
    return servico


Servico = Annotated[SolicitacaoService, Depends(_servico)]

rotas = APIRouter(prefix=BASE, tags=["solicitações"])


@rotas.post("", status_code=202, response_model_by_alias=True)
async def criar(corpo: NovaSolicitacaoRequest, servico: Servico, response: Response) -> SolicitacaoRecebidaResponse:
    solicitacao = await servico.criar(corpo.para_dados())
    response.headers["Location"] = f"{BASE}/{solicitacao.id}"
    return SolicitacaoRecebidaResponse(solicitacao_id=solicitacao.id, mensagem=MENSAGEM_PADRAO)


@rotas.get("/{id}")
async def consultar(id: str, servico: Servico) -> SolicitacaoResponse:
    return SolicitacaoResponse.de(await servico.consultar(id))


def problema(status: int, titulo: str, detalhe: str, request: Request) -> JSONResponse:
    """RFC 9457 (problem+json), como o ProblemDetail do Spring."""
    return JSONResponse(
        {"type": "about:blank", "title": titulo, "status": status, "detail": detalhe, "instance": request.url.path},
        status_code=status,
        media_type="application/problem+json",
    )


def registrar_rotas(app: FastAPI) -> None:
    app.include_router(rotas)

    @app.exception_handler(SolicitacaoNaoEncontradaError)
    async def nao_encontrada(request: Request, erro: SolicitacaoNaoEncontradaError) -> JSONResponse:
        return problema(404, "Not Found", str(erro), request)

    @app.exception_handler(RequestValidationError)
    async def invalida(request: Request, erro: RequestValidationError) -> JSONResponse:
        detalhe = "; ".join(f"{'/'.join(map(str, e['loc']))}: {e['msg']}" for e in erro.errors())
        return problema(400, "Bad Request", detalhe, request)

    @app.exception_handler(Exception)
    async def inesperado(request: Request, erro: Exception) -> JSONResponse:
        log.exception("Erro inesperado", caminho=request.url.path)
        return problema(500, "Internal Server Error", "Erro inesperado", request)
