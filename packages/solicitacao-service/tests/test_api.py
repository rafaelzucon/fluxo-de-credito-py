from typing import Any

import pytest
from comum.testes import BancoFake
from fastapi import FastAPI
from fastapi.testclient import TestClient
from solicitacao.api import BASE, MENSAGEM_PADRAO, registrar_rotas
from solicitacao.aplicacao.solicitacao_service import SolicitacaoService

from .apoio import PublicadorSolicitacaoFake, RepositorioMemoria

CORPO = {
    "clienteId": "cliente-123",
    "produtoCodigo": "CARTAO-GOLD",
    "valorSolicitado": 1500.50,
    "email": "cliente@exemplo.com.br",
    "tokenDispositivo": "token-abc",
}


@pytest.fixture
def cliente() -> TestClient:
    app = FastAPI()
    registrar_rotas(app)
    app.state.servico = SolicitacaoService(BancoFake(), RepositorioMemoria(), PublicadorSolicitacaoFake())
    return TestClient(app, raise_server_exceptions=False)


def test_aceita_a_solicitacao_com_202_location_e_a_mensagem_padrao(cliente: TestClient) -> None:
    resposta = cliente.post(BASE, json=CORPO)
    assert resposta.status_code == 202
    corpo = resposta.json()
    assert corpo["mensagem"] == MENSAGEM_PADRAO
    assert resposta.headers["location"] == f"{BASE}/{corpo['solicitacaoId']}"

    consulta = cliente.get(resposta.headers["location"]).json()
    assert consulta["status"] == "PENDENTE"
    assert consulta["valorSolicitado"] == 1500.5
    assert consulta["motivoDecisao"] is None


def test_responde_404_em_problem_json_quando_nao_existe(cliente: TestClient) -> None:
    resposta = cliente.get(f"{BASE}/inexistente")
    assert resposta.status_code == 404
    assert resposta.headers["content-type"] == "application/problem+json"
    assert resposta.json()["instance"] == f"{BASE}/inexistente"


@pytest.mark.parametrize(
    ("alteracao", "campo"),
    [
        ({"clienteId": None}, "clienteId"),
        ({"produtoCodigo": "   "}, "produtoCodigo"),
        ({"valorSolicitado": -10}, "valorSolicitado"),
        ({"valorSolicitado": 0}, "valorSolicitado"),
        ({"valorSolicitado": 10.123}, "valorSolicitado"),
        ({"valorSolicitado": 1e14}, "valorSolicitado"),
        ({"email": "nao-e-email"}, "email"),
        ({"tokenDispositivo": "x" * 513}, "tokenDispositivo"),
        ({"campoExtra": 1}, "campoExtra"),
    ],
)
def test_responde_400_quando_o_corpo_e_invalido(cliente: TestClient, alteracao: dict[str, Any], campo: str) -> None:
    corpo = {k: v for k, v in (CORPO | alteracao).items() if v is not None}
    resposta = cliente.post(BASE, json=corpo)
    assert resposta.status_code == 400
    assert resposta.headers["content-type"] == "application/problem+json"
    assert campo in resposta.json()["detail"]


def test_responde_400_quando_o_json_e_malformado(cliente: TestClient) -> None:
    resposta = cliente.post(BASE, content=b"{nao-json", headers={"content-type": "application/json"})
    assert resposta.status_code == 400


def test_responde_500_em_problem_json_sem_expor_o_erro(cliente: TestClient) -> None:
    async def falhar(*_: object) -> None:
        raise RuntimeError("segredo interno")

    cliente.app.state.servico.criar = falhar  # type: ignore[attr-defined]
    resposta = cliente.post(BASE, json=CORPO)
    assert resposta.status_code == 500
    assert resposta.json()["detail"] == "Erro inesperado"
