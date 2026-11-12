from comum.circuit_breaker import CircuitBreaker
from comum.http import Saude, registrar_saude
from fastapi import FastAPI
from fastapi.testclient import TestClient


def criar(saudavel: bool) -> TestClient:
    async def verificar() -> bool:
        return saudavel

    app = FastAPI()
    registrar_saude(app, Saude({"oracle": verificar}, [CircuitBreaker("kafka-producer")]))
    return TestClient(app)


def test_health_responde_up_com_o_estado_dos_circuit_breakers() -> None:
    resposta = criar(saudavel=True).get("/health")
    assert resposta.status_code == 200
    assert resposta.json() == {
        "status": "UP",
        "componentes": {"oracle": "UP"},
        "circuitBreakers": {"kafka-producer": "FECHADO"},
    }


def test_health_responde_503_quando_um_componente_esta_fora() -> None:
    resposta = criar(saudavel=False).get("/health")
    assert resposta.status_code == 503
    assert resposta.json()["status"] == "DOWN"


def test_circuit_breakers_lista_o_resumo_de_cada_um() -> None:
    assert criar(saudavel=True).get("/circuit-breakers").json() == [
        {"nome": "kafka-producer", "estado": "FECHADO", "falhas": 0, "sucessos": 0, "rejeitadas": 0}
    ]
