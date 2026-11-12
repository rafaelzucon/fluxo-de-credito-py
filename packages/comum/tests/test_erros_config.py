from decimal import Decimal

import pytest
from comum.config import ConfigCircuitBreaker, brokers
from comum.erros import MensagemInvalidaError, descrever, ler_json
from comum.eventos import ModeloJson, Valor


def test_ler_json_le_o_objeto_quando_todos_os_campos_obrigatorios_estao_presentes() -> None:
    assert ler_json('{"a": 1, "b": "x"}', "Evento", ("a", "b")) == {"a": 1, "b": "x"}


@pytest.mark.parametrize(
    ("payload", "mensagem"),
    [("{nao é json", "Evento inválida"), ("[1, 2]", "Evento inválida"), ('{"a": null}', "sem os campos a, b")],
)
def test_ler_json_rejeita_json_malformado_valores_que_nao_sao_objeto_e_campos_ausentes(
    payload: str, mensagem: str
) -> None:
    with pytest.raises(MensagemInvalidaError, match=mensagem):
        ler_json(payload, "Evento", ("a", "b"))


def test_descrever_usa_o_nome_do_erro_e_a_mensagem() -> None:
    assert descrever(ConnectionError("recusada")) == "ConnectionError: recusada"


def test_config_do_circuit_breaker_usa_os_padroes_e_le_as_variaveis_de_ambiente(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert ConfigCircuitBreaker().janela_chamadas == 10
    monkeypatch.setenv("CB_TEMPO_ABERTO_MS", "1000")
    monkeypatch.setenv("CB_CHAMADAS_MEIO_ABERTO", "1")
    config = ConfigCircuitBreaker()
    assert (config.tempo_aberto_ms, config.chamadas_meio_aberto, config.limite_falhas_percentual) == (1000, 1, 50)


def test_config_falha_quando_a_variavel_numerica_e_invalida(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CB_JANELA_CHAMADAS", "dez")
    with pytest.raises(ValueError, match="janela_chamadas"):
        ConfigCircuitBreaker()


def test_brokers_converte_a_lista_separada_por_virgula() -> None:
    assert brokers(" kafka:9092, outro:9092 ,") == ["kafka:9092", "outro:9092"]


def test_modelo_json_usa_camel_case_e_serializa_valor_como_numero() -> None:
    class Evento(ModeloJson):
        solicitacao_id: str
        valor_solicitado: Valor

    evento = Evento.model_validate({"solicitacaoId": "s-1", "valorSolicitado": 1234.5})
    assert evento.valor_solicitado == Decimal("1234.5")
    assert evento.para_json() == '{"solicitacaoId":"s-1","valorSolicitado":1234.5}'
