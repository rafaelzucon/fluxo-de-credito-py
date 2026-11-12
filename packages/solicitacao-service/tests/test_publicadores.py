import json
from typing import Any

from apoio_solicitacao import solicitacao
from comum.circuit_breaker import CircuitBreaker
from comum.testes import PublicadorKafkaFake
from solicitacao.eventos import DecisaoCredito, DecisaoTomada, SolicitacaoCriada
from solicitacao.infra.publicadores import PublicadorDecisaoSns, PublicadorSolicitacaoCriadaKafka


async def test_publica_solicitacao_criada_em_camel_case_com_a_chave_igual_ao_id() -> None:
    kafka = PublicadorKafkaFake()
    cb = CircuitBreaker("kafka-producer")
    await PublicadorSolicitacaoCriadaKafka(kafka, cb, "solicitacao-criada").publicar(
        SolicitacaoCriada.de(solicitacao())
    )
    [p] = kafka.publicacoes
    assert (p.topico, p.chave) == ("solicitacao-criada", "sol-1")
    corpo = json.loads(p.valor)
    assert corpo["valorSolicitado"] == 1500.5
    assert corpo["criadoEm"] == "2025-01-02T03:04:05.678000Z"
    assert cb.resumo()["sucessos"] == 1


async def test_publica_a_decisao_no_topico_sns() -> None:
    chamadas: list[dict[str, Any]] = []

    class SnsFake:
        async def publish(self, **kwargs: Any) -> None:
            chamadas.append(kwargs)

    decisao = DecisaoTomada.ler('{"eventoId":"e","solicitacaoId":"sol-1","decisao":"NEGADA","motivo":"Score"}')
    arn = "arn:aws:sns:us-east-1:000000000000:decisao-credito"
    await PublicadorDecisaoSns(SnsFake(), CircuitBreaker("sns-publisher"), arn).publicar(  # type: ignore[arg-type]
        DecisaoCredito.de(solicitacao(), decisao)
    )
    [chamada] = chamadas
    assert chamada["TopicArn"] == arn
    assert json.loads(chamada["Message"])["decisao"] == "NEGADA"
