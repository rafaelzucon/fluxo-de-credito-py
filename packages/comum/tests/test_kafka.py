import pytest
from comum.circuit_breaker import CircuitoAbertoError
from comum.erros import MensagemInvalidaError
from comum.kafka import (
    HEADER_ERRO,
    HEADER_PROCESSAR_APOS,
    HEADER_TENTATIVA,
    HEADER_TOPICO_ORIGINAL,
    ConsumidorComRetry,
    MensagemRecebida,
    OpcoesConsumidorComRetry,
    topicos_com_retry,
)
from comum.tempo import epoch_ms
from comum.testes import EsperaFake, PublicadorKafkaFake, RelogioFake


class Cenario:
    def __init__(self, *erros: BaseException) -> None:
        self.erros = list(erros)
        self.processadas: list[str] = []
        self.publicador = PublicadorKafkaFake()
        self.relogio = RelogioFake()
        self.espera = EsperaFake(self.relogio)
        self.consumidor = ConsumidorComRetry(
            OpcoesConsumidorComRetry(
                brokers=["kafka:9092"],
                grupo="grupo",
                topico="eventos",
                processar=self.processar,
                publicador=self.publicador,
                tentativas=4,
                intervalo_retry_s=5,
                intervalo_circuito_aberto_s=2,
                esperar=self.espera,
                relogio=self.relogio,
            )
        )

    async def processar(self, valor: str) -> None:
        self.processadas.append(valor)
        if self.erros:
            raise self.erros.pop(0)


def mensagem(headers: dict[str, str] | None = None) -> MensagemRecebida:
    return MensagemRecebida(topico="eventos", chave="s-1", valor='{"id":"s-1"}', headers=headers or {})


async def test_processa_a_mensagem_sem_publicar_em_retry_nem_dlt() -> None:
    c = Cenario()
    await c.consumidor.tratar(mensagem())
    assert c.processadas == ['{"id":"s-1"}']
    assert c.publicador.publicacoes == []


async def test_envia_para_o_topico_de_retry_com_a_proxima_tentativa_e_o_horario_de_reprocessamento() -> None:
    c = Cenario(ConnectionError("banco fora"))
    await c.consumidor.tratar(mensagem())
    [p] = c.publicador.publicacoes
    assert (p.topico, p.chave, p.valor) == ("eventos-retry", "s-1", '{"id":"s-1"}')
    assert p.headers == {
        HEADER_TOPICO_ORIGINAL: "eventos",
        HEADER_ERRO: "ConnectionError: banco fora",
        HEADER_TENTATIVA: "2",
        HEADER_PROCESSAR_APOS: str(epoch_ms(c.relogio()) + 5_000),
    }


async def test_envia_para_o_dead_letter_topic_quando_a_ultima_tentativa_falha() -> None:
    c = Cenario(ConnectionError("x" * 600))
    await c.consumidor.tratar(mensagem({HEADER_TENTATIVA: "4"}))
    [p] = c.publicador.publicacoes
    assert p.topico == "eventos-dlt"
    assert p.headers[HEADER_TENTATIVA] == "4"
    assert len(p.headers[HEADER_ERRO]) == 500


async def test_envia_mensagem_invalida_direto_para_o_dlt_sem_gastar_tentativas() -> None:
    c = Cenario(MensagemInvalidaError("sem solicitacaoId"))
    await c.consumidor.tratar(mensagem())
    [p] = c.publicador.publicacoes
    assert p.topico == "eventos-dlt"
    assert p.headers[HEADER_TENTATIVA] == "1"


async def test_com_o_circuit_breaker_aberto_espera_e_reprocessa_sem_ir_para_o_topico_de_retry() -> None:
    c = Cenario(CircuitoAbertoError("sns-publisher"), CircuitoAbertoError("sns-publisher"))
    await c.consumidor.tratar(mensagem())
    assert len(c.processadas) == 3
    assert c.espera.esperas == [2, 2]
    assert c.publicador.publicacoes == []


async def test_aguarda_o_horario_de_reprocessamento_da_mensagem_de_retry() -> None:
    c = Cenario()
    processar_apos = epoch_ms(c.relogio()) + 3_500
    await c.consumidor.tratar(mensagem({HEADER_TENTATIVA: "2", HEADER_PROCESSAR_APOS: str(processar_apos)}))
    assert c.espera.esperas == [3.5]
    assert len(c.processadas) == 1


async def test_propaga_o_erro_quando_nao_consegue_publicar_no_topico_de_retry() -> None:
    c = Cenario(ConnectionError("banco fora"))
    c.publicador.erro = ConnectionError("kafka fora")
    with pytest.raises(ConnectionError, match="kafka fora"):
        await c.consumidor.tratar(mensagem())


async def test_tratar_ate_conseguir_insiste_na_publicacao_sem_perder_a_mensagem() -> None:
    c = Cenario(ConnectionError("1"), ConnectionError("2"))
    c.publicador.erro = ConnectionError("kafka fora")
    publicar_original = c.publicador.publicar
    falhas = 0

    async def publicar_falhando_uma_vez(*args: object, **kwargs: object) -> None:
        nonlocal falhas
        if falhas == 0:
            falhas += 1
            raise ConnectionError("kafka fora")
        c.publicador.erro = None
        await publicar_original(*args, **kwargs)  # type: ignore[arg-type]

    c.publicador.publicar = publicar_falhando_uma_vez  # type: ignore[method-assign]
    await c.consumidor.tratar_ate_conseguir(mensagem())
    assert [p.topico for p in c.publicador.publicacoes] == ["eventos-retry"]
    assert 2 in c.espera.esperas


def test_marca_o_consumo_como_pausado_e_retomado() -> None:
    c = Cenario()
    assert not c.consumidor.pausado
    c.consumidor.pausar()
    assert c.consumidor.pausado
    c.consumidor.retomar()
    assert not c.consumidor.pausado


def test_topicos_com_retry_devolve_o_principal_o_de_retry_e_o_dead_letter_topic() -> None:
    assert topicos_com_retry("decisao-tomada") == ["decisao-tomada", "decisao-tomada-retry", "decisao-tomada-dlt"]
