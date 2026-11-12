import asyncio

import pytest
from comum.circuit_breaker import CircuitBreaker, CircuitoAbertoError, EstadoCircuito
from comum.config import ConfigCircuitBreaker
from comum.testes import MonotonicoFake

CONFIG = ConfigCircuitBreaker(
    janela_chamadas=10,
    limite_falhas_percentual=50,
    minimo_chamadas=4,
    tempo_aberto_ms=30_000,
    chamadas_meio_aberto=3,
    timeout_ms=200,
)


async def ok() -> str:
    return "ok"


async def falha() -> str:
    raise ConnectionError("provedor fora")


def criar() -> tuple[CircuitBreaker, MonotonicoFake, list[EstadoCircuito]]:
    relogio = MonotonicoFake()
    cb = CircuitBreaker("provedor", CONFIG, relogio=relogio)
    estados: list[EstadoCircuito] = []
    cb.ao_mudar_estado(estados.append)
    return cb, relogio, estados


async def abrir(cb: CircuitBreaker) -> None:
    for _ in range(CONFIG.minimo_chamadas):
        with pytest.raises(ConnectionError):
            await cb.executar(falha)


async def test_deixa_a_chamada_passar_e_devolve_o_resultado_com_o_circuito_fechado() -> None:
    cb, _, _ = criar()
    assert await cb.executar(ok) == "ok"
    assert cb.estado is EstadoCircuito.FECHADO
    assert cb.resumo() == {"nome": "provedor", "estado": "FECHADO", "falhas": 0, "sucessos": 1, "rejeitadas": 0}


async def test_nao_abre_antes_do_minimo_de_chamadas() -> None:
    cb, _, _ = criar()
    for _ in range(CONFIG.minimo_chamadas - 1):
        with pytest.raises(ConnectionError):
            await cb.executar(falha)
    assert cb.estado is EstadoCircuito.FECHADO


async def test_abre_quando_o_percentual_de_falhas_atinge_o_limite() -> None:
    cb, _, _ = criar()
    for _ in range(4):
        await cb.executar(ok)
    for _ in range(3):
        with pytest.raises(ConnectionError):
            await cb.executar(falha)
    assert cb.estado is EstadoCircuito.FECHADO  # 3 falhas em 7 chamadas = 43%
    with pytest.raises(ConnectionError):
        await cb.executar(falha)
    assert cb.estado is EstadoCircuito.ABERTO  # 4 falhas em 8 chamadas = 50%


async def test_janela_por_contagem_considera_so_as_ultimas_chamadas() -> None:
    cb, _, _ = criar()
    for _ in range(10):
        await cb.executar(ok)
    for _ in range(4):
        with pytest.raises(ConnectionError):
            await cb.executar(falha)
    assert cb.estado is EstadoCircuito.FECHADO  # janela: 6 sucessos e 4 falhas
    with pytest.raises(ConnectionError):
        await cb.executar(falha)
    assert cb.estado is EstadoCircuito.ABERTO  # janela: 5 sucessos e 5 falhas (as 15 chamadas dariam 33%)


async def test_abre_apos_atingir_o_percentual_de_falhas_e_rejeita_sem_chamar_o_provedor() -> None:
    cb, _, estados = criar()
    await abrir(cb)
    chamadas = 0

    async def contar() -> str:
        nonlocal chamadas
        chamadas += 1
        return "ok"

    with pytest.raises(CircuitoAbertoError) as erro:
        await cb.executar(contar)
    assert erro.value.circuit_breaker == "provedor"
    assert chamadas == 0
    assert estados == [EstadoCircuito.ABERTO]
    assert cb.resumo()["rejeitadas"] == 1


async def test_passa_para_meio_aberto_apos_o_tempo_aberto_e_fecha_quando_as_chamadas_de_teste_tem_sucesso() -> None:
    cb, relogio, estados = criar()
    await abrir(cb)
    relogio.avancar(29.9)
    assert cb.estado is EstadoCircuito.ABERTO
    relogio.avancar(0.1)
    assert cb.estado is EstadoCircuito.MEIO_ABERTO
    for _ in range(CONFIG.chamadas_meio_aberto):
        await cb.executar(ok)
    assert estados == [EstadoCircuito.ABERTO, EstadoCircuito.MEIO_ABERTO, EstadoCircuito.FECHADO]


async def test_volta_a_abrir_quando_as_chamadas_de_teste_no_meio_aberto_falham() -> None:
    cb, relogio, estados = criar()
    await abrir(cb)
    relogio.avancar(30)
    await cb.executar(ok)
    for _ in range(2):
        with pytest.raises(ConnectionError):
            await cb.executar(falha)
    assert cb.estado is EstadoCircuito.ABERTO
    assert estados[-2:] == [EstadoCircuito.MEIO_ABERTO, EstadoCircuito.ABERTO]


async def test_meio_aberto_libera_so_as_chamadas_de_teste_permitidas() -> None:
    cb, relogio, _ = criar()
    await abrir(cb)
    relogio.avancar(30)
    liberar = asyncio.Event()

    async def lenta() -> str:
        await liberar.wait()
        return "ok"

    em_andamento = [asyncio.create_task(cb.executar(lenta)) for _ in range(CONFIG.chamadas_meio_aberto)]
    await asyncio.sleep(0)
    with pytest.raises(CircuitoAbertoError):
        await cb.executar(ok)
    liberar.set()
    await asyncio.gather(*em_andamento)
    assert cb.estado is EstadoCircuito.FECHADO


async def test_passa_sozinho_para_meio_aberto_pelo_timer_e_avisa_os_ouvintes() -> None:
    cb = CircuitBreaker("timer", CONFIG.model_copy(update={"tempo_aberto_ms": 20}))
    estados: list[EstadoCircuito] = []
    cb.ao_mudar_estado(estados.append)
    await abrir(cb)
    await asyncio.sleep(0.05)
    assert estados == [EstadoCircuito.ABERTO, EstadoCircuito.MEIO_ABERTO]


async def test_conta_como_falha_a_chamada_que_excede_o_timeout() -> None:
    cb, _, _ = criar()

    async def lenta() -> str:
        await asyncio.sleep(1)
        return "tarde"

    with pytest.raises(TimeoutError, match="excedeu 200 ms"):
        await cb.executar(lenta)
    assert cb.resumo()["falhas"] == 1


async def test_erros_ignorados_nao_contam_como_falha() -> None:
    cb = CircuitBreaker("ignora", CONFIG, ignorar=(ValueError,))

    async def invalida() -> None:
        raise ValueError("dado inválido")

    for _ in range(10):
        with pytest.raises(ValueError):
            await cb.executar(invalida)
    assert cb.estado is EstadoCircuito.FECHADO
    assert cb.resumo()["falhas"] == 0


async def test_falha_de_um_ouvinte_nao_impede_a_transicao() -> None:
    cb, _, estados = criar()

    def quebrado(_: EstadoCircuito) -> None:
        raise RuntimeError("ouvinte com defeito")

    cb.ao_mudar_estado(quebrado)
    await abrir(cb)
    assert cb.estado is EstadoCircuito.ABERTO
    assert estados == [EstadoCircuito.ABERTO]
