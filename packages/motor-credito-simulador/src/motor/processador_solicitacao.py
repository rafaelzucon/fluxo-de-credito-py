from collections.abc import Awaitable, Callable

from comum.kafka import PublicadorKafka
from comum.logs import criar_logger

from motor.motor_decisao import SolicitacaoCriada, decidir

log = criar_logger("MotorCredito")


def criar_processador(kafka: PublicadorKafka, topico_decisao: str) -> Callable[[str], Awaitable[None]]:
    """Consome SolicitacaoCriada, decide e publica DecisaoTomada."""

    async def processar(payload: str) -> None:
        decisao = decidir(SolicitacaoCriada.ler(payload))
        await kafka.publicar(topico_decisao, decisao.solicitacao_id, decisao.para_json())
        log.info(
            "Solicitação decidida",
            solicitacao_id=decisao.solicitacao_id,
            decisao=decisao.decisao,
            motivo=decisao.motivo,
        )

    return processar
