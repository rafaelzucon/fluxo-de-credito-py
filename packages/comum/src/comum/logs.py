"""Logs estruturados em JSON (structlog), um por linha, com o nome do serviço e do componente.

Os logs das bibliotecas (logging da stdlib: uvicorn, aiokafka...) passam pelo mesmo formatador JSON.
"""

import logging
import os
import sys

import structlog

_configurado = False


def configurar_logs() -> None:
    global _configurado
    if _configurado:
        return
    _configurado = True
    nivel = logging.getLevelNamesMapping().get(os.environ.get("LOG_LEVEL", "info").upper(), logging.INFO)
    comuns: list[structlog.typing.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    structlog.configure(
        processors=[*comuns, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        wrapper_class=structlog.make_filtering_bound_logger(nivel),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )
    formatador = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*comuns, structlog.stdlib.add_logger_name],
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(ensure_ascii=False, default=str),
        ],
    )
    saida = logging.StreamHandler(sys.stdout)
    saida.setFormatter(formatador)
    raiz = logging.getLogger()
    raiz.handlers[:] = [saida]
    raiz.setLevel(nivel)
    for barulhento in (
        "aiokafka",
        "kafka",
        "botocore",
        "aiobotocore",
        "httpx",
        "httpx2",
        "httpcore",
        "httpcore2",
        "uvicorn.access",
    ):
        logging.getLogger(barulhento).setLevel(max(nivel, logging.WARNING))
    for uvicorn in ("uvicorn", "uvicorn.error"):
        logging.getLogger(uvicorn).handlers.clear()
        logging.getLogger(uvicorn).propagate = True


def criar_logger(componente: str) -> structlog.typing.FilteringBoundLogger:
    configurar_logs()
    log: structlog.typing.FilteringBoundLogger = structlog.get_logger(componente).bind(componente=componente)
    servico = os.environ.get("SERVICO")
    return log.bind(servico=servico) if servico else log
