import os

import uvicorn
from fastapi import FastAPI

from comum.logs import configurar_logs


def rodar(app: FastAPI, porta_padrao: int) -> None:
    """Sobe o servidor HTTP. Recursos (pools, consumidores) são abertos e fechados no lifespan da aplicação,
    que o uvicorn executa no início e ao receber SIGTERM/SIGINT."""
    configurar_logs()
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.environ.get("PORT", porta_padrao)),
        log_config=None,
        access_log=False,
        lifespan="on",
        timeout_graceful_shutdown=10,
    )
