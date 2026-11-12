# syntax=docker/dockerfile:1
# Uma imagem por serviço (targets solicitacao-service, notificacao-service e motor-credito-simulador).
# A etapa "verificacao" roda ruff, mypy e pytest; a imagem final só contém o ambiente virtual do serviço
# (sem código-fonte do workspace nem dependências de desenvolvimento).
ARG PYTHON_VERSION=3.15.2
ARG UV_VERSION=0.13.7

FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

FROM python:${PYTHON_VERSION}-slim AS base
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never UV_PYTHON=/usr/local/bin/python3
# Certificados extras (opcional): coloque arquivos .crt em docker/certs/ quando a rede usar um proxy com
# inspeção TLS. Sem arquivos, nada muda.
COPY docker/certs/ /tmp/certs/
RUN if ls /tmp/certs/*.crt >/dev/null 2>&1; then \
      cat /etc/ssl/certs/ca-certificates.crt /tmp/certs/*.crt > /etc/ssl/certs/bundle-extra.pem; \
    fi
WORKDIR /app
COPY pyproject.toml uv.lock .python-version ./
COPY packages/comum/pyproject.toml packages/comum/
COPY packages/solicitacao-service/pyproject.toml packages/solicitacao-service/
COPY packages/notificacao-service/pyproject.toml packages/notificacao-service/
COPY packages/motor-credito-simulador/pyproject.toml packages/motor-credito-simulador/

# ------------------------------------------------------------------ verificação (lint, tipos e testes)
FROM base AS verificacao
ARG SKIP_TESTS=false
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f /etc/ssl/certs/bundle-extra.pem ]; then export SSL_CERT_FILE=/etc/ssl/certs/bundle-extra.pem; fi; \
    uv sync --frozen --all-packages --no-install-workspace
COPY packages/ packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --all-packages \
    && if [ "$SKIP_TESTS" = "true" ]; then echo "Verificação ignorada"; else \
         .venv/bin/ruff check packages && .venv/bin/ruff format --check packages \
         && .venv/bin/mypy && .venv/bin/pytest -p no:cacheprovider; \
       fi \
    && touch /tmp/verificado

# ------------------------------------------------------------------ ambiente virtual de cada serviço
FROM base AS ambientes
ENV SERVICOS="solicitacao-service notificacao-service motor-credito-simulador"
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -f /etc/ssl/certs/bundle-extra.pem ]; then export SSL_CERT_FILE=/etc/ssl/certs/bundle-extra.pem; fi; \
    for s in $SERVICOS; do \
      UV_PROJECT_ENVIRONMENT=/opt/$s uv sync --frozen --no-dev --package "$s" --no-install-workspace || exit 1; \
    done
COPY packages/ packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    for s in $SERVICOS; do \
      UV_PROJECT_ENVIRONMENT=/opt/$s uv sync --frozen --no-dev --package "$s" --no-editable || exit 1; \
    done

FROM python:${PYTHON_VERSION}-slim AS runtime
RUN useradd --system --uid 10001 --no-create-home app
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
# Garante que a imagem só é gerada depois da verificação.
COPY --from=verificacao /tmp/verificado /tmp/verificado
USER app
ENV HEALTH_URL=http://localhost:8080/health
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=30 \
  CMD python -c "import os,sys,urllib.request as u; sys.exit(0 if u.urlopen(os.environ['HEALTH_URL'],timeout=4).status==200 else 1)"

FROM runtime AS solicitacao-service
COPY --from=ambientes /opt/solicitacao-service /opt/solicitacao-service
ENV SERVICO=solicitacao-service PORT=8080 HEALTH_URL=http://localhost:8080/health PATH=/opt/solicitacao-service/bin:$PATH
EXPOSE 8080
CMD ["solicitacao-service"]

FROM runtime AS notificacao-service
COPY --from=ambientes /opt/notificacao-service /opt/notificacao-service
ENV SERVICO=notificacao-service PORT=8081 HEALTH_URL=http://localhost:8081/health PATH=/opt/notificacao-service/bin:$PATH
EXPOSE 8081
CMD ["notificacao-service"]

FROM runtime AS motor-credito-simulador
COPY --from=ambientes /opt/motor-credito-simulador /opt/motor-credito-simulador
ENV SERVICO=motor-credito-simulador PORT=8082 HEALTH_URL=http://localhost:8082/health \
    PATH=/opt/motor-credito-simulador/bin:$PATH
EXPOSE 8082
CMD ["motor-credito-simulador"]
