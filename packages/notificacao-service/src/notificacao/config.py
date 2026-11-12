from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    porta: int = Field(default=8081, validation_alias="PORT")
    db_user: str = "notificacao"
    db_password: str = "notificacao"
    db_connect_string: str = "localhost:1521/FREEPDB1"

    aws_region: str = "us-east-1"
    aws_endpoint: str = "http://localhost:4566"
    fila_email: str = "fila-email"
    fila_push: str = "fila-push"

    smtp_host: str = "localhost"
    smtp_port: int = 1025
    email_remetente: str = "nao-responda@fluxocredito.com.br"

    push_url: str = "http://localhost:8089"
    push_timeout_ms: int = 3_000

    visibilidade_circuito_aberto_s: int = 30
    """Deve ser >= tempo em que o circuit breaker fica aberto (CB_TEMPO_ABERTO_MS)."""
