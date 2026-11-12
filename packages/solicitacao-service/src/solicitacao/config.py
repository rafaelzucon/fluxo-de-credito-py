from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(frozen=True)

    porta: int = Field(default=8080, validation_alias="PORT")
    db_user: str = "solicitacao"
    db_password: str = "solicitacao"
    db_connect_string: str = "localhost:1521/FREEPDB1"

    kafka_bootstrap_servers: str = "localhost:29092"
    topico_solicitacao_criada: str = "solicitacao-criada"
    topico_decisao_tomada: str = "decisao-tomada"
    grupo: str = "solicitacao-service"
    kafka_tentativas: int = 4
    """1 tentativa no tópico principal + 3 no tópico de retry; depois, dead letter topic."""
    kafka_intervalo_retry_ms: int = 5_000
    kafka_intervalo_circuito_aberto_ms: int = 5_000
    kafka_timeout_publicacao_ms: int = 10_000

    aws_region: str = "us-east-1"
    aws_endpoint: str = "http://localhost:4566"
    sns_topico_decisao_arn: str = "arn:aws:sns:us-east-1:000000000000:decisao-credito"

    republicacao_intervalo_ms: int = 15_000
