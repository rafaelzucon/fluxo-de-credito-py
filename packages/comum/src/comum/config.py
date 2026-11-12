"""Configuração comum lida das variáveis de ambiente (pydantic-settings)."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ConfigCircuitBreaker(BaseSettings):
    """Padrões equivalentes ao Resilience4j (janela por contagem)."""

    model_config = SettingsConfigDict(env_prefix="CB_", frozen=True)

    janela_chamadas: int = Field(default=10, ge=1)
    """Tamanho da janela deslizante (em número de chamadas)."""
    limite_falhas_percentual: float = Field(default=50, gt=0, le=100)
    """Percentual de falhas na janela que abre o circuito."""
    minimo_chamadas: int = Field(default=5, ge=1)
    """Mínimo de chamadas na janela antes de avaliar o percentual de falhas."""
    tempo_aberto_ms: int = Field(default=30_000, ge=1)
    """Tempo em que o circuito fica ABERTO antes de passar sozinho para MEIO_ABERTO."""
    chamadas_meio_aberto: int = Field(default=3, ge=1)
    """Chamadas de teste permitidas em MEIO_ABERTO."""
    timeout_ms: int = Field(default=5_000, ge=1)
    """Chamadas mais lentas que isso são interrompidas e contadas como falha."""


def brokers(valor: str) -> list[str]:
    """Converte "host1:9092,host2:9092" em lista de brokers."""
    return [b.strip() for b in valor.split(",") if b.strip()]
