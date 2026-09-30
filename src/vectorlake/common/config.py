from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VECTORLAKE_",
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    environment: Literal["local", "test", "production"] = "local"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    service_name: str = "vectorlake-api"
    otlp_endpoint: str | None = None


@lru_cache
def get_settings() -> Settings:
    return Settings()
