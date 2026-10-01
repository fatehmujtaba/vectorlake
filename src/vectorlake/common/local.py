from pydantic_settings import BaseSettings, SettingsConfigDict


class LocalInfrastructureSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="VECTORLAKE_LOCAL_",
        env_file=".env",
        env_ignore_empty=True,
        extra="ignore",
    )

    aws_endpoint_url: str = "http://127.0.0.1:4566"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"
    postgres_host: str = "127.0.0.1"
    postgres_port: int = 7001
    postgres_username: str = "vectorlake"
    postgres_password: str = "vectorlake-local-only"
    postgres_database: str = "vectorlake"
    postgres_instance_id: str = "vectorlake-control"
    kafka_bootstrap_servers: str = "127.0.0.1:19092"
    api_token: str = "vectorlake-local-dev-token"
    api_source_id: str = "camera-1"
