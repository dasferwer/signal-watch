from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

LIVE_RUN = "00000000-0000-0000-0000-000000000023"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://signal:signal@database:5432/signal"
    rabbitmq_url: str = "amqp://signal:signal@rabbitmq:5672/"
    redis_url: str = "redis://redis:6379/0"
    admin_token: str = Field(
        default="local-demo-signalwatch-admin-change-before-deployment", min_length=32
    )
    ingest_token: str = Field(
        default="local-demo-signalwatch-ingest-change-before-deployment", min_length=32
    )
    artifact_path: str = "artifacts"
    batch_size: int = Field(default=50, ge=1, le=200)
    worker_before_commit_delay: float = Field(default=0, ge=0, le=30)
    dispatcher_after_publish_delay: float = Field(default=0, ge=0, le=30)
    testing: bool = False


settings = Settings()
