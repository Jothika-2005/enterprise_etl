"""
Enterprise ETL Platform - Configuration Management

Centralized, environment-driven configuration using Pydantic BaseSettings.
All secrets are loaded from environment variables or .env files.
"""
from typing import List, Optional
from pydantic import AnyHttpUrl, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    Precedence: Environment Variables > .env file > defaults
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Application ────────────────────────────────────────────────────────
    APP_NAME: str = "Enterprise ETL Platform"
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"
    DEBUG: bool = False

    # ── Database ───────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql://etl_user:etl_pass@localhost:5432/etl_db"
    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30

    # ── Redis ──────────────────────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECONDS: int = 300

    # ── Airflow ────────────────────────────────────────────────────────────
    AIRFLOW_API_URL: str = "http://localhost:8080"
    AIRFLOW_USER: str = "airflow"
    AIRFLOW_PASSWORD: str = "airflow"

    # ── CORS ───────────────────────────────────────────────────────────────
    ALLOWED_ORIGINS: List[str] = ["http://localhost:8501", "http://localhost:3000"]

    # ── Alerting - Email ───────────────────────────────────────────────────
    SMTP_HOST: str = "smtp.gmail.com"
    SMTP_PORT: int = 587
    SMTP_USER: Optional[str] = None
    SMTP_PASSWORD: Optional[str] = None
    ALERT_EMAIL: str = "admin@example.com"
    EMAIL_ALERTS_ENABLED: bool = False

    # ── Alerting - Slack ───────────────────────────────────────────────────
    SLACK_WEBHOOK_URL: Optional[str] = None
    SLACK_ALERTS_ENABLED: bool = False

    # ── Data Quality ───────────────────────────────────────────────────────
    DEFAULT_QUALITY_THRESHOLD: float = 0.80
    MAX_NULL_RATIO: float = 0.20
    MAX_DUPLICATE_RATIO: float = 0.05

    # ── Pipeline ───────────────────────────────────────────────────────────
    MAX_RETRY_ATTEMPTS: int = 3
    RETRY_DELAY_SECONDS: int = 60
    CONFIG_DIR: str = "/app/configs"
    DATA_DIR: str = "/app/data"

    @field_validator("ENVIRONMENT")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"ENVIRONMENT must be one of {allowed}")
        return v

    @property
    def is_production(self) -> bool:
        """Returns True if running in production environment."""
        return self.ENVIRONMENT == "production"

    @property
    def db_url_sync(self) -> str:
        """Synchronous database URL for SQLAlchemy."""
        return self.DATABASE_URL

    @property
    def db_url_async(self) -> str:
        """Async database URL for SQLAlchemy."""
        return self.DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://")


# Singleton settings instance
settings = Settings()
