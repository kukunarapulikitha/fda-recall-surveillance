"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://fda_user:fda_pass@localhost:5432/fda_recalls"

    # FDA API
    FDA_API_KEY: str = ""
    FDA_BASE_URL: str = "https://api.fda.gov"
    FDA_WEBSITE_URL: str = "https://www.fda.gov/safety/recalls-market-withdrawals-safety-alerts"

    # Pipeline settings
    BACKFILL_START_YEAR: int = 2020
    DAILY_LOOKBACK_DAYS: int = 7
    BATCH_SIZE: int = 500
    REQUESTS_PER_MINUTE: int = 200

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


settings = Settings()
