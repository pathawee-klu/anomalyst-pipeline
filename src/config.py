import os
from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # Pipeline Mode
    MOCK_MODE: bool = True
    SYMBOLS_STR: str = "AAPL,GOOGL,TSLA,NVDA"

    # Finnhub API Key
    FINNHUB_API_KEY: str = ""

    # LINE Messaging API
    LINE_CHANNEL_ACCESS_TOKEN: str = ""
    LINE_USER_ID: str = ""

    # Kafka Config
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_TOPIC_RAW_TICKS: str = "stock-raw-ticks"
    KAFKA_TOPIC_ANOMALIES: str = "stock-anomalies"

    # PostgreSQL / TimescaleDB Config
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5433
    POSTGRES_DB: str = "stock_monitoring"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"

    # Anomaly Detection Settings
    ANOMALY_PERCENT_CHANGE_THRESHOLD: float = 2.0
    ANOMALY_WINDOW_SECONDS: int = 60

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @property
    def SYMBOLS(self) -> List[str]:
        return [s.strip() for s in self.SYMBOLS_STR.split(",") if s.strip()]

settings = Settings()
