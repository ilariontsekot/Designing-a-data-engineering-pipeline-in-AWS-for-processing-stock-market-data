from __future__ import annotations

import os
from dataclasses import dataclass


def _maybe_load_dotenv() -> None:
    """
    En local cargará .env si python-dotenv está instalado.
    En Lambda no hace falta; y si no está instalado, no pasa nada.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv()
    except Exception:
        pass


@dataclass(frozen=True)
class Settings:
    ALPHAVANTAGE_API_KEY: str
    S3_BRONZE_BUCKET: str

    BRONZE_PREFIX: str = "bronze"
    SILVER_PREFIX: str = "silver"

    DEFAULT_SYMBOLS: str = "AAPL"
    LOG_LEVEL: str = "INFO"


def get_settings() -> Settings:
    _maybe_load_dotenv()

    api_key = os.getenv("ALPHAVANTAGE_API_KEY")
    bucket = os.getenv("S3_BRONZE_BUCKET")

    if not api_key:
        raise ValueError("Missing env var ALPHAVANTAGE_API_KEY")
    if not bucket:
        raise ValueError("Missing env var S3_BRONZE_BUCKET")

    return Settings(
        ALPHAVANTAGE_API_KEY=api_key,
        S3_BRONZE_BUCKET=bucket,
        BRONZE_PREFIX=os.getenv("BRONZE_PREFIX", "bronze"),
        SILVER_PREFIX=os.getenv("SILVER_PREFIX", "silver"),
        DEFAULT_SYMBOLS=os.getenv("DEFAULT_SYMBOLS", "AAPL"),
        LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO"),
    )