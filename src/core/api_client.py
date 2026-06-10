# src/core/api_client.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ALPHAVANTAGE_BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageError(Exception):
    """Error base del cliente Alpha Vantage."""


@dataclass(frozen=True)
class ApiCallResult:
    symbol: str
    ok: bool
    status: str  # SUCCESS | RATE_LIMIT | ERROR
    payload: Dict[str, Any]
    fetched_at_utc: str
    error_message: Optional[str] = None
    request_url: Optional[str] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _redact_api_key(url: str) -> str:
    # Quita el apikey de la URL para no guardarlo en S3/logs
    return url.replace(f"apikey=", "apikey=<redacted>")

def _http_get_json(url: str, timeout: int = 30) -> Dict[str, Any]:
    """
    GET HTTP y parseo JSON usando librerías estándar (sin requests).
    Lanza AlphaVantageError si hay error de red/HTTP/JSON inválido.
    """
    req = Request(url, headers={"User-Agent": "tfg-aws-data/1.0"})
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except HTTPError as e:
        raise AlphaVantageError(f"HTTPError {e.code}: {e.reason}") from e
    except URLError as e:
        raise AlphaVantageError(f"URLError: {e.reason}") from e
    except Exception as e:
        raise AlphaVantageError(f"NetworkError: {type(e).__name__}: {e}") from e

    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise AlphaVantageError(f"Invalid JSON response: {type(e).__name__}: {e}") from e


def fetch_daily(
    api_key: str,
    symbol: str,
    outputsize: str = "compact",
    timeout: int = 30,
    function: str = "TIME_SERIES_DAILY",
) -> ApiCallResult:
    """
    Llama a Alpha Vantage (Daily) y devuelve un ApiCallResult.
    Detecta:
      - RATE LIMIT (campo 'Note')
      - Errores lógicos (campo 'Error Message' o 'Information')
      - Estructura inesperada (falta 'Meta Data' o 'Time Series (Daily)')
    """
    fetched_at = _utc_now_iso()
    symbol = (symbol or "").strip().upper()

    if not symbol:
        return ApiCallResult(
            symbol="",
            ok=False,
            status="ERROR",
            payload={},
            fetched_at_utc=fetched_at,
            error_message="Symbol vacío",
        )

    outputsize_norm = (outputsize or "").strip().lower()
    if outputsize_norm not in ("compact", "full"):
        outputsize_norm = "compact"

    params = {
        "function": function,
        "symbol": symbol,
        "outputsize": outputsize_norm,
        "apikey": api_key,
    }
    request_url = f"{ALPHAVANTAGE_BASE_URL}?{urlencode(params)}"

    # URL “segura” para guardar en metadata (sin exponer la key)
    params_safe = dict(params)
    params_safe["apikey"] = "<redacted>"
    safe_url = f"{ALPHAVANTAGE_BASE_URL}?{urlencode(params_safe)}"

    try:
        data = _http_get_json(request_url, timeout=timeout)
    except AlphaVantageError as e:
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="ERROR",
            payload={},
            fetched_at_utc=fetched_at,
            error_message=str(e),
            request_url=safe_url,
        )

    if not isinstance(data, dict):
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="ERROR",
            payload={},
            fetched_at_utc=fetched_at,
            error_message="Respuesta no es un objeto JSON",
            request_url=safe_url,
        )

    # Rate limit / throttling (Alpha Vantage suele devolver "Note")
    note = data.get("Note")
    if isinstance(note, str) and note.strip():
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="RATE_LIMIT",
            payload=data,
            fetched_at_utc=fetched_at,
            error_message=note.strip(),
            request_url=safe_url,
        )

    # Errores lógicos comunes
    err_msg = data.get("Error Message")
    info_msg = data.get("Information")

    if isinstance(err_msg, str) and err_msg.strip():
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="ERROR",
            payload=data,
            fetched_at_utc=fetched_at,
            error_message=err_msg.strip(),
            request_url=safe_url,
        )

    if isinstance(info_msg, str) and info_msg.strip():
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="ERROR",
            payload=data,
            fetched_at_utc=fetched_at,
            error_message=info_msg.strip(),
            request_url=safe_url,
        )

    # Validación mínima de estructura TIME_SERIES_DAILY
    ts_key = "Time Series (Daily)"
    if not (isinstance(data.get("Meta Data"), dict) and isinstance(data.get(ts_key), dict)):
        return ApiCallResult(
            symbol=symbol,
            ok=False,
            status="ERROR",
            payload=data,
            fetched_at_utc=fetched_at,
            error_message=f"Estructura inesperada: falta 'Meta Data' o '{ts_key}'",
            request_url=safe_url,
        )

    return ApiCallResult(
        symbol=symbol,
        ok=True,
        status="SUCCESS",
        payload=data,
        fetched_at_utc=fetched_at,
        error_message=None,
        request_url=safe_url,
    )


def build_bronze_like_payload(result: ApiCallResult) -> Dict[str, Any]:
    """
    Wrapper Bronze: trazabilidad + payload original (o payload de error).
    """
    return {
        "metadata": {
            "source": "alphavantage",
            "symbol": result.symbol,
            "fetched_at_utc": result.fetched_at_utc,
            "status": result.status,
            "error_message": result.error_message,
            "request_url": result.request_url,
        },
        "payload": result.payload,
    }


def save_json_to_file(obj: Dict[str, Any], filepath: str) -> None:
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)