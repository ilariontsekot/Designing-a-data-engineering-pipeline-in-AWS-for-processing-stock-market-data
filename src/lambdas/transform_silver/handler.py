from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote_plus

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.s3_io import (
    build_layer_key,
    download_json_from_s3,
    get_latest_s3_key,
    upload_json_to_s3,
)

log = get_logger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_symbols(event: Dict[str, Any], default_symbols_csv: str) -> List[str]:
    raw = event.get("symbols")
    if raw is None or raw == "":
        raw = default_symbols_csv

    if isinstance(raw, list):
        symbols = [str(x).strip().upper() for x in raw if str(x).strip()]
    else:
        symbols = [s.strip().upper() for s in str(raw).split(",") if s.strip()]

    seen = set()
    out: List[str] = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _bronze_key_from_s3_event(event: Dict[str, Any]) -> Optional[str]:
    """
    Extrae la key del objeto S3 desde un evento de S3.
    La decodifica porque S3 puede enviarla URL-encoded.
    """
    try:
        records = event.get("Records", [])
        if not records:
            return None

        rec = records[0]
        if rec.get("eventSource") != "aws:s3":
            return None

        raw_key = rec["s3"]["object"]["key"]
        return unquote_plus(raw_key)
    except Exception:
        return None

def _symbol_from_s3_key(key: str) -> Optional[str]:
    """
    Extrae el símbolo desde una key tipo:
    bronze/raw/source=alphavantage/symbol=AAPL/2026/03/04/003005_560.json
    """
    parts = key.split("/")
    for part in parts:
        if part.startswith("symbol="):
            value = part.split("=", 1)[1].strip().upper()
            return value or None
    return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(str(value).strip())
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _validate_record(record: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []

    if not record.get("symbol"):
        errors.append("missing_symbol")

    if not record.get("date"):
        errors.append("missing_date")

    for field in ["open", "high", "low", "close"]:
        if record.get(field) is None:
            errors.append(f"invalid_{field}")

    if record.get("volume") is None:
        errors.append("invalid_volume")

    open_v = record.get("open")
    high_v = record.get("high")
    low_v = record.get("low")
    close_v = record.get("close")
    volume_v = record.get("volume")

    for field_name, value in [
        ("open", open_v),
        ("high", high_v),
        ("low", low_v),
        ("close", close_v),
    ]:
        if value is not None and value <= 0:
            errors.append(f"non_positive_{field_name}")

    if volume_v is not None and volume_v < 0:
        errors.append("negative_volume")

    if high_v is not None and low_v is not None and high_v < low_v:
        errors.append("high_lower_than_low")

    if low_v is not None and open_v is not None and low_v > open_v:
        errors.append("low_greater_than_open")

    if low_v is not None and close_v is not None and low_v > close_v:
        errors.append("low_greater_than_close")

    if high_v is not None and open_v is not None and high_v < open_v:
        errors.append("high_lower_than_open")

    if high_v is not None and close_v is not None and high_v < close_v:
        errors.append("high_lower_than_close")

    return len(errors) == 0, errors


def _transform_bronze_to_rows(
    bronze_doc: Dict[str, Any],
    bronze_s3_key: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    metadata = bronze_doc.get("metadata", {}) or {}
    payload = bronze_doc.get("payload", {}) or {}
    api_meta = payload.get("Meta Data", {}) or {}
    ts = payload.get("Time Series (Daily)", {}) or {}

    source = metadata.get("source", "alphavantage")
    symbol = metadata.get("symbol") or api_meta.get("2. Symbol")
    fetched_at_utc = metadata.get("fetched_at_utc")
    run_id = metadata.get("run_id")
    lambda_request_id = metadata.get("lambda_request_id")
    api_last_refreshed = api_meta.get("3. Last Refreshed")
    api_timezone = api_meta.get("5. Time Zone")

    valid_rows: List[Dict[str, Any]] = []
    invalid_rows: List[Dict[str, Any]] = []

    if not isinstance(ts, dict) or not ts:
        invalid_rows.append(
            {
                "source": source,
                "symbol": symbol,
                "date": None,
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": None,
                "fetched_at_utc": fetched_at_utc,
                "api_last_refreshed": api_last_refreshed,
                "api_timezone": api_timezone,
                "run_id": run_id,
                "lambda_request_id": lambda_request_id,
                "bronze_s3_key": bronze_s3_key,
                "processed_at_utc": _utc_now_iso(),
                "dq_errors": ["missing_time_series_daily"],
            }
        )
        return valid_rows, invalid_rows

    seen_dates = set()

    for date_str, values in sorted(ts.items(), reverse=True):
        if date_str in seen_dates:
            invalid_rows.append(
                {
                    "source": source,
                    "symbol": symbol,
                    "date": date_str,
                    "open": None,
                    "high": None,
                    "low": None,
                    "close": None,
                    "volume": None,
                    "fetched_at_utc": fetched_at_utc,
                    "api_last_refreshed": api_last_refreshed,
                    "api_timezone": api_timezone,
                    "run_id": run_id,
                    "lambda_request_id": lambda_request_id,
                    "bronze_s3_key": bronze_s3_key,
                    "processed_at_utc": _utc_now_iso(),
                    "dq_errors": ["duplicate_date"],
                }
            )
            continue

        seen_dates.add(date_str)

        row = {
            "source": source,
            "symbol": symbol,
            "date": date_str,
            "open": _safe_float(values.get("1. open")),
            "high": _safe_float(values.get("2. high")),
            "low": _safe_float(values.get("3. low")),
            "close": _safe_float(values.get("4. close")),
            "volume": _safe_int(values.get("5. volume")),
            "fetched_at_utc": fetched_at_utc,
            "api_last_refreshed": api_last_refreshed,
            "api_timezone": api_timezone,
            "run_id": run_id,
            "lambda_request_id": lambda_request_id,
            "bronze_s3_key": bronze_s3_key,
            "processed_at_utc": _utc_now_iso(),
        }

        is_valid, dq_errors = _validate_record(row)
        if is_valid:
            valid_rows.append(row)
        else:
            bad_row = dict(row)
            bad_row["dq_errors"] = dq_errors
            invalid_rows.append(bad_row)

    return valid_rows, invalid_rows


def _build_silver_document(
    symbol: str,
    source: str,
    bronze_s3_key: str,
    rows: List[Dict[str, Any]],
    status: str,
    failed_reason: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "metadata": {
            "layer": "silver",
            "source": source,
            "symbol": symbol,
            "status": status,
            "bronze_s3_key": bronze_s3_key,
            "processed_at_utc": _utc_now_iso(),
            "row_count": len(rows),
            "failed_reason": failed_reason,
        },
        "records": rows,
    }


def handler(event: Optional[Dict[str, Any]] = None, context: Any = None) -> Dict[str, Any]:
    t0 = time.time()
    event = event or {}

    s = get_settings()
    run_id = str(uuid.uuid4())
    request_id = getattr(context, "aws_request_id", None)

    event_bronze_key = _bronze_key_from_s3_event(event)

    # Caso 1: invocación automática desde S3 -> procesar SOLO ese fichero
    if event_bronze_key:
        bronze_key = event_bronze_key
        symbol = _symbol_from_s3_key(bronze_key)

        if not symbol:
            return {
                "ok": False,
                "run_id": run_id,
                "request_id": request_id,
                "error": "Could not extract symbol from S3 key",
                "bronze_key": bronze_key,
                "elapsed_ms": int((time.time() - t0) * 1000),
            }

        symbols = [symbol]

    # Caso 2: test manual -> usar bronze_key o último por símbolo
    else:
        symbols = _parse_symbols(event, s.DEFAULT_SYMBOLS)
        bronze_key = event.get("bronze_key")

        if not symbols:
            return {
                "ok": False,
                "run_id": run_id,
                "error": "No symbols provided",
                "count": 0,
                "results": [],
                "elapsed_ms": 0,
            }

    log.info(
        "Start Silver | run_id=%s | request_id=%s | symbols=%s | bucket=%s | event_bronze_key=%s",
        run_id,
        request_id,
        symbols,
        s.S3_BRONZE_BUCKET,
        event_bronze_key,
    )

    results: List[Dict[str, Any]] = []

    for idx, symbol in enumerate(symbols, start=1):
        try:
            current_bronze_key = bronze_key

            if not current_bronze_key:
                bronze_prefix = f"{s.BRONZE_PREFIX}/raw/source=alphavantage/symbol={symbol}"
                current_bronze_key = get_latest_s3_key(s.S3_BRONZE_BUCKET, bronze_prefix)

            if not current_bronze_key:
                results.append(
                    {
                        "symbol": symbol,
                        "status": "ERROR",
                        "message": "No Bronze file found",
                        "bronze_key": None,
                    }
                )
                log.error("[%s/%s] No Bronze found | symbol=%s", idx, len(symbols), symbol)
                continue

            bronze_doc = download_json_from_s3(s.S3_BRONZE_BUCKET, current_bronze_key)
            valid_rows, invalid_rows = _transform_bronze_to_rows(bronze_doc, current_bronze_key)

            source = bronze_doc.get("metadata", {}).get("source", "alphavantage")

            clean_prefix = f"{s.SILVER_PREFIX}/clean/source={source}/symbol={symbol}"
            failed_prefix = f"{s.SILVER_PREFIX}/failed/source={source}/symbol={symbol}"

            clean_key = None
            failed_key = None

            if valid_rows:
                clean_doc = _build_silver_document(
                    symbol=symbol,
                    source=source,
                    bronze_s3_key=current_bronze_key,
                    rows=valid_rows,
                    status="SUCCESS",
                )
                clean_key = build_layer_key(clean_prefix)
                upload_json_to_s3(clean_doc, s.S3_BRONZE_BUCKET, clean_key)

            if invalid_rows:
                failed_doc = _build_silver_document(
                    symbol=symbol,
                    source=source,
                    bronze_s3_key=current_bronze_key,
                    rows=invalid_rows,
                    status="FAILED_PARTIAL" if valid_rows else "FAILED",
                    failed_reason="dq_validation_errors",
                )
                failed_key = build_layer_key(failed_prefix)
                upload_json_to_s3(failed_doc, s.S3_BRONZE_BUCKET, failed_key)

            results.append(
                {
                    "symbol": symbol,
                    "status": "OK" if valid_rows else "ERROR",
                    "bronze_key": current_bronze_key,
                    "silver_clean_key": clean_key,
                    "silver_failed_key": failed_key,
                    "rows_valid": len(valid_rows),
                    "rows_invalid": len(invalid_rows),
                }
            )

            log.info(
                "[%s/%s] Silver done | symbol=%s | valid=%s | invalid=%s | bronze_key=%s",
                idx,
                len(symbols),
                symbol,
                len(valid_rows),
                len(invalid_rows),
                current_bronze_key,
            )

        except Exception as e:
            results.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "message": f"{type(e).__name__}: {e}",
                }
            )
            log.exception("[%s/%s] Silver exception | symbol=%s", idx, len(symbols), symbol)

    elapsed_ms = int((time.time() - t0) * 1000)
    ok = len(results) > 0 and all(r["status"] == "OK" for r in results)

    log.info(
        "Done Silver | ok=%s | run_id=%s | count=%s | elapsed_ms=%s",
        ok,
        run_id,
        len(results),
        elapsed_ms,
    )

    return {
        "ok": ok,
        "run_id": run_id,
        "request_id": request_id,
        "count": len(results),
        "results": results,
        "elapsed_ms": elapsed_ms,
    }


lambda_handler = handler