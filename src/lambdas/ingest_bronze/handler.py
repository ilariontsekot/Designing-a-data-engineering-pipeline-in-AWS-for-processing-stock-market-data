# src/lambdas/ingest_bronze/handler.py
from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

from src.core.api_client import build_bronze_like_payload, fetch_daily
from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.s3_io import build_bronze_key, upload_json_to_s3

log = get_logger(__name__)

# ---- CONFIG “safe” ----
BASE_SLEEP_BETWEEN_SYMBOLS_SEC = 15
MAX_RETRIES = 3
BACKOFF_BASE_SEC = 2
RATE_LIMIT_COOLDOWN_SEC = 60


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_today_parts() -> Tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")


def _parse_symbols(event: Dict[str, Any], default_symbols_csv: str) -> List[str]:
    raw = event.get("symbols")
    if raw is None or raw == "":
        raw = default_symbols_csv

    if isinstance(raw, list):
        symbols = [str(x).strip().upper() for x in raw if str(x).strip()]
    else:
        symbols = [s.strip().upper() for s in str(raw).split(",") if s.strip()]

    # de-dup conservando orden
    seen = set()
    out: List[str] = []
    for s in symbols:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _parse_outputsize(event: Dict[str, Any]) -> str:
    val = str(event.get("outputsize", "compact")).strip().lower()
    return val if val in ("compact", "full") else "compact"


def _sleep_with_jitter(seconds: float) -> None:
    time.sleep(max(0.0, seconds + random.uniform(0.0, 0.5)))


def _fetch_daily_with_retries(api_key: str, symbol: str, outputsize: str):
    """
    Reintenta especialmente si hay RATE_LIMIT.
    Devuelve el último result.
    """
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        last = fetch_daily(api_key=api_key, symbol=symbol, outputsize=outputsize)

        if last.ok:
            return last

        if last.status == "RATE_LIMIT":
            wait = RATE_LIMIT_COOLDOWN_SEC if attempt < MAX_RETRIES else 0
            log.warning(
                "RATE_LIMIT | symbol=%s | attempt=%s/%s | sleeping=%ss",
                symbol, attempt, MAX_RETRIES, wait
            )
            if wait:
                _sleep_with_jitter(wait)
            continue

        # otros errores: backoff corto para no spamear
        if attempt < MAX_RETRIES:
            backoff = BACKOFF_BASE_SEC * (2 ** (attempt - 1))
            log.warning(
                "RETRYABLE_ERROR | symbol=%s | status=%s | attempt=%s/%s | sleeping=%ss",
                symbol, getattr(last, "status", None), attempt, MAX_RETRIES, backoff
            )
            _sleep_with_jitter(backoff)

    return last


def _s3_prefix_has_any_object(bucket: str, prefix: str) -> bool:
    """
    True si existe al menos 1 objeto en ese prefix.
    Usamos MaxKeys=1 para que sea barato.
    """
    s3 = boto3.client("s3")
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(resp.get("Contents"))


def _already_ingested_today(bucket: str, bronze_prefix: str, symbol: str) -> bool:
    """
    Idempotencia: si ya existe cualquier objeto hoy para ese símbolo en raw/,
    no volvemos a llamar a la API.
    """
    y, m, d = _utc_today_parts()
    prefix_today = f"{bronze_prefix}/raw/source=alphavantage/symbol={symbol}/{y}/{m}/{d}/"
    return _s3_prefix_has_any_object(bucket, prefix_today)


def handler(event: Optional[Dict[str, Any]] = None, context: Any = None) -> Dict[str, Any]:
    t0 = time.time()
    event = event or {}

    s = get_settings()

    run_id = str(event.get("run_id") or uuid.uuid4())
    symbols = _parse_symbols(event, s.DEFAULT_SYMBOLS)
    outputsize = _parse_outputsize(event)

    request_id = getattr(context, "aws_request_id", None)
    started_at = _utc_now_iso()

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
        "Start | run_id=%s | request_id=%s | symbols=%s | outputsize=%s | bucket=%s | bronze_prefix=%s",
        run_id, request_id, symbols, outputsize, s.S3_BRONZE_BUCKET, s.BRONZE_PREFIX
    )

    results: List[Dict[str, Any]] = []
    stopped_early_reason: Optional[str] = None

    for idx, symbol in enumerate(symbols, start=1):
        key: Optional[str] = None

        # ---- IDEMPOTENCIA ----
        try:
            if _already_ingested_today(s.S3_BRONZE_BUCKET, s.BRONZE_PREFIX, symbol):
                results.append({"symbol": symbol, "status": "SKIPPED_ALREADY_INGESTED_TODAY"})
                log.info("[%s/%s] SKIP | symbol=%s | already ingested today", idx, len(symbols), symbol)
                _sleep_with_jitter(1.0)
                continue
        except Exception:
            log.exception("[%s/%s] WARN | idempotency check failed | symbol=%s", idx, len(symbols), symbol)

        try:
            # ---- FETCH CON RETRIES ----
            result = _fetch_daily_with_retries(
                api_key=s.ALPHAVANTAGE_API_KEY,
                symbol=symbol,
                outputsize=outputsize,
            )

            bronze_doc = build_bronze_like_payload(result=result)

            # contexto de ejecución (auditoría)
            bronze_doc.setdefault("metadata", {})
            bronze_doc["metadata"].update(
                {
                    "run_id": run_id,
                    "lambda_request_id": request_id,
                    "started_at_utc": started_at,
                }
            )

            if result and result.ok:
                prefix = f"{s.BRONZE_PREFIX}/raw/source=alphavantage/symbol={symbol}"
                key = build_bronze_key(prefix=prefix)
                upload_json_to_s3(bronze_doc, s.S3_BRONZE_BUCKET, key)

                results.append({"symbol": symbol, "status": "OK", "api_status": result.status, "s3_key": key})
                log.info("[%s/%s] OK | symbol=%s | key=%s", idx, len(symbols), symbol, key)

                if idx < len(symbols):
                    _sleep_with_jitter(BASE_SLEEP_BETWEEN_SYMBOLS_SEC)
                continue

            # Si tras retries sigue RATE_LIMIT: guardamos failed y paramos (lo más seguro)
            if result and result.status == "RATE_LIMIT":
                prefix = f"{s.BRONZE_PREFIX}/failed/source=alphavantage/symbol={symbol}"
                key = build_bronze_key(prefix=prefix)
                upload_json_to_s3(bronze_doc, s.S3_BRONZE_BUCKET, key)

                results.append(
                    {
                        "symbol": symbol,
                        "status": "ERROR_RATE_LIMIT",
                        "api_status": result.status,
                        "s3_key": key,
                        "message": result.error_message,
                    }
                )
                stopped_early_reason = "RATE_LIMIT"
                log.error(
                    "[%s/%s] RATE_LIMIT_FINAL | symbol=%s | key=%s | msg=%s",
                    idx, len(symbols), symbol, key, result.error_message
                )
                break

            # Error normal: guardamos failed y seguimos
            prefix = f"{s.BRONZE_PREFIX}/failed/source=alphavantage/symbol={symbol}"
            key = build_bronze_key(prefix=prefix)
            upload_json_to_s3(bronze_doc, s.S3_BRONZE_BUCKET, key)

            results.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "api_status": getattr(result, "status", None),
                    "s3_key": key,
                    "message": getattr(result, "error_message", "Unknown error"),
                }
            )
            log.error(
                "[%s/%s] ERROR | symbol=%s | key=%s | msg=%s",
                idx, len(symbols), symbol, key, getattr(result, "error_message", "Unknown error")
            )

            if idx < len(symbols):
                _sleep_with_jitter(BASE_SLEEP_BETWEEN_SYMBOLS_SEC)

        except Exception as e:
            err_doc = {
                "metadata": {
                    "source": "alphavantage",
                    "symbol": symbol,
                    "fetched_at_utc": _utc_now_iso(),
                    "status": "ERROR",
                    "error_message": f"{type(e).__name__}: {e}",
                    "run_id": run_id,
                    "lambda_request_id": request_id,
                    "started_at_utc": started_at,
                },
                "payload": None,
            }
            prefix = f"{s.BRONZE_PREFIX}/failed/source=alphavantage/symbol={symbol}"
            key = build_bronze_key(prefix=prefix)
            upload_json_to_s3(err_doc, s.S3_BRONZE_BUCKET, key)

            results.append(
                {
                    "symbol": symbol,
                    "status": "ERROR",
                    "api_status": None,
                    "s3_key": key,
                    "message": f"{type(e).__name__}: {e}",
                }
            )
            log.exception("[%s/%s] EXCEPTION | symbol=%s | key=%s", idx, len(symbols), symbol, key)

            if idx < len(symbols):
                _sleep_with_jitter(BASE_SLEEP_BETWEEN_SYMBOLS_SEC)

    elapsed_ms = int((time.time() - t0) * 1000)
    ok = len(results) > 0 and all(r["status"] in ("OK", "SKIPPED_ALREADY_INGESTED_TODAY") for r in results)

    log.info(
        "Done | ok=%s | run_id=%s | count=%s | elapsed_ms=%s | stopped_early_reason=%s",
        ok, run_id, len(results), elapsed_ms, stopped_early_reason
    )

    return {
        "ok": ok,
        "run_id": run_id,
        "request_id": request_id,
        "outputsize": outputsize,
        "count": len(results),
        "results": results,
        "stopped_early_reason": stopped_early_reason,
        "elapsed_ms": elapsed_ms,
    }


lambda_handler = handler