# src/lambdas/transform_gold/handler.py
from __future__ import annotations
from urllib.parse import unquote_plus

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import boto3

from src.core.config import get_settings
from src.core.indicators import compute_gold_metrics
from src.core.logging import get_logger
from src.core.s3_io import upload_json_to_s3

log = get_logger(__name__)

s3 = boto3.client("s3")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return json.loads(body)


def _head_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False


def _extract_symbol_from_key(silver_key: str) -> Optional[str]:
    # silver/clean/source=alphavantage/symbol=AAPL/2026/04/13/003007_926.json
    token = "symbol="
    if token not in silver_key:
        return None
    after = silver_key.split(token, 1)[1]
    return after.split("/", 1)[0] if after else None


def _extract_date_from_key(silver_key: str) -> Optional[Tuple[str, str, str]]:
    # .../YYYY/MM/DD/...
    parts = silver_key.split("/")
    for i in range(len(parts) - 3):
        y, m, d = parts[i], parts[i + 1], parts[i + 2]
        if len(y) == 4 and len(m) == 2 and len(d) == 2 and y.isdigit() and m.isdigit() and d.isdigit():
            return y, m, d
    return None


def _gold_key(prefix: str, source: str, symbol: str, y: str, m: str, d: str, silver_key: str) -> str:
    # idempotencia: key determinista por "input silver key"
    h = hashlib.md5(silver_key.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}/metrics/source={source}/symbol={symbol}/{y}/{m}/{d}/gold_{h}.json"


def _normalize_s3_event(event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Soporta S3 Put event típico.
    Devuelve (bucket, key) o (None, None) si no lo encuentra.
    """
    recs = event.get("Records") or []
    if not recs:
        return None, None
    r0 = recs[0]
    s3info = r0.get("s3") or {}
    b = (s3info.get("bucket") or {}).get("name")
    k = (s3info.get("object") or {}).get("key")
    return b, k


def handler(event: Optional[Dict[str, Any]] = None, context: Any = None) -> Dict[str, Any]:
    t0 = time.time()
    event = event or {}

    s = get_settings()

    # Buckets/prefix:
    # - Puedes usar el mismo bucket que Silver si quieres (recomendado para tu TFG)
    # - Si no tienes env vars nuevas, caemos a S3_BRONZE_BUCKET por compatibilidad
    gold_bucket = getattr(s, "S3_GOLD_BUCKET", None) or s.S3_BRONZE_BUCKET
    gold_prefix = getattr(s, "GOLD_PREFIX", None) or "gold"

    silver_bucket, silver_key = _normalize_s3_event(event)

    # Permite invocar manualmente desde consola con {"bucket":"...","key":"..."}
    if not silver_bucket or not silver_key:
        silver_bucket = event.get("bucket") or s.S3_BRONZE_BUCKET
        silver_key = event.get("key")

    if not silver_key:
        return {"ok": False, "error": "Missing S3 key in event", "elapsed_ms": int((time.time() - t0) * 1000)}

    silver_key = unquote_plus(silver_key)

    symbol = _extract_symbol_from_key(silver_key) or "UNKNOWN"
    date_parts = _extract_date_from_key(silver_key)
    if not date_parts:
        return {"ok": False, "error": f"Could not parse YYYY/MM/DD from key: {silver_key}"}
    y, m, d = date_parts

    source = "alphavantage"
    out_key = _gold_key(gold_prefix, source, symbol, y, m, d, silver_key)

    # ---- IDEMPOTENCIA ----
    if _head_exists(gold_bucket, out_key):
        log.info("SKIP | already exists | gold_key=%s", out_key)
        return {"ok": True, "status": "SKIPPED_ALREADY_DONE", "gold_key": out_key}

    log.info(
        "Start GOLD | silver_bucket=%s | silver_key=%s | gold_bucket=%s | gold_key=%s",
        silver_bucket, silver_key, gold_bucket, out_key
    )

    try:
        silver_doc = _read_json_from_s3(silver_bucket, silver_key)
        meta = silver_doc.get("metadata") or {}
        records = silver_doc.get("records") or []

        if not records:
            gold_doc = {
                "metadata": {
                    "layer": "gold",
                    "source": meta.get("source", source),
                    "symbol": meta.get("symbol", symbol),
                    "status": "EMPTY",
                    "input_silver_s3_key": silver_key,
                    "processed_at_utc": _utc_now_iso(),
                    "row_count": 0,
                    "failed_reason": "No records in Silver",
                },
                "records": [],
            }
            upload_json_to_s3(gold_doc, gold_bucket, out_key)
            return {"ok": True, "status": "EMPTY", "gold_key": out_key}

        # ordenar por fecha ASC para rolling windows
        records_sorted = sorted(records, key=lambda r: r.get("date") or "")

        gold_rows = compute_gold_metrics(records_sorted)

        # validaciones mínimas (sin romper pipeline)
        valid_rows = []
        for r in gold_rows:
            c = r.get("close")
            v = r.get("volume")
            if c is None or c <= 0:
                continue
            if v is None or v < 0:
                continue
            valid_rows.append(r)

        gold_doc = {
            "metadata": {
                "layer": "gold",
                "source": meta.get("source", source),
                "symbol": meta.get("symbol", symbol),
                "status": "SUCCESS",
                "input_silver_s3_key": silver_key,
                "processed_at_utc": _utc_now_iso(),
                "row_count": len(valid_rows),
                "failed_reason": None,
                "silver_processed_at_utc": meta.get("processed_at_utc"),
                "bronze_s3_key": meta.get("bronze_s3_key"),
            },
            "records": valid_rows,
        }

        upload_json_to_s3(gold_doc, gold_bucket, out_key)

        elapsed_ms = int((time.time() - t0) * 1000)
        log.info("Done GOLD | ok=true | rows=%s | elapsed_ms=%s | gold_key=%s", len(valid_rows), elapsed_ms, out_key)

        return {
            "ok": True,
            "status": "SUCCESS",
            "symbol": symbol,
            "row_count": len(valid_rows),
            "gold_bucket": gold_bucket,
            "gold_key": out_key,
            "elapsed_ms": elapsed_ms,
        }

    except Exception as e:
        elapsed_ms = int((time.time() - t0) * 1000)
        log.exception("GOLD FAILED | silver_key=%s | error=%s", silver_key, e)
        return {
            "ok": False,
            "status": "ERROR",
            "silver_key": silver_key,
            "error": f"{type(e).__name__}: {e}",
            "elapsed_ms": elapsed_ms,
        }


lambda_handler = handler