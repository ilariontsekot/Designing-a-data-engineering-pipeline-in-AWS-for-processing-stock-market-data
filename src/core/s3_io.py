from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3

_s3 = boto3.client("s3")  # Reutiliza cliente entre invocaciones


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _strip_slashes(s: str) -> str:
    return s.strip("/")


def build_bronze_key(prefix: str = "bronze/raw") -> str:
    """
    Key S3 particionada por fecha:
      bronze/raw/2026/02/01/134107_123.json
    """
    prefix = _strip_slashes(prefix)
    now = _utc_now()
    ts = now.strftime("%H%M%S")
    ms = f"{int(now.microsecond / 1000):03d}"
    return f"{prefix}/{now:%Y/%m/%d}/{ts}_{ms}.json"


def build_layer_key(prefix: str) -> str:
    """
    Genera una key particionada por fecha/hora para cualquier capa.
    Ejemplo:
      silver/clean/source=alphavantage/symbol=AAPL/2026/03/02/003005_666.json
    """
    prefix = _strip_slashes(prefix)
    now = _utc_now()
    ts = now.strftime("%H%M%S")
    ms = f"{int(now.microsecond / 1000):03d}"
    return f"{prefix}/{now:%Y/%m/%d}/{ts}_{ms}.json"


def upload_json_to_s3(
    data: Dict[str, Any],
    bucket: str,
    key: str,
    content_type: str = "application/json",
) -> None:
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def download_json_from_s3(bucket: str, key: str) -> Dict[str, Any]:
    obj = _s3.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    return json.loads(raw.decode("utf-8"))


def list_s3_keys(bucket: str, prefix: str, max_keys: int = 1000) -> List[str]:
    resp = _s3.list_objects_v2(Bucket=bucket, Prefix=_strip_slashes(prefix), MaxKeys=max_keys)
    contents = resp.get("Contents", [])
    return [item["Key"] for item in contents if item["Key"] != _strip_slashes(prefix)]


def get_latest_s3_key(bucket: str, prefix: str) -> Optional[str]:
    resp = _s3.list_objects_v2(Bucket=bucket, Prefix=_strip_slashes(prefix))
    contents = resp.get("Contents", [])
    if not contents:
        return None

    latest = max(contents, key=lambda x: x["LastModified"])
    return latest["Key"]