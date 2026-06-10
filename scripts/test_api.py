from __future__ import annotations

from pathlib import Path
import sys

from src.core.config import get_settings
from src.core.api_client import fetch_daily, build_bronze_like_payload, save_json_to_file


def main() -> int:
    settings = get_settings()

    # Puedes pasar símbolo por CLI: python scripts/test_api.py MSFT
    symbol = sys.argv[1].strip().upper() if len(sys.argv) > 1 else settings.default_symbol

    out_dir: Path = settings.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[local-test] Fetching Alpha Vantage daily for symbol={symbol} ...")
    result = fetch_daily(settings, symbol)

    wrapper = build_bronze_like_payload(result)
    out_file = out_dir / "raw_test.json"
    save_json_to_file(wrapper, str(out_file))

    print(f"[local-test] status={result.status} ok={result.ok}")
    if result.error_message:
        print(f"[local-test] error_message={result.error_message}")

    print(f"[local-test] Saved: {out_file.resolve()}")
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
