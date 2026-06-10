from __future__ import annotations

import pandas as pd
from pathlib import Path

IN_CSV = Path("data/analytics/gold_2025_11_to_2026_04_history.csv")
OUT_CSV = Path("data/analytics/gold_2025_11_to_2026_04_history_base100.csv")

def main() -> None:
    # Tu CSV está en formato europeo por tu script: sep=";" decimal=","
    df = pd.read_csv(IN_CSV, sep=";", decimal=",", parse_dates=["date"])

    # Asegurar orden correcto
    df = df.dropna(subset=["symbol", "date", "close"]).copy()
    df = df.sort_values(["symbol", "date"])

    # Base 100 por símbolo: close / primer close del símbolo * 100
    first_close = df.groupby("symbol")["close"].transform("first")
    df["price_base100"] = (df["close"] / first_close) * 100.0

    # Guardar manteniendo formato compatible Power BI ES
    df.to_csv(OUT_CSV, index=False, sep=";", decimal=",", encoding="utf-8")

    print("OK ✅")
    print("in:", IN_CSV)
    print("out:", OUT_CSV)
    print("rows:", len(df))
    print("symbols:", df["symbol"].nunique())
    print("date_min:", df["date"].min())
    print("date_max:", df["date"].max())

if __name__ == "__main__":
    main()
