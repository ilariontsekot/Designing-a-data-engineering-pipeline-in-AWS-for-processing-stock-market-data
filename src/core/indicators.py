# src/core/indicators.py
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional


def _safe_div(a: float, b: float) -> Optional[float]:
    if b == 0:
        return None
    return a / b


def _mean(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs) / len(xs)


def _stddev_sample(xs: List[float]) -> Optional[float]:
    # desviación típica muestral
    n = len(xs)
    if n < 2:
        return None
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    return math.sqrt(var)


def _rolling_sma(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 0:
        return out

    s = 0.0
    for i, v in enumerate(values):
        s += v
        if i >= window:
            s -= values[i - window]
        if i >= window - 1:
            out[i] = s / window
    return out


def _ema(values: List[float], period: int) -> List[Optional[float]]:
    """
    EMA clásica con alpha=2/(period+1).
    Seed: primera EMA = primer valor (simple y estable para pipelines).
    """
    out: List[Optional[float]] = [None] * len(values)
    if period <= 0 or not values:
        return out

    alpha = 2.0 / (period + 1.0)
    ema_prev = values[0]
    out[0] = ema_prev

    for i in range(1, len(values)):
        ema_prev = alpha * values[i] + (1.0 - alpha) * ema_prev
        out[i] = ema_prev
    return out


def _returns(close: List[float], n: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(close)
    if n <= 0:
        return out
    for i in range(len(close)):
        if i - n < 0:
            out[i] = None
            continue
        prev = close[i - n]
        out[i] = None if prev == 0 else (close[i] / prev - 1.0)
    return out


def _log_return_1d(close: List[float]) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(close)
    for i in range(len(close)):
        if i == 0:
            out[i] = None
            continue
        prev = close[i - 1]
        if prev <= 0 or close[i] <= 0:
            out[i] = None
        else:
            out[i] = math.log(close[i] / prev)
    return out


def _true_range(high: List[float], low: List[float], close: List[float]) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(close)
    for i in range(len(close)):
        if i == 0:
            out[i] = high[i] - low[i]
            continue
        tr = max(
            high[i] - low[i],
            abs(high[i] - close[i - 1]),
            abs(low[i] - close[i - 1]),
        )
        out[i] = tr
    return out


def _atr_sma(tr: List[Optional[float]], period: int) -> List[Optional[float]]:
    # ATR como SMA del True Range
    vals = [0.0 if v is None else float(v) for v in tr]
    return _rolling_sma(vals, period)


def _rsi_wilder(close: List[float], period: int = 14) -> List[Optional[float]]:
    """
    RSI (Wilder):
      - gains/losses
      - avg_gain/avg_loss con smoothing Wilder
    """
    out: List[Optional[float]] = [None] * len(close)
    if len(close) < 2 or period <= 0:
        return out

    gains: List[float] = [0.0] * len(close)
    losses: List[float] = [0.0] * len(close)

    for i in range(1, len(close)):
        diff = close[i] - close[i - 1]
        gains[i] = max(diff, 0.0)
        losses[i] = max(-diff, 0.0)

    # seed con media simple de los primeros "period"
    if len(close) <= period:
        return out

    avg_gain = sum(gains[1 : period + 1]) / period
    avg_loss = sum(losses[1 : period + 1]) / period

    # primer RSI disponible en i=period
    rs = None if avg_loss == 0 else (avg_gain / avg_loss)
    out[period] = 100.0 if avg_loss == 0 else (100.0 - (100.0 / (1.0 + rs)))

    for i in range(period + 1, len(close)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

        if avg_loss == 0:
            out[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            out[i] = 100.0 - (100.0 / (1.0 + rs))

    return out


def _rolling_zscore(values: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if window <= 1:
        return out

    for i in range(len(values)):
        if i < window - 1:
            continue
        w = values[i - window + 1 : i + 1]
        m = _mean(w)
        sd = _stddev_sample(w)
        if m is None or sd is None or sd == 0:
            out[i] = None
        else:
            out[i] = (values[i] - m) / sd
    return out


def _obv(close: List[float], volume: List[int]) -> List[Optional[int]]:
    out: List[Optional[int]] = [None] * len(close)
    if not close:
        return out
    obv = 0
    out[0] = obv
    for i in range(1, len(close)):
        if close[i] > close[i - 1]:
            obv += int(volume[i])
        elif close[i] < close[i - 1]:
            obv -= int(volume[i])
        out[i] = obv
    return out


def compute_gold_metrics(rows: List[Dict]) -> List[Dict]:
    """
    Entrada: filas Silver (ya tipadas) con al menos:
      date, open, high, low, close, volume

    Salida: filas Gold con esas columnas + métricas.
    OJO: asume rows ordenadas por fecha ASC.
    """
    if not rows:
        return []

    close = [float(r["close"]) for r in rows]
    high = [float(r["high"]) for r in rows]
    low = [float(r["low"]) for r in rows]
    open_ = [float(r["open"]) for r in rows]
    volume = [int(r["volume"]) for r in rows]

    # returns
    r1 = _returns(close, 1)
    r5 = _returns(close, 5)
    r20 = _returns(close, 20)
    lr1 = _log_return_1d(close)

    # volatility (sobre log_return_1d)
    # (vol = stddev de log-returns en ventana)
    vol10: List[Optional[float]] = [None] * len(rows)
    vol20: List[Optional[float]] = [None] * len(rows)
    for i in range(len(rows)):
        if i >= 10:
            w = [x for x in lr1[i - 9 : i + 1] if x is not None]
            vol10[i] = _stddev_sample(w)
        if i >= 20:
            w = [x for x in lr1[i - 19 : i + 1] if x is not None]
            vol20[i] = _stddev_sample(w)

    # range / ATR
    tr = _true_range(high, low, close)
    atr14 = _atr_sma(tr, 14)
    range_pct: List[Optional[float]] = [None] * len(rows)
    for i in range(len(rows)):
        denom = close[i]
        range_pct[i] = None if denom == 0 else (high[i] - low[i]) / denom

    # moving averages
    sma20 = _rolling_sma(close, 20)
    sma50 = _rolling_sma(close, 50)
    ema12 = _ema(close, 12)
    ema26 = _ema(close, 26)

    # MACD
    macd: List[Optional[float]] = [None] * len(rows)
    for i in range(len(rows)):
        if ema12[i] is None or ema26[i] is None:
            macd[i] = None
        else:
            macd[i] = float(ema12[i]) - float(ema26[i])
    macd_signal = _ema([0.0 if x is None else float(x) for x in macd], 9)
    macd_hist: List[Optional[float]] = [None] * len(rows)
    for i in range(len(rows)):
        if macd[i] is None or macd_signal[i] is None:
            macd_hist[i] = None
        else:
            macd_hist[i] = float(macd[i]) - float(macd_signal[i])

    # RSI
    rsi14 = _rsi_wilder(close, 14)

    # volume features
    vol_sma20 = _rolling_sma([float(v) for v in volume], 20)
    vol_z20 = _rolling_zscore([float(v) for v in volume], 20)
    obv = _obv(close, volume)

    # distance to MAs
    c_to_sma20: List[Optional[float]] = [None] * len(rows)
    c_to_sma50: List[Optional[float]] = [None] * len(rows)
    for i in range(len(rows)):
        c_to_sma20[i] = None if sma20[i] in (None, 0) else (close[i] / float(sma20[i]) - 1.0)
        c_to_sma50[i] = None if sma50[i] in (None, 0) else (close[i] / float(sma50[i]) - 1.0)

    # optional states
    rsi_state: List[Optional[str]] = [None] * len(rows)
    for i in range(len(rows)):
        v = rsi14[i]
        if v is None:
            rsi_state[i] = None
        elif v >= 70:
            rsi_state[i] = "overbought"
        elif v <= 30:
            rsi_state[i] = "oversold"
        else:
            rsi_state[i] = "neutral"

    ma_cross_state: List[Optional[str]] = [None] * len(rows)
    for i in range(len(rows)):
        if sma20[i] is None or sma50[i] is None:
            ma_cross_state[i] = None
        else:
            ma_cross_state[i] = "bullish" if float(sma20[i]) >= float(sma50[i]) else "bearish"

    # build output rows
    out: List[Dict] = []
    for i, r in enumerate(rows):
        out.append(
            {
                "symbol": r.get("symbol"),
                "date": r.get("date"),

                # base fields
                "open": open_[i],
                "high": high[i],
                "low": low[i],
                "close": close[i],
                "volume": volume[i],

                # returns
                "log_return_1d": lr1[i],
                "return_1d": r1[i],
                "return_5d": r5[i],
                "return_20d": r20[i],

                # risk / volatility
                "volatility_10": vol10[i],
                "volatility_20": vol20[i],

                # range / atr
                "atr_14": atr14[i],
                "range_pct": range_pct[i],

                # MAs
                "sma_20": sma20[i],
                "sma_50": sma50[i],
                "ema_12": ema12[i],
                "ema_26": ema26[i],

                # MACD
                "macd": macd[i],
                "macd_signal": macd_signal[i],
                "macd_hist": macd_hist[i],

                # RSI
                "rsi_14": rsi14[i],

                # volume features
                "volume_sma_20": vol_sma20[i],
                "volume_zscore_20": vol_z20[i],
                "obv": obv[i],

                # distances
                "close_to_sma20_pct": c_to_sma20[i],
                "close_to_sma50_pct": c_to_sma50[i],

                # optional states
                "rsi_state": rsi_state[i],
                "ma_cross_state": ma_cross_state[i],

                # trazabilidad (por fila, heredada de Silver)
                "fetched_at_utc": r.get("fetched_at_utc"),
                "api_last_refreshed": r.get("api_last_refreshed"),
                "api_timezone": r.get("api_timezone"),
                "run_id": r.get("run_id"),
                "lambda_request_id": r.get("lambda_request_id"),
                "bronze_s3_key": r.get("bronze_s3_key"),
                "silver_processed_at_utc": r.get("processed_at_utc"),
            }
        )
    return out