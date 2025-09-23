# utils/risk_levels.py
from typing import Literal, List, Dict
import numpy as np
import pandas as pd
from .swing_levels import recent_swing

def atr(df: pd.DataFrame, period: int = 14) -> float:
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - prev_c).abs(),
        (l - prev_c).abs()
    ], axis=1).max(axis=1)
    atr_series = tr.rolling(period, min_periods=period).mean()
    val = float(atr_series.iloc[-1]) if len(atr_series) and not np.isnan(atr_series.iloc[-1]) else np.nan
    return val

def stop_loss_from_swing(
    df: pd.DataFrame,
    side: Literal["BUY","SELL"],
    method: Literal["window","fractal"] = "window",
    window: int = 5, left: int = 2, right: int = 2,
    atr_k: float = 0.0
) -> float:
    if side == "BUY":
        base = recent_swing(df["High"], df["Low"], side="low", method=method, window=window, left=left, right=right)
        out = base
        if atr_k > 0:
            last_atr = atr(df)
            if not np.isnan(last_atr):
                out = base - atr_k * last_atr
    else:
        base = recent_swing(df["High"], df["Low"], side="high", method=method, window=window, left=left, right=right)
        out = base
        if atr_k > 0:
            last_atr = atr(df)
            if not np.isnan(last_atr):
                out = base + atr_k * last_atr
    return float(out)

def take_profits_rr(entry: float, sl: float, side: Literal["BUY","SELL"], rr_targets: List[float]) -> List[float]:
    """
    TPs por múltiplos de riesgo (R:R).
    Riesgo = |entry - SL|.
    BUY: TP = entry + R*risk
    SELL: TP = entry - R*risk
    """
    risk = abs(entry - sl)
    if risk == 0 or np.isnan(risk):
        return [np.nan for _ in rr_targets]

    if side == "BUY":
        return [entry + r * risk for r in rr_targets]
    else:
        return [entry - r * risk for r in rr_targets]

def build_levels(
    df: pd.DataFrame,
    side: Literal["BUY","SELL"],
    entry: float,
    rr_targets: List[float] = [1.0, 1.5, 1.75],
    sl_method: Literal["window","fractal"] = "window",
    window: int = 5, left: int = 2, right: int = 2, atr_k: float = 0.0
) -> Dict:
    sl = stop_loss_from_swing(
        df, side=side, method=sl_method, window=window, left=left, right=right, atr_k=atr_k
    )
    tps = take_profits_rr(entry, sl, side, rr_targets)

    return {
        "entry": float(entry),
        "sl": float(sl),
        "tps": [float(x) if not np.isnan(x) else np.nan for x in tps],
        "rr": rr_targets[:],          # por claridad
        "rr_targets": rr_targets[:]   # alias
    }

def format_signal_msg(
    symbol: str,
    side: Literal["BUY","SELL"],
    levels: Dict,
    ts_local_str: str,
    source_url: str
) -> str:
    """
    Mensaje limpio centrado en R:R (sin % vs entrada).
    Pensado para BUY; para SELL tu bot usa formato simple.
    """
    arrow = "🟢" if side == "BUY" else "🔴"
    entry = levels["entry"]
    sl    = levels["sl"]
    tps   = levels["tps"]
    rr_targets = levels.get("rr_targets", levels.get("rr", []))

    lines = [
        f"{arrow} NUEVA SEÑAL para {symbol}:",
        f"📍 {side}",
        f"💵 Entrada: {entry:,.6f}",
        f"🛑 Stop Loss: {sl:,.6f}",
    ]

    for i, (tp, rmult) in enumerate(zip(tps, rr_targets), start=1):
        if np.isnan(tp):
            lines.append(f"🎯 TP{i}: N/A (R:R {rmult:.2f}x)")
        else:
            lines.append(f"🎯 TP{i}: {tp:,.6f} (R:R {rmult:.2f}x)")

    lines += [
        f"🕒 {ts_local_str} (CR)",
        f"🔗 base: {source_url}"
    ]
    return "\n".join(lines)
