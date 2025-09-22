# utils/risk_levels.py
from typing import Literal, List, Dict
import numpy as np
import pandas as pd
from .swing_levels import recent_swing

def atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    ATR simple (media móvil) del último valor.
    Requiere columnas: High, Low, Close.
    """
    h, l, c = df["High"], df["Low"], df["Close"]
    prev_c = c.shift(1)
    tr = pd.concat([
        (h - l),
        (h - prev_c).abs(),
        (l - prev_c).abs()
    ], axis=1).max(axis=1)
    atr_series = tr.rolling(period, min_periods=period).mean()
    return float(atr_series.iloc[-1]) if not np.isnan(atr_series.iloc[-1]) else np.nan

def stop_loss_from_swing(
    df: pd.DataFrame,
    side: Literal["BUY","SELL"],
    method: Literal["window","fractal"] = "window",
    window: int = 5, left: int = 2, right: int = 2,
    atr_k: float = 0.0
) -> float:
    """
    SL basado en swing más reciente +/- margen ATR opcional.
    BUY: usa swing low. SELL: usa swing high.
    """
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

def take_profits(entry: float, side: Literal["BUY","SELL"], percents: List[float]) -> List[float]:
    """
    TP relativos a la entrada. Percents en % (ej. [1.0, 1.5, 1.75]).
    """
    m = 1 if side == "BUY" else -1
    return [entry * (1 + m * p / 100.0) for p in percents]

def build_levels(
    df: pd.DataFrame,
    side: Literal["BUY","SELL"],
    entry: float,
    tp_percents: List[float] = [1.0, 1.5, 1.75],
    sl_method: Literal["window","fractal"] = "window",
    window: int = 5, left: int = 2, right: int = 2, atr_k: float = 0.0
) -> Dict:
    sl = stop_loss_from_swing(
        df, side=side, method=sl_method, window=window, left=left, right=right, atr_k=atr_k
    )
    tps = take_profits(entry, side, tp_percents)

    # R:R con distancia absoluta a SL
    dist_risk = abs(entry - sl) if entry != sl else np.nan
    rr = [(abs(tp - entry) / dist_risk) if dist_risk and not np.isnan(dist_risk) else np.nan for tp in tps]

    return {"entry": float(entry), "sl": float(sl), "tps": [float(x) for x in tps], "rr": rr, "tp_percents": tp_percents}

def format_signal_msg(
    symbol: str,
    side: Literal["BUY","SELL"],
    levels: Dict,
    ts_local_str: str,
    source_url: str
) -> str:
    """
    Formatea un mensaje “operable” con SL/TPs y R:R.
    ts_local_str: por ejemplo fila['Open time'] de tu DF (string ya bonito en CR).
    """
    arrow = "🟢" if side == "BUY" else "🔴"
    entry = levels["entry"]; sl = levels["sl"]; tps = levels["tps"]; rr = levels["rr"]
    lines = [
        f"{arrow} NUEVA SEÑAL para {symbol}:",
        f"📍 {side}",
        f"💵 Entrada: {entry:,.6f}",
        f"🛑 Stop Loss: {sl:,.6f}",
    ]
    for i, (tp, r, pct) in enumerate(zip(tps, rr, levels["tp_percents"]), start=1):
        move_pct = (tp/entry - 1.0) * (100 if side=="BUY" else -100)
        rr_txt = f" | R:R ~ {r:.2f}" if not np.isnan(r) else ""
        lines.append(f"🎯 TP{i}: {tp:,.6f} (+{pct:.2f}%) ({move_pct:+.2f}% vs entrada){rr_txt}")
    lines += [
        f"🕒 {ts_local_str} (CR)",
        f"🔗 base: {source_url}"
    ]
    return "\n".join(lines)
