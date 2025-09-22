# utils/swing_levels.py
from typing import Literal, Tuple
import pandas as pd

def swing_low_window(lows: pd.Series, window: int = 5) -> Tuple[float, int]:
    """
    Devuelve (valor, pos_relativa) del mínimo en las últimas `window` velas.
    """
    lows = lows.dropna()
    if len(lows) == 0:
        raise ValueError("Serie de lows vacía.")
    idx = lows[-window:].idxmin()
    pos = int(lows.index.get_loc(idx))  # posición absoluta dentro del índice
    return float(lows.loc[idx]), pos

def swing_high_window(highs: pd.Series, window: int = 5) -> Tuple[float, int]:
    highs = highs.dropna()
    if len(highs) == 0:
        raise ValueError("Serie de highs vacía.")
    idx = highs[-window:].idxmax()
    pos = int(highs.index.get_loc(idx))
    return float(highs.loc[idx]), pos

def fractal_points(series: pd.Series, side: Literal["low","high"] = "low", left: int = 2, right: int = 2) -> pd.Index:
    """
    Marca fractales: centro es mínimo/máximo respecto a left/right velas.
    Devuelve un Index con los timestamps que son fractales.
    """
    s = series.dropna()
    vals = s.values
    idxs = []
    for i in range(left, len(vals) - right):
        window = vals[i-left:i+right+1]
        center = vals[i]
        if side == "low" and center == window.min():
            idxs.append(s.index[i])
        if side == "high" and center == window.max():
            idxs.append(s.index[i])
    return pd.Index(idxs)

def recent_swing(
    highs: pd.Series, lows: pd.Series,
    side: Literal["low","high"], method: Literal["window","fractal"]="window",
    window: int = 5, left: int = 2, right: int = 2
) -> float:
    """
    Devuelve el valor del swing (low/high) más reciente.
    - method="window": usa el min/max de las últimas `window` velas.
    - method="fractal": usa el último fractal válido (fallback a window si no hay).
    """
    if method == "window":
        if side == "low":
            v, _ = swing_low_window(lows, window=window)
            return v
        else:
            v, _ = swing_high_window(highs, window=window)
            return v

    # fractal
    idxs = fractal_points(lows if side == "low" else highs, side=side, left=left, right=right)
    if len(idxs) == 0:
        # fallback
        return recent_swing(highs, lows, side, method="window", window=window)
    last_idx = idxs[-1]
    return float((lows if side == "low" else highs).loc[last_idx])
