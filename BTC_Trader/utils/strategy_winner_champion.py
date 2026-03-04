# utils/strategy_winner_champion.py
# ==========================================================
# Winner/Champion Strategy (BTC trigger) — reusable module
# ----------------------------------------------------------
# Copiado/espejo de alert_bot.py para consumir en Streamlit u otros.
# NO hace I/O. Solo feature engineering + señales.
# ==========================================================

from __future__ import annotations

from typing import Dict, Any
import numpy as np
import pandas as pd


def rolling_z(x: pd.Series, win: int) -> pd.Series:
    mu = x.rolling(win, min_periods=win).mean()
    sd = x.rolling(win, min_periods=win).std().replace(0, np.nan)
    return ((x - mu) / sd).fillna(0.0)


def build_features_winner(
    df: pd.DataFrame,
    P: Dict[str, Any],
    ENERGY_ZWIN: int = 120,
    STRUCT_ZWIN: int = 120,
    STRUCT_WIN: int = 48,
    DON_WIN: int = 48,
) -> pd.DataFrame:
    """
    Espera df con columnas: Open, High, Low, Close, Volume.
    Index: datetime (ideal), pero puede ser cualquier index ordenable.
    Devuelve df con features: zspeed, zaccel, zenergy, struct_score, buy_raw, sell_raw, etc.
    """
    d = df.copy()
    eps = 1e-12

    mom_win = int(P["mom_win"])
    speed_win = int(P["speed_win"])
    accel_win = int(P["accel_win"])
    z_win = int(P["z_win"])
    zspeed_min = float(P["zspeed_min"])
    zaccel_min = float(P["zaccel_min"])

    d["mom"] = d["Close"].diff()
    d["mom_smooth"] = d["mom"].rolling(mom_win, min_periods=1).mean()

    d["speed"] = d["mom_smooth"].diff()
    d["speed_smooth"] = d["speed"].rolling(speed_win, min_periods=1).median()

    d["accel"] = d["speed_smooth"].diff()
    d["accel_smooth"] = d["accel"].rolling(accel_win, min_periods=1).median()

    std_speed = d["speed_smooth"].rolling(z_win).std().replace(0, np.nan)
    std_accel = d["accel_smooth"].rolling(z_win).std().replace(0, np.nan)

    d["zspeed"] = (d["speed_smooth"] / std_speed).fillna(0.0)
    d["zaccel"] = (d["accel_smooth"] / std_accel).fillna(0.0)

    d["buy_raw"] = (
        (d["zspeed"].shift(1) < 0) &
        (d["zspeed"] > zspeed_min) &
        (d["zaccel"] > zaccel_min)
    ).fillna(False).astype(bool)

    d["sell_raw"] = (
        (d["zspeed"].shift(1) > 0) &
        (d["zspeed"] < -zspeed_min) &
        (d["zaccel"] < -zaccel_min)
    ).fillna(False).astype(bool)

    # Energy
    d["energy"] = d["speed_smooth"] * d["accel_smooth"]
    d["zenergy"] = rolling_z(d["energy"], int(ENERGY_ZWIN))
    d["zenergy_diff"] = d["zenergy"].diff().fillna(0.0)

    # Structure
    d["range"] = (d["High"] - d["Low"]).clip(lower=0.0)
    d["body"] = (d["Close"] - d["Open"]).abs()
    d["upper_wick"] = (d["High"] - d[["Open", "Close"]].max(axis=1)).clip(lower=0.0)
    d["lower_wick"] = (d[["Open", "Close"]].min(axis=1) - d["Low"]).clip(lower=0.0)

    d["body_ratio"] = (d["body"] / (d["range"] + eps)).clip(0, 1)
    d["wick_ratio"] = ((d["upper_wick"] + d["lower_wick"]) / (d["range"] + eps)).clip(0, 2)

    d["range_pct"] = d["range"] / (d["Close"] + eps)
    d["z_range_pct"] = rolling_z(d["range_pct"], int(STRUCT_ZWIN))
    d["vol_z"] = rolling_z(d["Volume"].replace(0, np.nan).ffill().fillna(0.0), int(STRUCT_ZWIN))

    don_hi = d["High"].rolling(int(DON_WIN), min_periods=int(DON_WIN)).max()
    d["breakout_up"] = (d["Close"] > don_hi.shift(1)).fillna(False).astype(bool)

    score = (
        0.35 * d["body_ratio"].fillna(0.0) +
        0.25 * (1.0 - (d["wick_ratio"].fillna(0.0) / 2.0).clip(0, 1)) +
        0.20 * (1.0 / (1.0 + np.exp(-d["z_range_pct"].fillna(0.0)))) +
        0.15 * (1.0 / (1.0 + np.exp(-d["vol_z"].fillna(0.0)))) +
        0.05 * d["breakout_up"].astype(int)
    )
    d["struct_score"] = score.clip(0.0, 1.0)

    d["atr_pct"] = d["range_pct"].rolling(int(STRUCT_WIN), min_periods=1).mean().fillna(0.0)

    return d


def struct_modulated_threshold(d: pd.DataFrame, base_thr: float, k: float) -> pd.Series:
    # thr_eff = base - k*(struct_score - 0.5)
    return float(base_thr) - float(k) * (d["struct_score"] - 0.5)


def buy_signal_champion(
    d: pd.DataFrame,
    P: Dict[str, Any],
    ENTRY_ZENERGY_MIN: float = 1.8,
    ENTRY_K_STRUCT: float = 0.4,
    ENTRY_USE_ASYM: bool = False,
    ENTRY_N_DOWN: int = 1,
) -> pd.Series:
    zaccel_gate = float(P["zaccel_gate"])

    buy_base = d["buy_raw"] & (d["zaccel"] >= zaccel_gate)
    thr_eff = struct_modulated_threshold(d, ENTRY_ZENERGY_MIN, ENTRY_K_STRUCT)

    buy_ok = buy_base & (d["energy"] > 0) & (d["zenergy"] >= thr_eff)

    if ENTRY_USE_ASYM:
        neg = (d["zenergy_diff"] < 0).astype(int)
        streak = neg.rolling(int(ENTRY_N_DOWN), min_periods=int(ENTRY_N_DOWN)).sum()
        buy_ok = buy_ok & (streak < int(ENTRY_N_DOWN)).fillna(True)

    return buy_ok.fillna(False)


def simulate_sellraw_only(d: pd.DataFrame, buy_ok: pd.Series) -> pd.DataFrame:
    """
    Genera columnas BUY/SELL (bool) con state machine:
      - entra con buy_ok
      - sale con sell_raw
    """
    out = d.copy()
    n = len(out)
    BUY = np.zeros(n, dtype=bool)
    SELL = np.zeros(n, dtype=bool)

    in_pos = False
    for i in range(n):
        b = bool(buy_ok.iloc[i])
        s = bool(out["sell_raw"].iloc[i])

        if (not in_pos) and b:
            BUY[i] = True
            in_pos = True
        elif in_pos and s:
            SELL[i] = True
            in_pos = False

    out["BUY"] = BUY
    out["SELL"] = SELL
    return out
