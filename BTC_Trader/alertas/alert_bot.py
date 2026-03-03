# ==========================================================
# alertas/alert_bot.py
# 5m BTC Trigger → BNB Execution (Spot/Margin via Router)
# - Estrategia ACTIVA: WINNER/CHAMPION (Energy + Structure)
# - Estrategia LEGACY (params): Momentum Físico Speed (dejada como fallback/manual)
# - Anti-caídas: estado.json (transición real + last_close_ms)
# - Railway: TRIGGER_SYMBOL=BTCUSDT | TRADE_SYMBOL/ALLOWED_SYMBOLS=BNBUSDT
# ==========================================================

import os
import sys
import requests
import pandas as pd
import numpy as np

# Asegurar imports desde raíz del repo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.binance_fetch import (
    get_binance_5m_data,
    fetch_last_closed_kline_5m,
    bases_para,
)

# Router de ejecución (Spot/Margin)
from utils.trade_executor_router import route_signal

from signal_tracker import cargar_estado_anterior, guardar_estado_actual


# ==========================================================
# ENV / CONFIG
# ==========================================================

TRIGGER_SYMBOL = os.getenv("TRIGGER_SYMBOL", "BTCUSDT").strip().upper()
TRADE_SYMBOL   = os.getenv("TRADE_SYMBOL", "BNBUSDT").strip().upper()

DRY_RUN        = os.getenv("DRY_RUN", "false").lower() == "true"
STATE_PATH     = os.getenv("STATE_PATH", "./estado.json")

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"
GRACE_MINUTES    = int(os.getenv("GRACE_MINUTES", "7"))
HISTORY_LIMIT_5M = int(os.getenv("HISTORY_LIMIT_5M", "900"))

# Para logs claros (Railway)
ALLOWED_SYMBOLS_ENV = (os.getenv("ALLOWED_SYMBOLS") or "").strip().upper()
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"
USE_MARGIN_ENV      = os.getenv("USE_MARGIN", "false").lower() == "true"

print("==================================================", flush=True)
print("🚀 alert_bot.py — BTC Trigger → BNB Exec (5m)", flush=True)
print(f"🔧 DRY_RUN={DRY_RUN} | USE_MARGIN={USE_MARGIN}", flush=True)
print(f"🎯 TRIGGER_SYMBOL={TRIGGER_SYMBOL}", flush=True)
print(f"💱 TRADE_SYMBOL={TRADE_SYMBOL}", flush=True)
print(f"🔒 STRICT_TRADE_SYMBOL={STRICT_TRADE_SYMBOL} | ALLOWED_SYMBOLS={ALLOWED_SYMBOLS_ENV or '(not set)'}", flush=True)
print("==================================================", flush=True)


# ==========================================================
# LEGACY PARAMS (NO se usan en la estrategia activa)
# Mantener por si quieres volver al momentum viejo.
# ==========================================================

SYMBOL_PARAMS = {
    "BTCUSDT": {"mom_win": 4, "speed_win": 9, "accel_win": 7, "zspeed_min": 0.3, "zaccel_min": 0.1},
    "ETHUSDT": {"mom_win": 7, "speed_win": 9, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.2},
    "ADAUSDT": {"mom_win": 4, "speed_win": 7, "accel_win": 5, "zspeed_min": 0.2, "zaccel_min": 0.3},
    "XRPUSDT": {"mom_win": 5, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.2, "zaccel_min": 0.0},
    "BNBUSDT": {"mom_win": 6, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.0},
}

# ==========================================================
# ACTIVE STRATEGY — WINNER/CHAMPION CONFIG (BTC→BNB)
# ==========================================================

P = dict(
    mom_win=4,
    speed_win=9,
    accel_win=7,
    z_win=20,          # ✅ clave
    zspeed_min=0.30,
    zaccel_min=0.10,
    zaccel_gate=4.0    # ✅ champion
)

ENERGY_ZWIN = 120
STRUCT_ZWIN = 120
STRUCT_WIN  = 48
DON_WIN     = 48

ENTRY_ZENERGY_MIN = 1.8      # ✅ champion
ENTRY_K_STRUCT    = 0.4      # ✅ champion
ENTRY_USE_ASYM    = False
ENTRY_N_DOWN      = 1


# ==========================================================
# Telegram helper
# ==========================================================

def enviar_mensaje_telegram(mensaje: str):
    if DRY_RUN:
        print("💤 DRY_RUN → Telegram deshabilitado", flush=True)
        return

    if not TOKEN or not CHAT_ID:
        print("❌ ERROR: TELEGRAM_TOKEN o TELEGRAM_CHAT_ID no definidos", flush=True)
        return

    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje}, timeout=20)
        if r.status_code == 200:
            print("✅ Telegram enviado", flush=True)
        else:
            print(f"⚠️ Telegram error: {r.text}", flush=True)
    except Exception as e:
        print(f"⚠️ Telegram excepción: {e}", flush=True)


# ==========================================================
# WINNER/CHAMPION functions (Energy + Structure)
# ==========================================================

def rolling_z(x: pd.Series, win: int) -> pd.Series:
    mu = x.rolling(win, min_periods=win).mean()
    sd = x.rolling(win, min_periods=win).std().replace(0, np.nan)
    return ((x - mu) / sd).fillna(0.0)

def build_features_winner(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    eps = 1e-12

    d["mom"] = d["Close"].diff()
    d["mom_smooth"] = d["mom"].rolling(int(P["mom_win"]), min_periods=1).mean()

    d["speed"] = d["mom_smooth"].diff()
    d["speed_smooth"] = d["speed"].rolling(int(P["speed_win"]), min_periods=1).median()

    d["accel"] = d["speed_smooth"].diff()
    d["accel_smooth"] = d["accel"].rolling(int(P["accel_win"]), min_periods=1).median()

    std_speed = d["speed_smooth"].rolling(int(P["z_win"])).std().replace(0, np.nan)
    std_accel = d["accel_smooth"].rolling(int(P["z_win"])).std().replace(0, np.nan)

    d["zspeed"] = (d["speed_smooth"] / std_speed).fillna(0.0)
    d["zaccel"] = (d["accel_smooth"] / std_accel).fillna(0.0)

    d["buy_raw"] = (
        (d["zspeed"].shift(1) < 0) &
        (d["zspeed"] > float(P["zspeed_min"])) &
        (d["zaccel"] > float(P["zaccel_min"]))
    ).fillna(False).astype(bool)

    d["sell_raw"] = (
        (d["zspeed"].shift(1) > 0) &
        (d["zspeed"] < -float(P["zspeed_min"])) &
        (d["zaccel"] < -float(P["zaccel_min"]))
    ).fillna(False).astype(bool)

    d["energy"] = d["speed_smooth"] * d["accel_smooth"]
    d["zenergy"] = rolling_z(d["energy"], int(ENERGY_ZWIN))
    d["zenergy_diff"] = d["zenergy"].diff().fillna(0.0)

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
    return float(base_thr) - float(k) * (d["struct_score"] - 0.5)

def buy_signal_champion(d: pd.DataFrame) -> pd.Series:
    buy_base = d["buy_raw"] & (d["zaccel"] >= float(P["zaccel_gate"]))
    thr_eff  = struct_modulated_threshold(d, ENTRY_ZENERGY_MIN, ENTRY_K_STRUCT)

    buy_ok = buy_base & (d["energy"] > 0) & (d["zenergy"] >= thr_eff)

    if ENTRY_USE_ASYM:
        neg = (d["zenergy_diff"] < 0).astype(int)
        streak = neg.rolling(int(ENTRY_N_DOWN), min_periods=int(ENTRY_N_DOWN)).sum()
        buy_ok = buy_ok & (streak < int(ENTRY_N_DOWN)).fillna(True)

    return buy_ok.fillna(False)

def simulate_sellraw_only(d: pd.DataFrame, buy_ok: pd.Series) -> pd.DataFrame:
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


# ==========================================================
# Last closed 5m candle (server-confirmed)
# ==========================================================

def _last_closed_for(symbol: str):
    for base in bases_para(symbol):
        try:
            k, last_open, last_close, server_ms = fetch_last_closed_kline_5m(symbol, base)
            print(f"[{symbol}] Última 5m cerrada confirmada con {base}", flush=True)
            return last_open, last_close, base, server_ms
        except Exception as e:
            print(f"[{symbol}] fallo confirmando en {base}: {e}", flush=True)

    raise RuntimeError(f"[{symbol}] No se pudo confirmar la última vela cerrada 5m.")


# ==========================================================
# MAIN
# ==========================================================

def main():
    # Cargar estado anterior (estado.json)
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    symbol = TRIGGER_SYMBOL  # BTC

    try:
        print(f"\n===================== TRIGGER {symbol} =====================", flush=True)

        # 1) Última vela 5m cerrada
        last_open_ms, last_close_ms, base, server_ms = _last_closed_for(symbol)
        last_open_utc         = pd.to_datetime(last_open_ms, unit="ms", utc=True)
        last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1, unit="ms", utc=True)

        prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
        prev_signal = prev.get("signal")
        prev_close  = int(prev.get("last_close_ms") or 0)

        # 2) Grace period
        if GRACE_MINUTES > 0 and (server_ms - last_close_ms) > GRACE_MINUTES * 60_000:
            print(f"⏭️ [{symbol}] Señal atrasada → ignorada (grace).", flush=True)
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
            print(f"💾 Estado: {estado_actual}", flush=True)
            guardar_estado_actual(estado_actual)
            print("✅ Finalizado", flush=True)
            return

        # 3) Descargar histórico 5m del TRIGGER (BTC)
        df = get_binance_5m_data(symbol, limit=HISTORY_LIMIT_5M, preferred_base=base)

        # Adaptar a OHLCV con índice = Close time UTC (para matchear last_close_utc_minus1)
        btc_ohlcv = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        btc_ohlcv.index = pd.to_datetime(df["Close time UTC"], utc=True)
        btc_ohlcv = btc_ohlcv.sort_index()

        # 4) Señales winner/champion
        d = build_features_winner(btc_ohlcv)

        # Logs útiles (Railway)
        try:
            zenergy_max = float(np.nanmax(d["zenergy"]))
            zaccel_max  = float(np.nanmax(d["zaccel"]))
        except Exception:
            zenergy_max, zaccel_max = np.nan, np.nan

        buy_ok = buy_signal_champion(d)
        sig = simulate_sellraw_only(d, buy_ok)

        print(
            f"[WINNER CFG] zaccel_gate={P['zaccel_gate']} zenergy_min={ENTRY_ZENERGY_MIN} k_struct={ENTRY_K_STRUCT} | "
            f"zenergy_max={zenergy_max:.4f} zaccel_max={zaccel_max:.4f}",
            flush=True
        )

        # 5) Leer señal en la vela cerrada exacta
        ts = last_close_utc_minus1
        if ts not in sig.index:
            print(f"⚠️ [{symbol}] No encontré ts exacto en winner signals: {ts}", flush=True)
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
            guardar_estado_actual(estado_actual)
            return

        row = sig.loc[ts]
        curr_clean = "BUY" if bool(row["BUY"]) else "SELL" if bool(row["SELL"]) else None
        btc_price  = float(d.loc[ts, "Close"])
        fecha_utc  = ts

        # 6) Anti-caídas: transición real + last_close_ms distinto
        signal = None
        if curr_clean in ["BUY", "SELL"] and curr_clean != prev_signal:
            signal = curr_clean

        debe_enviar = (last_close_ms != prev_close) and (signal in ["BUY", "SELL"])

        print(
            f"[{symbol}] prev_signal={prev_signal} | curr={curr_clean} | "
            f"ts={fecha_utc} | last_close_ms={last_close_ms} prev_close={prev_close} | "
            f"signal={signal} | EXEC? {debe_enviar}",
            flush=True
        )

        # 7) Ejecutar/enviar si corresponde
        if debe_enviar:
            emoji = "🟢" if signal == "BUY" else "🔴"
            mensaje = (
                f"{emoji} {signal} TRIGGER {symbol} → TRADE {TRADE_SYMBOL}\n"
                f"📌 Trigger price ({symbol}): {btc_price:,.4f}\n"
                f"🕒 {fecha_utc}\n"
                f"⚙️ winner: zaccel_gate={P['zaccel_gate']} zenergy_min={ENTRY_ZENERGY_MIN} k_struct={ENTRY_K_STRUCT}\n"
            )

            enviar_mensaje_telegram(mensaje)

            try:
                trade_result = route_signal({"symbol": TRADE_SYMBOL, "side": signal})
                print(f"[TRADE {TRADE_SYMBOL}] ✅ Resultado {signal}: {trade_result}", flush=True)
            except Exception as e:
                print(f"⚠️ [TRADE {TRADE_SYMBOL}] Error {signal} (route_signal): {e}", flush=True)

            estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}
        else:
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}

    except Exception as e:
        print(f"❌ Error procesando trigger {TRIGGER_SYMBOL}: {e}", flush=True)

    print(f"💾 Guardando estado actual: {estado_actual}", flush=True)
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado", flush=True)


if __name__ == "__main__":
    main()
