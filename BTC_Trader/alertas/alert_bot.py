# ==========================================================
# alertas/alert_bot.py
# 5m BTC Trigger → BNB Execution (Spot/Margin via Router)
# - Estrategia ACTIVA: WINNER/CHAMPION (Energy + Structure)
# - Datos: Google Sheets (1200 velas) + wait/poll para esperar incremental job
# - Anti-caídas: estado.json (transición real + last_close_ms)
# - Telegram: incluye precio BTC (trigger) + precio BNB (trade)
# ==========================================================

import os
import sys
import time
import requests
import pandas as pd
import numpy as np
import pytz
from pandas.api.types import is_datetime64tz_dtype

# Asegurar imports desde raíz del repo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.load_from_sheets import load_symbol_df
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

# Espera para que el incremental job termine de escribir en Sheets
# (tu log: ~37s a ~60s total)
MAX_WAIT_SECONDS = int(os.getenv("MAX_WAIT_SECONDS", "90"))
POLL_EVERY_SEC   = int(os.getenv("POLL_EVERY_SEC", "6"))

# Para logs claros (Railway)
ALLOWED_SYMBOLS_ENV = (os.getenv("ALLOWED_SYMBOLS") or "").strip().upper()
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

print("==================================================", flush=True)
print("🚀 alert_bot.py — BTC Trigger → BNB Exec (5m) [SHEETS MODE]", flush=True)
print(f"🔧 DRY_RUN={DRY_RUN} | USE_MARGIN={USE_MARGIN}", flush=True)
print(f"🎯 TRIGGER_SYMBOL={TRIGGER_SYMBOL}", flush=True)
print(f"💱 TRADE_SYMBOL={TRADE_SYMBOL}", flush=True)
print(f"🔒 STRICT_TRADE_SYMBOL={STRICT_TRADE_SYMBOL} | ALLOWED_SYMBOLS={ALLOWED_SYMBOLS_ENV or '(not set)'}", flush=True)
print(f"⏳ WAIT: MAX_WAIT_SECONDS={MAX_WAIT_SECONDS} | POLL_EVERY_SEC={POLL_EVERY_SEC}", flush=True)
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
# Sheets helpers
# ==========================================================

CR = pytz.timezone("America/Costa_Rica")

def to_utc(ts) -> pd.Timestamp:
    """
    Acepta string/datetime/timestamp.
    Devuelve Timestamp tz-aware en UTC.
    """
    t = pd.to_datetime(ts)
    if getattr(t, "tzinfo", None) is None:
        # naive -> asumir CR (o la tz que uses en sheets)
        t = t.tz_localize(CR)
    # aware -> convertir
    return t.tz_convert("UTC")


def _parse_close_time_series(s: pd.Series) -> pd.DatetimeIndex:
    """
    Convierte 'Close time' a DatetimeIndex tz-aware en UTC, robusto a:
    - strings con offset (-06:00)
    - datetime tz-aware
    - mezcla rara (object) por lecturas durante escritura
    """
    if s is None or len(s) == 0:
        return pd.DatetimeIndex([], tz="UTC", name="ts")

    # 1) Si ya es datetime tz-aware -> tz_convert directo
    if is_datetime64tz_dtype(s):
        return pd.DatetimeIndex(s.dt.tz_convert("UTC"), name="ts")

    # 2) Forzar a string para evitar mezcla Timestamp/string/NaT en object
    #    (Google Sheets a veces devuelve cosas raras si se lee mientras escriben)
    s_str = s.astype(str).replace({"": np.nan, "None": np.nan, "nan": np.nan})

    # 3) Parsear. Si viene con offset (-06:00), pandas lo entiende.
    dt = pd.to_datetime(s_str, errors="coerce")

    # 4) Si quedó tz-aware (porque el string tenía -06:00) -> tz_convert
    try:
        if dt.dt.tz is not None:
            return pd.DatetimeIndex(dt.dt.tz_convert("UTC"), name="ts")
    except Exception:
        pass

    # 5) Si quedó naive -> asumir CR y localize, luego a UTC
    dt = dt.dt.tz_localize(CR, ambiguous="infer", nonexistent="shift_forward").dt.tz_convert("UTC")
    return pd.DatetimeIndex(dt, name="ts")

def _expected_last_close_utc(now_utc: pd.Timestamp) -> pd.Timestamp:
    """
    La vela "cerrada" más reciente tiene close_time = (floor_5m(now) - 1ms).
    Ej: 11:04:59.999...
    """
    floor_5m = now_utc.floor("5min")
    return floor_5m - pd.Timedelta(milliseconds=1)

def _load_ohlcv_from_sheet(symbol: str) -> pd.DataFrame:
    """
    Carga df desde Sheets y devuelve OHLCV con index = Close time UTC.
    Requiere columnas: Open, High, Low, Close, Volume, Close time
    """
    df = load_symbol_df(symbol)
    if df is None or df.empty:
        raise RuntimeError(f"[SHEETS] {symbol}: DF vacío")

    required = ["Open", "High", "Low", "Close", "Volume", "Close time"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"[SHEETS] {symbol}: faltan columnas {missing} (requiere {required})")

    close_utc = _parse_close_time_series(df["Close time"])
    out = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    out.index = pd.DatetimeIndex(close_utc, name="ts")
    out = out.sort_index()

    # numeric
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    out["Volume"] = out["Volume"].fillna(0.0)

    return out

def _wait_for_fresh_sheet(symbol: str, prev_close_ms: int) -> tuple[pd.DataFrame, pd.Timestamp, int]:
    """
    Espera hasta que Sheets tenga la última vela cerrada (o al menos una vela NUEVA vs prev_close_ms),
    para evitar leer antes de que termine el incremental job.

    Return:
      ohlcv (index=close_time_utc), ts_last (close_time_utc), last_close_ms (epoch ms aproximado)
    """
    t0 = time.time()
    last_err = None

    while True:
        try:
            ohlcv = _load_ohlcv_from_sheet(symbol)
            ts_last = ohlcv.index.max()
            if pd.isna(ts_last):
                raise RuntimeError("ts_last es NaT")

            # Aproximación last_close_ms: (ts_last + 1ms) en epoch ms
            # porque tu 'Close time' es ...:59.999 local, equivalente a close_ms-1 en el enfoque anterior
            last_close_ms = int((ts_last + pd.Timedelta(milliseconds=1)).value // 1_000_000)

            now_utc = pd.Timestamp.now(tz="UTC")
            expected = _expected_last_close_utc(now_utc)

            ok_expected = ts_last >= expected
            ok_new = last_close_ms != int(prev_close_ms or 0)

            if ok_expected or ok_new:
                # si está al día o al menos avanzó vs estado anterior, seguimos
                return ohlcv, ts_last, last_close_ms

            # Si no está fresco, esperamos
            waited = int(time.time() - t0)
            if waited >= MAX_WAIT_SECONDS:
                print(
                    f"⏳ [SHEETS] timeout esperando vela fresca. ts_last={ts_last} expected≈{expected} prev_close_ms={prev_close_ms}",
                    flush=True
                )
                return ohlcv, ts_last, last_close_ms

            print(
                f"⏳ [SHEETS] esperando incremental... ts_last={ts_last} expected≈{expected} (sleep {POLL_EVERY_SEC}s)",
                flush=True
            )
            time.sleep(POLL_EVERY_SEC)

        except Exception as e:
            last_err = e
            waited = int(time.time() - t0)
            if waited >= MAX_WAIT_SECONDS:
                raise RuntimeError(f"[SHEETS] No pude obtener data fresca para {symbol}. Último error: {last_err}")
            time.sleep(POLL_EVERY_SEC)
            
def _wait_trade_sheet_at_least(symbol: str, target_ts: pd.Timestamp) -> pd.DataFrame:
    """
    Espera hasta que el sheet de TRADE_SYMBOL tenga ts_last >= target_ts.
    """
    t0 = time.time()
    last_err = None

    while True:
        try:
            ohlcv = _load_ohlcv_from_sheet(symbol)
            ts_last = ohlcv.index.max()

            if pd.notna(ts_last) and ts_last >= target_ts:
                return ohlcv

            waited = int(time.time() - t0)
            if waited >= MAX_WAIT_SECONDS:
                print(f"⏳ [SHEETS] timeout esperando {symbol}. ts_last={ts_last} target_ts={target_ts}", flush=True)
                return ohlcv

            print(f"⏳ [SHEETS] esperando {symbol}... ts_last={ts_last} target_ts={target_ts}", flush=True)
            time.sleep(POLL_EVERY_SEC)

        except Exception as e:
            last_err = e
            waited = int(time.time() - t0)
            if waited >= MAX_WAIT_SECONDS:
                print(f"⚠️ [SHEETS] no pude leer {symbol} a tiempo: {last_err}", flush=True)
                return _load_ohlcv_from_sheet(symbol)
            time.sleep(POLL_EVERY_SEC)

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
# MAIN
# ==========================================================

def main():
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    symbol = TRIGGER_SYMBOL  # BTC

    try:
        print(f"\n===================== TRIGGER {symbol} =====================", flush=True)

        prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
        prev_signal = prev.get("signal")
        prev_close  = int(prev.get("last_close_ms") or 0)

        # 1) Esperar / leer velas de BTC desde Sheets
        btc_ohlcv, ts_last, last_close_ms = _wait_for_fresh_sheet(symbol, prev_close)

        # 2) Señales winner/champion sobre BTC
        d = build_features_winner(btc_ohlcv)

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

        # 3) La vela “a evaluar” es la ÚLTIMA cerrada en Sheets (ts_last)
        ts = ts_last
        if ts not in sig.index:
            print(f"⚠️ [{symbol}] No encontré ts exacto en winner signals: {ts}", flush=True)
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
            guardar_estado_actual(estado_actual)
            return

        row = sig.loc[ts]
        curr_clean = "BUY" if bool(row["BUY"]) else "SELL" if bool(row["SELL"]) else None

        btc_price = float(d.loc[ts, "Close"])

        # 4) Precio BNB desde Sheets (última vela cerrada)
        bnb_price = None
        try:
            bnb_ohlcv = _wait_trade_sheet_at_least(TRADE_SYMBOL, ts)
            bnb_price = float(bnb_ohlcv.loc[bnb_ohlcv.index.max(), "Close"])
        except Exception as e:
            print(f"[WARN] No pude obtener precio de {TRADE_SYMBOL} desde Sheets: {e}", flush=True)

        print(f"[PRICE] trigger {symbol}={btc_price:.2f} | trade {TRADE_SYMBOL}={bnb_price}", flush=True)

        # 5) Anti-caídas: transición real + vela nueva
        signal = None
        if curr_clean in ["BUY", "SELL"] and curr_clean != prev_signal:
            signal = curr_clean

        debe_enviar = (last_close_ms != prev_close) and (signal in ["BUY", "SELL"])

        print(
            f"[{symbol}] prev_signal={prev_signal} | curr={curr_clean} | "
            f"ts={ts} | last_close_ms={last_close_ms} prev_close={prev_close} | "
            f"signal={signal} | EXEC? {debe_enviar}",
            flush=True
        )

        # 6) Ejecutar/enviar
        if debe_enviar:
            emoji = "🟢" if signal == "BUY" else "🔴"

            mensaje = (
                f"{emoji} {signal} TRIGGER {symbol} → TRADE {TRADE_SYMBOL}\n"
                f"📌 Trigger price ({symbol}): {btc_price:,.4f}\n"
            )
            if bnb_price is not None:
                mensaje += f"💰 Trade price ({TRADE_SYMBOL}): {bnb_price:,.4f}\n"

            mensaje += (
                f"🕒 {ts}\n"
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
