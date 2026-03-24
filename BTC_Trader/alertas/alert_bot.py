# ==========================================================
# alertas/alert_bot.py
# 5m BTC Trigger → BNB Execution (Spot/Margin via Router)
# - Estrategia ACTIVA: WINNER/CHAMPION (Energy + Structure)
# - Datos: Google Sheets (1200 velas) + wait/poll para esperar incremental job
# - Anti-caídas: estado.json (transición real + last_close_ms)
# - Telegram: incluye precio BTC (trigger) + precio BNB (trade)
# - PRIORIDAD 1:
#     * retry real de ejecución
#     * alerta crítica real cuando falle
#     * commit de estado SOLO si el trade se ejecutó
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
from utils.trade_executor_margin import get_margin_position_state
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
MAX_WAIT_SECONDS = int(os.getenv("MAX_WAIT_SECONDS", "90"))
POLL_EVERY_SEC   = int(os.getenv("POLL_EVERY_SEC", "6"))

# Retry de ejecución (PRIORIDAD 1)
MAX_ROUTE_RETRIES      = int(os.getenv("MAX_ROUTE_RETRIES", "3"))
ROUTE_RETRY_SLEEP_SEC  = int(os.getenv("ROUTE_RETRY_SLEEP_SEC", "3"))

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
print(f"🔁 EXEC RETRY: MAX_ROUTE_RETRIES={MAX_ROUTE_RETRIES} | ROUTE_RETRY_SLEEP_SEC={ROUTE_RETRY_SLEEP_SEC}", flush=True)
print("==================================================", flush=True)


# ==========================================================
# LEGACY PARAMS (NO se usan en la estrategia activa)
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
    z_win=20,
    zspeed_min=0.30,
    zaccel_min=0.10,
    zaccel_gate=4.0
)

ENERGY_ZWIN = 120
STRUCT_ZWIN = 120
STRUCT_WIN  = 48
DON_WIN     = 48

ENTRY_ZENERGY_MIN = 1.8
ENTRY_K_STRUCT    = 0.4
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


def enviar_alerta_critica_trade(
    signal: str,
    trigger_symbol: str,
    trade_symbol: str,
    trigger_price: float | None,
    trade_price: float | None,
    ts: pd.Timestamp,
    trade_result: dict,
):
    """
    Alerta CRÍTICA cuando una ejecución falla después de existir una señal válida.
    """
    ts_cr = ts.tz_convert(CR) if getattr(ts, "tzinfo", None) is not None else pd.Timestamp(ts).tz_localize("UTC").tz_convert(CR)

    lines = [
        "🚨 CRITICAL TRADE FAILURE",
        f"Signal: {signal}",
        f"Trigger: {trigger_symbol}",
        f"Trade: {trade_symbol}",
        f"Status: {trade_result.get('status')}",
        f"Executed: {trade_result.get('executed')}",
        f"Error: {trade_result.get('error')}",
        f"Trade ID: {trade_result.get('trade_id')}",
        f"Time: {ts_cr.isoformat()}",
    ]

    if trigger_price is not None:
        lines.append(f"Trigger price: {trigger_price:,.4f}")
    if trade_price is not None:
        lines.append(f"Trade price: {trade_price:,.4f}")

    enviar_mensaje_telegram("\n".join(lines))

def evaluar_reconciliacion_pre_trade(signal: str, symbol: str) -> dict:
    """
    Revisa la posición REAL en Binance antes de ejecutar.

    Reglas:
      - BUY + ya hay posición real  -> BLOCK
      - SELL + no hay posición real -> BLOCK
      - si no se puede leer Binance -> BLOCK por seguridad
    """
    recon = get_margin_position_state(symbol)

    if not recon.get("ok", False):
        return {
            "allow_trade": False,
            "reason": "RECON_FAILED",
            "recon": recon,
        }

    has_position = bool(recon.get("has_position", False))

    if signal == "BUY" and has_position:
        return {
            "allow_trade": False,
            "reason": "BLOCK_BUY_ALREADY_OPEN",
            "recon": recon,
        }

    if signal == "SELL" and not has_position:
        return {
            "allow_trade": False,
            "reason": "BLOCK_SELL_NO_POSITION",
            "recon": recon,
        }

    return {
        "allow_trade": True,
        "reason": "OK",
        "recon": recon,
    }

def enviar_alerta_reconciliacion(
    signal: str,
    trigger_symbol: str,
    trade_symbol: str,
    ts: pd.Timestamp,
    recon_eval: dict,
):
    recon = recon_eval.get("recon", {}) or {}
    ts_cr = ts.tz_convert(CR) if getattr(ts, "tzinfo", None) is not None else pd.Timestamp(ts).tz_localize("UTC").tz_convert(CR)

    mensaje = (
        "🚨 RECONCILIATION BLOCK\n"
        f"Signal: {signal}\n"
        f"Trigger: {trigger_symbol}\n"
        f"Trade: {trade_symbol}\n"
        f"Reason: {recon_eval.get('reason')}\n"
        f"Time: {ts_cr.isoformat()}\n"
        f"Recon status: {recon.get('status')}\n"
        f"Has position: {recon.get('has_position')}\n"
        f"Free qty: {recon.get('free_qty')}\n"
        f"Net qty: {recon.get('net_asset_qty')}\n"
        f"Borrowed USDT: {recon.get('borrowed_usdt')}\n"
        f"Margin level: {recon.get('margin_level')}\n"
        f"Error: {recon.get('error')}"
    )
    enviar_mensaje_telegram(mensaje)


# ==========================================================
# Sheets helpers
# ==========================================================

CR = pytz.timezone("America/Costa_Rica")

def to_utc(ts) -> pd.Timestamp:
    t = pd.to_datetime(ts)
    if getattr(t, "tzinfo", None) is None:
        t = t.tz_localize(CR)
    return t.tz_convert("UTC")


def _parse_close_time_series(s: pd.Series) -> pd.DatetimeIndex:
    if s is None or len(s) == 0:
        return pd.DatetimeIndex([], tz="UTC", name="ts")

    if is_datetime64tz_dtype(s):
        return pd.DatetimeIndex(s.dt.tz_convert("UTC"), name="ts")

    s_str = s.astype(str).replace({"": np.nan, "None": np.nan, "nan": np.nan})
    dt = pd.to_datetime(s_str, errors="coerce")

    try:
        if dt.dt.tz is not None:
            return pd.DatetimeIndex(dt.dt.tz_convert("UTC"), name="ts")
    except Exception:
        pass

    dt = dt.dt.tz_localize(CR, ambiguous="infer", nonexistent="shift_forward").dt.tz_convert("UTC")
    return pd.DatetimeIndex(dt, name="ts")


def _expected_last_close_utc(now_utc: pd.Timestamp) -> pd.Timestamp:
    floor_5m = now_utc.floor("5min")
    return floor_5m - pd.Timedelta(milliseconds=1)


def _load_ohlcv_from_sheet(symbol: str) -> pd.DataFrame:
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

    for c in ["Open", "High", "Low", "Close", "Volume"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    out["Volume"] = out["Volume"].fillna(0.0)
    return out


def _wait_for_fresh_sheet(symbol: str, prev_close_ms: int) -> tuple[pd.DataFrame, pd.Timestamp, int]:
    t0 = time.time()
    last_err = None

    while True:
        try:
            ohlcv = _load_ohlcv_from_sheet(symbol)
            ts_last = ohlcv.index.max()
            if pd.isna(ts_last):
                raise RuntimeError("ts_last es NaT")

            last_close_ms = int((ts_last + pd.Timedelta(milliseconds=1)).value // 1_000_000)

            now_utc = pd.Timestamp.now(tz="UTC")
            expected = _expected_last_close_utc(now_utc)

            ok_expected = ts_last >= expected
            ok_new = last_close_ms != int(prev_close_ms or 0)

            if ok_expected or ok_new:
                return ohlcv, ts_last, last_close_ms

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
# PRIORIDAD 1 — ejecución robusta
# ==========================================================

def ejecutar_trade_con_retry(
    signal: str,
    ts: pd.Timestamp,
    btc_price: float | None,
    bnb_price: float | None,
) -> dict:
    """
    Ejecuta route_signal con retry corto.
    Retorna SIEMPRE un dict canónico.
    """
    trade_result = {
        "status": "NOT_ATTEMPTED",
        "executed": False,
        "error": None,
        "trade_id": None,
    }

    payload = {
        "symbol": TRADE_SYMBOL,
        "side": signal,
        "context": {
            "ts": str(ts),
            "btc_price": btc_price,
            "bnb_price": bnb_price,
        }
    }

    for attempt in range(1, MAX_ROUTE_RETRIES + 1):
        try:
            print(f"🔁 [TRADE {TRADE_SYMBOL}] intento {attempt}/{MAX_ROUTE_RETRIES} → {signal}", flush=True)

            trade_result = route_signal(payload)

            print(
                f"[TRADE {TRADE_SYMBOL}] intento {attempt} resultado: "
                f"status={trade_result.get('status')} "
                f"executed={trade_result.get('executed')} "
                f"trade_id={trade_result.get('trade_id')} "
                f"error={trade_result.get('error')}",
                flush=True
            )

            if bool(trade_result.get("executed", False)) and trade_result.get("status") == "OK":
                return trade_result

        except Exception as e:
            trade_result = {
                "status": "ERROR",
                "executed": False,
                "error": str(e),
                "trade_id": None,
            }
            print(f"⚠️ [TRADE {TRADE_SYMBOL}] intento {attempt} excepción: {e}", flush=True)

        if attempt < MAX_ROUTE_RETRIES:
            time.sleep(ROUTE_RETRY_SLEEP_SEC)

    return trade_result


# ==========================================================
# MAIN
# ==========================================================

def main():
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    symbol = TRIGGER_SYMBOL

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
            f"[WINNER CFG] zaccel_gate={P['zaccel_gate']} zenergy_min={ENTRY_ZENERGY_MIN} "
            f"k_struct={ENTRY_K_STRUCT} | zenergy_max={zenergy_max:.4f} zaccel_max={zaccel_max:.4f}",
            flush=True
        )

        # 3) La vela “a evaluar” es la última cerrada
        ts = ts_last
        if ts not in sig.index:
            print(f"⚠️ [{symbol}] No encontré ts exacto en winner signals: {ts}", flush=True)
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
            guardar_estado_actual(estado_actual)
            return

        row = sig.loc[ts]
        curr_clean = "BUY" if bool(row["BUY"]) else "SELL" if bool(row["SELL"]) else None

        btc_price = float(d.loc[ts, "Close"])

        # 4) Precio BNB desde Sheets
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
        ##
        # 6) Ejecutar/enviar

        if debe_enviar:
            emoji = "🟢" if signal == "BUY" else "🔴"

            mensaje = (
                f"{emoji} {signal} TRIGGER {symbol} → TRADE {TRADE_SYMBOL}\n"
                f"📌 Trigger price ({symbol}): {btc_price:,.4f}\n"
            )
            if bnb_price is not None:
                mensaje += f"💰 Trade price ({TRADE_SYMBOL}): {bnb_price:,.4f}\n"

            ts_cr = ts.tz_convert(CR)
            mensaje += (
                f"🕒 {ts_cr.isoformat()}\n"
                f"⚙️ winner: zaccel_gate={P['zaccel_gate']} "
                f"zenergy_min={ENTRY_ZENERGY_MIN} "
                f"k_struct={ENTRY_K_STRUCT}\n"
            )

            enviar_mensaje_telegram(mensaje)

            # =====================================================
            # PRIORIDAD 2 — RECONCILIACIÓN PRE-TRADE
            # =====================================================
            if USE_MARGIN:
                recon_eval = evaluar_reconciliacion_pre_trade(
                    signal=signal,
                    symbol=TRADE_SYMBOL,
                )
            else:
                recon_eval = {
                    "allow_trade": True,
                    "reason": "SPOT_MODE_SKIP_RECON",
                    "recon": {},
                }

            print(
                f"📡 [RECON_EVAL] allow_trade={recon_eval.get('allow_trade')} "
                f"reason={recon_eval.get('reason')} "
                f"recon_status={recon_eval.get('recon', {}).get('status')}",
                flush=True
            )

            if not recon_eval.get("allow_trade", False):
                estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}

                print(
                    f"⛔ [TRADE BLOCKED] signal={signal} "
                    f"reason={recon_eval.get('reason')}",
                    flush=True
                )

                enviar_alerta_reconciliacion(
                    signal=signal,
                    trigger_symbol=symbol,
                    trade_symbol=TRADE_SYMBOL,
                    ts=ts,
                    recon_eval=recon_eval,
                )

            else:
                trade_result = ejecutar_trade_con_retry(
                    signal=signal,
                    ts=ts,
                    btc_price=btc_price,
                    bnb_price=bnb_price,
                )

                print(f"[TRADE {TRADE_SYMBOL}] ✅ Resultado final {signal}: {trade_result}", flush=True)

                # Commit SOLO si el trade realmente se ejecutó
                if bool(trade_result.get("executed", False)) and trade_result.get("status") == "OK":
                    estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}
                    print(f"✅ [STATE] Commit de estado por ejecución real: {signal}", flush=True)
                else:
                    estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
                    print(
                        f"⚠️ [STATE] Sin commit de señal. "
                        f"trade_result.status={trade_result.get('status')} "
                        f"executed={trade_result.get('executed')}",
                        flush=True
                    )

                    enviar_alerta_critica_trade(
                        signal=signal,
                        trigger_symbol=symbol,
                        trade_symbol=TRADE_SYMBOL,
                        trigger_price=btc_price,
                        trade_price=bnb_price,
                        ts=ts,
                        trade_result=trade_result,
                    )
            ##
        else:
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}

    except Exception as e:
        print(f"❌ Error procesando trigger {TRIGGER_SYMBOL}: {e}", flush=True)
        estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": prev_close}

    print(f"💾 Guardando estado actual: {estado_actual}", flush=True)
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado", flush=True)


if __name__ == "__main__":
    main()
