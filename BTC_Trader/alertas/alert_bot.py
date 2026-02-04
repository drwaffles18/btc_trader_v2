# ==========================================================
# alertas/alert_bot.py
# Versión 5m + Momentum Físico (BUY/SELL)
# Anti-caídas: usa estado.json para detectar transición real
# Soporta Spot o Margin vía USE_MARGIN (en trade_executor_router)
# ==========================================================

import os
import sys
import requests
import pandas as pd

# Asegurar imports desde raíz del repo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.indicators import calcular_momentum_fisico_speed
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.binance_fetch import (
    get_binance_5m_data,
    fetch_last_closed_kline_5m,
    bases_para,
)
from utils.risk_levels import build_levels, format_signal_msg

# Router de ejecución (Spot/Margin)
from utils.trade_executor_router import route_signal

from signal_tracker import cargar_estado_anterior, guardar_estado_actual


# ==========================================================
# Variables de entorno
# ==========================================================

BINANCE_API_KEY_TRADING    = os.getenv("BINANCE_API_KEY_TRADING")
BINANCE_API_SECRET_TRADING = os.getenv("BINANCE_API_SECRET_TRADING")

TRIGGER_SYMBOL = os.getenv("TRIGGER_SYMBOL", "BTCUSDT").upper()
TRADE_SYMBOL   = os.getenv("TRADE_SYMBOL", "BNBUSDT").upper()

DRY_RUN        = os.getenv("DRY_RUN", "false").lower() == "true"
STATE_PATH     = os.getenv("STATE_PATH", "./estado.json")
TRADE_LOG_PATH = os.getenv("TRADE_LOG_PATH", "./trade_logs.csv")

TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"
print(f"🔧 USE_MARGIN = {USE_MARGIN}", flush=True)

GRACE_MINUTES   = int(os.getenv("GRACE_MINUTES", "7"))
HISTORY_LIMIT_5M = int(os.getenv("HISTORY_LIMIT_5M", "900"))

SYMBOL_PARAMS = {
    "BTCUSDT": {"mom_win": 4, "speed_win": 9, "accel_win": 7, "zspeed_min": 0.3, "zaccel_min": 0.1},
    "ETHUSDT": {"mom_win": 7, "speed_win": 9, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.2},
    "ADAUSDT": {"mom_win": 4, "speed_win": 7, "accel_win": 5, "zspeed_min": 0.2, "zaccel_min": 0.3},
    "XRPUSDT": {"mom_win": 5, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.2, "zaccel_min": 0.0},
    "BNBUSDT": {"mom_win": 6, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.0},
}


# ==========================================================
# Telegram helper
# ==========================================================

def enviar_mensaje_telegram(mensaje: str):
    if DRY_RUN:
        print("💤 DRY_RUN → Telegram deshabilitado")
        return
    
    if not TOKEN or not CHAT_ID:
        print("❌ ERROR: TOKEN o CHAT_ID no definidos", flush=True)
        return
    try:
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": mensaje}, timeout=20)
        if r.status_code == 200:
            print("✅ Mensaje enviado correctamente", flush=True)
        else:
            print(f"⚠️ Error al enviar mensaje: {r.text}", flush=True)
    except Exception as e:
        print(f"⚠️ Excepción enviando mensaje a Telegram: {e}", flush=True)


# ==========================================================
# Última vela cerrada 5m
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
    print("🚀 Iniciando verificación de señales 5m...", flush=True)
    print(f"🎯 Trigger: {TRIGGER_SYMBOL}  |  💱 Trade: {TRADE_SYMBOL}", flush=True)

    # Cargar estado anterior (desde estado.json)
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    symbol = TRIGGER_SYMBOL  # SOLO BTC como trigger

    try:
        print(f"\n===================== TRIGGER {symbol} =====================", flush=True)

        # 1) Última vela 5m cerrada
        last_open_ms, last_close_ms, base, server_ms = _last_closed_for(symbol)
        last_open_utc         = pd.to_datetime(last_open_ms,       unit="ms", utc=True)
        last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1,   unit="ms", utc=True)

        prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
        prev_signal = prev.get("signal")
        prev_close  = int(prev.get("last_close_ms") or 0)

        # 2) Grace period
        if GRACE_MINUTES > 0:
            if (server_ms - last_close_ms) > GRACE_MINUTES * 60_000:
                print(f"⏭️ [{symbol}] Señal atrasada → ignorada. (server_ms - last_close_ms > grace)", flush=True)
                estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
                print(f"💾 Guardando estado actual: {estado_actual}", flush=True)
                guardar_estado_actual(estado_actual)
                print("✅ Finalizado", flush=True)
                return

        # 3) Descargar histórico 5m del TRIGGER (BTC)
        df = get_binance_5m_data(symbol, limit=HISTORY_LIMIT_5M, preferred_base=base)

        if symbol not in SYMBOL_PARAMS:
            raise RuntimeError(f"No hay params en SYMBOL_PARAMS para {symbol}")

        params = SYMBOL_PARAMS[symbol]
        df = calcular_momentum_fisico_speed(df, **params)

        # 4) Limpiar señales (evitar repetidas consecutivas)
        df_clean = limpiar_señales_consecutivas(df, columna='Momentum Signal')
        df['Signal Final'] = df_clean['Signal Final']

        # 5) Encontrar vela exacta
        exact = df[
            (df["Open time UTC"]  == last_open_utc) &
            (df["Close time UTC"] == last_close_utc_minus1)
        ]
        if exact.empty:
            print(f"⚠️ [{symbol}] No encontré la vela exacta para last_close_ms={last_close_ms}.", flush=True)
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
            print(f"💾 Guardando estado actual: {estado_actual}", flush=True)
            guardar_estado_actual(estado_actual)
            print("✅ Finalizado", flush=True)
            return

        fila = exact.iloc[0]
        curr_clean = fila['Signal Final']
        btc_price  = float(fila['Close'])
        fecha_cr   = fila['Close time']

        # 6) Señal anti-caídas basada en ESTADO del TRIGGER
        signal = None
        if curr_clean in ['BUY', 'SELL'] and curr_clean != prev_signal:
            signal = curr_clean

        # 7) Ejecutar/enviar si:
        debe_enviar = (last_close_ms != prev_close) and (signal in ['BUY', 'SELL'])

        print(
            f"[{symbol}] prev_signal={prev_signal} | curr_clean={curr_clean} | "
            f"last_close_ms={last_close_ms} | prev_close={prev_close} | "
            f"signal={signal} | ¿Debe enviar? {debe_enviar}",
            flush=True
        )

        # --------------------------------------------------
        # EJECUCIÓN: señal de BTC, trade en BNB
        # --------------------------------------------------
        if debe_enviar:

            # Mensaje de Telegram SIEMPRE con contexto de trigger/trade
            emoji = "🟢" if signal == "BUY" else "🔴"
            mensaje = (
                f"{emoji} {signal} TRIGGER {symbol} → TRADE {TRADE_SYMBOL}\n"
                f"📌 Trigger price ({symbol}): {btc_price:,.4f}\n"
                f"🕒 {fecha_cr}\n"
            )

            if DRY_RUN:
                print(f"💤 DRY_RUN activo → {mensaje}", flush=True)
                # Opcional: si querés que DRY_RUN sí mande Telegram, quita este if y llama enviar_mensaje_telegram.
            else:
                enviar_mensaje_telegram(mensaje)

                try:
                    trade_result = route_signal({"symbol": TRADE_SYMBOL, "side": signal})
                    print(f"[TRADE {TRADE_SYMBOL}] ✅ Resultado {signal}: {trade_result}", flush=True)
                except Exception as e:
                    print(f"⚠️ [TRADE {TRADE_SYMBOL}] Error {signal} (route_signal): {e}", flush=True)

            # Guardar estado del TRIGGER para evitar duplicados
            estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}
        else:
            # Importante: si NO ejecuta, igual conviene persistir el last_close_ms para no reprocesar
            estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}

    except Exception as e:
        print(f"❌ Error procesando trigger {TRIGGER_SYMBOL}: {e}", flush=True)
        # Si algo falló antes de calcular last_close_ms, no forzamos estado.

    print(f"💾 Guardando estado actual: {estado_actual}", flush=True)
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado", flush=True)



if __name__ == "__main__":
    main()
