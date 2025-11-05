# alertas/alertas_bot.py
# BUY: SL + TPs por R:R | SELL: simple
# Emula el gráfico: SOLO dispara en transición de `Signal Final` (primera vela del tramo)
# Usa la ÚLTIMA vela 4H CERRADA (UTC), validando open & close y alineando la misma base.

import os
import sys
import requests
import pandas as pd

# Import path raíz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.binance_fetch import (
    get_binance_4h_data,
    fetch_last_closed_kline,
    bases_para,
)
from utils.risk_levels import build_levels, format_signal_msg
from utils.trade_executor_v2 import route_signal  # 🧩 NUEVO: Autotrader
from signal_tracker import cargar_estado_anterior, guardar_estado_actual

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")



# Ventana de gracia (min) ABSOLUTA: bloquea cualquier envío tardío
GRACE_MINUTES = int(os.getenv("GRACE_MINUTES", "15"))

def enviar_mensaje_telegram(mensaje: str):
    if not TOKEN or not CHAT_ID:
        print("❌ ERROR: TOKEN o CHAT_ID no definidos")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": mensaje}
    try:
        r = requests.post(url, data=payload, timeout=20)
        if r.status_code == 200:
            print("✅ Mensaje enviado correctamente")
        else:
            print(f"⚠️ Error al enviar mensaje: {r.text}")
    except Exception as e:
        print(f"⚠️ Excepción enviando mensaje a Telegram: {e}")

def _last_closed_for(symbol: str):
    for base in bases_para(symbol):
        try:
            _k, last_open, last_close, server_ms = fetch_last_closed_kline(symbol, base)
            print(f"[{symbol}] Última cerrada confirmada en base {base} | "
                  f"open_ms={last_open} close_ms={last_close} server_ms={server_ms}")
            return last_open, last_close, base, server_ms
        except Exception as e:
            print(f"[{symbol}] fallo confirmando última cerrada en {base}: {e}")
    raise RuntimeError(f"[{symbol}] No se pudo confirmar la última vela cerrada en ninguna base.")

def main():
    print("🚀 Iniciando verificación de señales...")
    env_symbols = os.getenv("SYMBOLS")
    symbols = [s.strip().upper() for s in env_symbols.split(",")] if env_symbols else \
              ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

    SL_METHOD = os.getenv("SL_METHOD", "window").lower()
    SL_WINDOW = int(os.getenv("SL_WINDOW", "5"))
    SL_LEFT   = int(os.getenv("SL_LEFT", "2"))
    SL_RIGHT  = int(os.getenv("SL_RIGHT", "2"))
    ATR_K     = float(os.getenv("ATR_K", "0.0"))
    RR_TARGETS = [float(x) for x in os.getenv("RR_TARGETS", "1.0,1.5,1.75").split(",")]

    estado_anterior = cargar_estado_anterior()
    print(f"📥 Estado anterior cargado: {estado_anterior}")
    estado_actual = {}

    for symbol in symbols:
        try:
            print(f"\n===================== {symbol} =====================")
            last_open_ms, last_close_ms, base, server_ms = _last_closed_for(symbol)
            last_open_utc  = pd.to_datetime(last_open_ms,  unit="ms", utc=True)
            last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1, unit="ms", utc=True)

            prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
            prev_signal = prev.get("signal")
            prev_close  = prev.get("last_close_ms", 0)

            # Grace period
            if GRACE_MINUTES > 0:
                delta_ms = server_ms - last_close_ms
                if delta_ms > (GRACE_MINUTES * 60 * 1000):
                    print(f"⏭️ [{symbol}] vela cerrada hace más de {GRACE_MINUTES}m → no envío señal atrasada.")
                    estado_actual[symbol] = {"signal": None, "last_close_ms": last_close_ms}
                    continue

            print(f"[{symbol}] Descargando histórico con preferred_base={base} ...")
            df = get_binance_4h_data(symbol, preferred_base=base)
            df = calculate_indicators(df)
            df = calcular_momentum_integral(df, window=6)
            df_clean = limpiar_señales_consecutivas(df, columna='Momentum Signal')
            df['Signal Final'] = df_clean['Signal Final']

            exact = df[(df["Open time UTC"] == last_open_utc) &
                       (df["Close time UTC"] == last_close_utc_minus1)]
            if exact.empty:
                print(f"⚠️ [{symbol}] No encontré la vela cerrada EXACTA.")
                estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
                continue
            fila = exact.iloc[0]

            raw_signal  = fila.get('Momentum Signal', None)
            prop_signal = fila.get('Signal Final', None)
            price  = float(fila.get('Close', float('nan')))
            fecha_cr = fila.get('Close time')

            idx = df_clean.index.get_loc(fila.name)
            prev_clean = df_clean.iloc[idx-1]['Signal Final'] if idx > 0 else None
            curr_clean = prop_signal

            signal = None
            if curr_clean == 'BUY' and prev_clean != 'BUY':
                signal = 'BUY'
            elif curr_clean == 'SELL' and prev_clean != 'SELL':
                signal = 'SELL'

            debe_enviar = (last_close_ms != prev_close) and (signal in ['BUY', 'SELL'])
            print(f"[{symbol}] ¿Debe enviar? {debe_enviar} | signal={signal}")

            if debe_enviar:
                if signal == 'BUY':
                    # --- Cálculo de niveles ---
                    df_recorte = df[df["Open time UTC"] <= last_open_utc].copy()
                    for col in ["High", "Low", "Close"]:
                        if col not in df_recorte.columns:
                            raise RuntimeError(f"Falta columna {col} para calcular SL/TP en {symbol}")

                    levels = build_levels(
                        df=df_recorte,
                        side='BUY',
                        entry=price,
                        rr_targets=RR_TARGETS,
                        sl_method=SL_METHOD,
                        window=SL_WINDOW,
                        left=SL_LEFT,
                        right=SL_RIGHT,
                        atr_k=ATR_K
                    )
                    mensaje = format_signal_msg(
                        symbol=symbol,
                        side='BUY',
                        levels=levels,
                        ts_local_str=str(fecha_cr),
                        source_url=base
                    )

                    print(f"[{symbol}] 📢 Enviando:\n{mensaje}")
                    enviar_mensaje_telegram(mensaje)

                    # 🚀 Ejecutar trade (Market + OCO)
                    try:
                        rr_target = 1.5  # usa TP2
                        tp_price = levels['tps'][1] if len(levels['tps']) > 1 else levels['tps'][0]
                        sl_limit_pct = abs(price - levels['sl']) / price
                        trade_result = route_signal({
                            "symbol": symbol,
                            "side": "BUY",
                            "tp_price": tp_price,
                            "sl_limit_pct": sl_limit_pct,
                            "rr": rr_target
                        })
                        print(f"[{symbol}] 🛒 Resultado trade BUY: {trade_result}")
                    except Exception as e:
                        print(f"⚠️ [{symbol}] Error ejecutando trade BUY: {e}")

                elif signal == 'SELL':
                    mensaje = (
                        f"🔴 NUEVA SEÑAL para {symbol}:\n"
                        f"📍 SELL\n"
                        f"💵 Precio: {price:,.4f}\n"
                        f"🕒 {fecha_cr} (CR)\n"
                        f"🔗 base: {base}"
                    )

                    print(f"[{symbol}] 📢 Enviando:\n{mensaje}")
                    enviar_mensaje_telegram(mensaje)

                    # 🚀 Cancelar OCO y vender posición
                    try:
                        trade_result = route_signal({"symbol": symbol, "side": "SELL"})
                        print(f"[{symbol}] 💰 Resultado trade SELL: {trade_result}")
                    except Exception as e:
                        print(f"⚠️ [{symbol}] Error ejecutando trade SELL: {e}")

                estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}
            else:
                print(f"[{symbol}] ⏭️ No se envía (transición={signal is not None}, curr_clean={curr_clean}).")
                estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}

        except Exception as e:
            print(f"❌ Error procesando {symbol}: {e}")

    print(f"💾 Guardando estado actual: {estado_actual}")
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado")

if __name__ == "__main__":
    main()

