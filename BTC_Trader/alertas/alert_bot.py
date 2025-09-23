# alertas/alertas_bot.py
# BUY: SL + TPs por R:R | SELL: simple
import os
import sys
import requests
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.binance_fetch import (
    get_binance_4h_data,
    fetch_last_closed_kline,
    bases_para,
)
from utils.risk_levels import build_levels, format_signal_msg
from signal_tracker import cargar_estado_anterior, guardar_estado_actual

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GRACE_MINUTES = int(os.getenv("GRACE_MINUTES", "15"))  # ventana de gracia post-cierre

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
    """
    Devuelve (last_open_ms, last_close_ms, base_usada, server_ms)
    """
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

    estado_anterior = cargar_estado_anterior()  # {symbol: {"signal": "...", "last_close_ms": int}}
    print(f"📥 Estado anterior cargado: {estado_anterior}")
    estado_actual = {}

    for symbol in symbols:
        try:
            print(f"\n===================== {symbol} =====================")
            # 1) Última vela cerrada (UTC) + base + hora servidor
            last_open_ms, last_close_ms, base, server_ms = _last_closed_for(symbol)
            last_open_utc  = pd.to_datetime(last_open_ms,  unit="ms", utc=True)
            last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1, unit="ms", utc=True)
            print(f"[{symbol}] Ventana cerrada: open_utc={last_open_utc} | close_utc≈{last_close_utc_minus1} | base={base}")

            # 2) Ventana de gracia (opcional)
            if GRACE_MINUTES > 0:
                delta_ms = server_ms - last_close_ms
                print(f"[{symbol}] Δ(server_ms - last_close_ms) = {delta_ms} ms (grace={GRACE_MINUTES}m)")
                if delta_ms > (GRACE_MINUTES * 60 * 1000):
                    print(f"⏭️ [{symbol}] fuera de ventana de gracia → no envío esta vela.")
                    prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
                    estado_actual[symbol] = {"signal": prev.get("signal"), "last_close_ms": last_close_ms}
                    continue

            # 3) Descarga histórico ALINEADO a la MISMA base
            print(f"[{symbol}] Descargando histórico con preferred_base={base} ...")
            df = get_binance_4h_data(symbol, preferred_base=base)
            print(f"[{symbol}] Histórico recibido: filas={len(df)} rango={df['Open time UTC'].iloc[0]} → {df['Open time UTC'].iloc[-1]}")

            # 4) Indicadores y señal
            df = calculate_indicators(df)
            df = calcular_momentum_integral(df, window=6)
            df = limpiar_señales_consecutivas(df, columna='Momentum Signal')

            # 5) Selección exacta de la vela cerrada (open & close)
            exact = df[(df["Open time UTC"] == last_open_utc) & (df["Close time UTC"] == last_close_utc_minus1)]
            print(f"[{symbol}] Match exacto open&close: {len(exact)} filas")
            if exact.empty:
                # Fallback ±60s en ambos bordes
                try:
                    open_ms_series  = (df["Open time UTC"].astype("int64")  // 1_000_000)
                    close_ms_series = (df["Close time UTC"].astype("int64") // 1_000_000)
                except Exception:
                    open_ms_series  = (df["Open time UTC"].view("int64")  // 1_000_000)
                    close_ms_series = (df["Close time UTC"].view("int64") // 1_000_000)

                df["_open_delta_ms"]  = open_ms_series  - last_open_ms
                df["_close_delta_ms"] = close_ms_series - (last_close_ms - 1)
                cand = df[(df["_open_delta_ms"].abs() <= 60_000) & (df["_close_delta_ms"].abs() <= 60_000)]
                print(f"[{symbol}] Fallback ±60s: candidatos={len(cand)}")
                if cand.empty:
                    print(f"⚠️ [{symbol}] no encontré la vela cerrada EXACTA (open={last_open_utc}, close≈{last_close_utc_minus1}).")
                    continue
                fila = cand.iloc[[-1]]
            else:
                fila = exact

            fila = fila.iloc[0]
            signal = fila.get('Signal Final', None)
            price  = float(fila.get('Close', float('nan')))
            fecha_cr = fila.get('Close time')  # mostramos CIERRE en CR
            print(f"[{symbol}] Señal detectada en vela cerrada: signal={signal} price={price:,.4f} fecha_CR={fecha_cr}")

            # 6) Deduplicación por vela
            prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
            prev_signal = prev.get("signal")
            prev_close  = prev.get("last_close_ms", 0)
            debe_enviar = (last_close_ms != prev_close) or (signal != prev_signal)

            print(f"[{symbol}] Estado previo: signal={prev_signal} last_close_ms={prev_close}")
            print(f"[{symbol}] Estado actual : signal={signal} last_close_ms={last_close_ms}")
            print(f"[{symbol}] ¿Debe enviar? {debe_enviar}")

            if signal in ['BUY', 'SELL'] and debe_enviar:
                if signal == 'BUY':
                    # Recorte para niveles (evita look-ahead)
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
                    print(f"[{symbol}] Niveles calculados: SL={levels['sl']:.6f} TPs={', '.join(f'{t:.6f}' for t in levels['tps'])} RR={levels['rr']}")
                    mensaje = format_signal_msg(
                        symbol=symbol,
                        side='BUY',
                        levels=levels,
                        ts_local_str=str(fecha_cr),
                        source_url=base
                    )
                else:
                    mensaje = (
                        f"🔴 NUEVA SEÑAL para {symbol}:\n"
                        f"📍 SELL\n"
                        f"💵 Precio: {price:,.4f}\n"
                        f"🕒 {fecha_cr} (CR)\n"
                        f"🔗 base: {base}"
                    )

                print(f"[{symbol}] 📢 Enviando:\n{mensaje}")
                enviar_mensaje_telegram(mensaje)
                estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}
            else:
                print(f"[{symbol}] ⏭️ No se envía (signal={signal}, prev_signal={prev_signal}, "
                      f"last_close_ms={last_close_ms}, prev_close={prev_close}).")
                estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}

        except Exception as e:
            print(f"❌ Error procesando {symbol}: {e}")

    print(f"💾 Guardando estado actual: {estado_actual}")
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado")

if __name__ == "__main__":
    main()
