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
from signal_tracker import cargar_estado_anterior, guardar_estado_actual

TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Ventana de gracia (min) SOLO para re-ENVIOS tardíos de una vela ya registrada
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
    """
    Devuelve (last_open_ms, last_close_ms, base_usada, server_ms)
    consultando /time y /klines con endTime=last_close-1 para evitar velas en curso.
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

    # Parámetros para niveles (solo aplican para BUY)
    SL_METHOD = os.getenv("SL_METHOD", "window").lower()  # "window" | "fractal"
    SL_WINDOW = int(os.getenv("SL_WINDOW", "5"))
    SL_LEFT   = int(os.getenv("SL_LEFT", "2"))
    SL_RIGHT  = int(os.getenv("SL_RIGHT", "2"))
    ATR_K     = float(os.getenv("ATR_K", "0.0"))
    RR_TARGETS = [float(x) for x in os.getenv("RR_TARGETS", "1.0,1.5,1.75").split(",")]

    estado_anterior = cargar_estado_anterior()  # {SYM: {"signal": str|None, "last_close_ms": int}}
    print(f"📥 Estado anterior cargado: {estado_anterior}")
    estado_actual = {}

    for symbol in symbols:
        try:
            print(f"\n===================== {symbol} =====================")
            # 1) Confirmar última vela 4H CERRADA (UTC) + base + hora server
            last_open_ms, last_close_ms, base, server_ms = _last_closed_for(symbol)
            last_open_utc  = pd.to_datetime(last_open_ms,  unit="ms", utc=True)
            last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1, unit="ms", utc=True)
            print(f"[{symbol}] Ventana cerrada: open_utc={last_open_utc} | close_utc≈{last_close_utc_minus1} | base={base}")

            # 2) Cargar estado previo
            prev = estado_anterior.get(symbol, {"signal": None, "last_close_ms": 0})
            prev_signal = prev.get("signal")
            prev_close  = prev.get("last_close_ms", 0)

            # 3) Gracia CONDICIONAL: si ya registramos esta vela y estamos tarde, no reenviar
            if GRACE_MINUTES > 0:
                delta_ms = server_ms - last_close_ms
                print(f"[{symbol}] Δ(server_ms - last_close_ms) = {delta_ms} ms (grace={GRACE_MINUTES}m) | prev_close={prev_close}")
                if (prev_close == last_close_ms) and (delta_ms > (GRACE_MINUTES * 60 * 1000)):
                    print(f"⏭️ [{symbol}] fuera de ventana de gracia y la vela ya estaba registrada → no envío.")
                    estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
                    continue

            # 4) Descarga histórico ALINEADO a la MISMA base
            print(f"[{symbol}] Descargando histórico con preferred_base={base} ...")
            df = get_binance_4h_data(symbol, preferred_base=base)
            print(f"[{symbol}] Histórico recibido: filas={len(df)} "
                  f"rango={df['Open time UTC'].iloc[0]} → {df['Open time UTC'].iloc[-1]}")

            # 5) Indicadores + señal cruda
            df = calculate_indicators(df)
            df = calcular_momentum_integral(df, window=6)

            # 6) Señal “limpia” como en el chart (propagada); NO se usa sola para disparar,
            #     sino para detectar TRANSICIÓN (cambio de tramo) igual que en la app.
            df_clean = limpiar_señales_consecutivas(df, columna='Momentum Signal')
            df['Signal Final'] = df_clean['Signal Final']

            # 7) Selección exacta de la vela cerrada (open & close)
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
                    estado_actual[symbol] = {"signal": prev_signal, "last_close_ms": last_close_ms}
                    continue
                fila = cand.iloc[[-1]]
            else:
                fila = exact

            fila = fila.iloc[0]

            # 8) Señales de ESTA vela
            raw_signal  = fila.get('Momentum Signal', None)   # cruda (info)
            prop_signal = fila.get('Signal Final', None)      # “limpia” (chart)
            price  = float(fila.get('Close', float('nan')))
            fecha_cr = fila.get('Close time')                 # hora de cierre en CR
            print(f"[{symbol}] Señal cruda en vela cerrada: Momentum={raw_signal} | Propagada={prop_signal} | price={price:,.4f} fecha_CR={fecha_cr}")

            # 9) Emular el gráfico: disparar SOLO si hay TRANSICIÓN de Signal Final
            #    (primera vela del nuevo tramo BUY/SELL)
            #    Buscamos la fila previa en df_clean por índice
            try:
                idx = df_clean.index.get_loc(fila.name)
            except Exception:
                # Fallback robusto si el index difiere: localizar por timestamp
                idx = df_clean.index[df_clean["Open time UTC"] == fila["Open time UTC"]][0]
            prev_clean = df_clean.iloc[idx-1]['Signal Final'] if idx > 0 else None
            curr_clean = prop_signal

            signal = None
            if curr_clean == 'BUY' and prev_clean != 'BUY':
                signal = 'BUY'
            elif curr_clean == 'SELL' and prev_clean != 'SELL':
                signal = 'SELL'

            print(f"[{symbol}] Chart-like transition: prev_clean={prev_clean} -> curr_clean={curr_clean} => signal={signal}")

            # 10) DEDUP estricto: máximo 1 envío por vela
            debe_enviar = (last_close_ms != prev_close) and (signal in ['BUY', 'SELL'])
            print(f"[{symbol}] Estado previo: signal={prev_signal} last_close_ms={prev_close}")
            print(f"[{symbol}] Estado actual : signal={signal} last_close_ms={last_close_ms}")
            print(f"[{symbol}] ¿Debe enviar? {debe_enviar}")

            if debe_enviar:
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
                    print(f"[{symbol}] Niveles calculados: SL={levels['sl']:.6f} "
                          f"TPs={', '.join(f'{t:.6f}' for t in levels['tps'])} RR={levels['rr']}")
                    mensaje = format_signal_msg(
                        symbol=symbol,
                        side='BUY',
                        levels=levels,
                        ts_local_str=str(fecha_cr),
                        source_url=base
                    )
                else:
                    # SELL simple como en la app/mensajes anteriores
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
                print(f"[{symbol}] ⏭️ No se envía (transición={signal is not None}, curr_clean={curr_clean}).")
                estado_actual[symbol] = {"signal": signal, "last_close_ms": last_close_ms}

        except Exception as e:
            print(f"❌ Error procesando {symbol}: {e}")

    print(f"💾 Guardando estado actual: {estado_actual}")
    guardar_estado_actual(estado_actual)
    print("✅ Finalizado")

if __name__ == "__main__":
    main()
