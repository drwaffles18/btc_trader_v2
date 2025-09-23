# alertas/alertas_bot.py
# Bot de Telegram para enviar señales:
# - BUY: mensaje enriquecido con Entrada, SL (swing) y TPs por R:R (1.0x, 1.5x, 1.75x)
# - SELL: mensaje simple (como antes)
# Usa solo la ÚLTIMA vela 4H CERRADA (UTC) y evita look-ahead.

import os
import sys
import requests
import pandas as pd

# Agregar el path raíz para poder importar utils correctamente
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

# Token y chat ID desde variables de entorno
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def enviar_mensaje_telegram(mensaje: str):
    if not TOKEN or not CHAT_ID:
        print("❌ ERROR: TOKEN o CHAT_ID no definidos")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": mensaje}
    try:
        response = requests.post(url, data=payload, timeout=20)
        if response.status_code == 200:
            print("✅ Mensaje enviado correctamente")
        else:
            print(f"⚠️ Error al enviar mensaje: {response.text}")
    except Exception as e:
        print(f"⚠️ Excepción enviando mensaje a Telegram: {e}")

def _last_closed_for(symbol: str):
    """
    Devuelve (last_open_ms, last_close_ms, base_usada) de la ÚLTIMA vela 4H cerrada (UTC),
    probando bases recomendadas.
    """
    for base in bases_para(symbol):
        try:
            _k, last_open, last_close, server_ms = fetch_last_closed_kline(symbol, base)
            return last_open, last_close, base
        except Exception as e:
            print(f"[{symbol}] fallo confirmando última cerrada en {base}: {e}")
    raise RuntimeError(f"[{symbol}] No se pudo confirmar la última vela cerrada en ninguna base.")

def main():
    print("🚀 Iniciando verificación de señales...")
    # Lista de símbolos configurable por env
    env_symbols = os.getenv("SYMBOLS")
    if env_symbols:
        symbols = [s.strip().upper() for s in env_symbols.split(",") if s.strip()]
    else:
        symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]  # default

    # Parámetros para niveles (solo aplican para BUY)
    SL_METHOD = os.getenv("SL_METHOD", "window").lower()  # "window" | "fractal"
    SL_WINDOW = int(os.getenv("SL_WINDOW", "5"))
    SL_LEFT   = int(os.getenv("SL_LEFT", "2"))
    SL_RIGHT  = int(os.getenv("SL_RIGHT", "2"))
    ATR_K     = float(os.getenv("ATR_K", "0.0"))  # margen ATR opcional, ej. 0.2

    # RR targets (múltiplos de riesgo)
    RR_TARGETS = [float(x) for x in os.getenv("RR_TARGETS", "1.0,1.5,1.75").split(",")]

    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    for symbol in symbols:
        try:
            # 1) Confirma la ventana de la ÚLTIMA vela 4H CERRADA (UTC) y base usada
            last_open_ms, last_close_ms, base = _last_closed_for(symbol)
            last_open_utc  = pd.to_datetime(last_open_ms,  unit="ms", utc=True)
            # Binance reporta close time como endTime; para la vela cerrada anterior usamos last_close_ms-1
            last_close_utc_minus1 = pd.to_datetime(last_close_ms - 1, unit="ms", utc=True)

            # 2) Descarga histórico alineado a la MISMA base para evitar cortes distintos
            df = get_binance_4h_data(symbol, preferred_base=base)

            # 3) Calcula indicadores y señal sobre el histórico alineado
            df = calculate_indicators(df)
            df = calcular_momentum_integral(df, window=6)
            df = limpiar_señales_consecutivas(df, columna='Momentum Signal')  # crea/actualiza 'Signal Final'

            # 4) Selecciona EXACTAMENTE la vela cerrada por open & close
            fila = df[(df["Open time UTC"] == last_open_utc) & (df["Close time UTC"] == last_close_utc_minus1)]
            if fila.empty:
                # Fallback ±60s por cualquier drift, chequeando ambos bordes
                try:
                    open_ms_series  = (df["Open time UTC"].astype("int64")  // 1_000_000)
                    close_ms_series = (df["Close time UTC"].astype("int64") // 1_000_000)
                except Exception:
                    open_ms_series  = (df["Open time UTC"].view("int64")  // 1_000_000)
                    close_ms_series = (df["Close time UTC"].view("int64") // 1_000_000)

                df["_open_delta_ms"]  = open_ms_series  - last_open_ms
                df["_close_delta_ms"] = close_ms_series - (last_close_ms - 1)
                cand = df[(df["_open_delta_ms"].abs() <= 60_000) & (df["_close_delta_ms"].abs() <= 60_000)]
                if cand.empty:
                    print(f"⚠️ {symbol}: no encontré la vela cerrada EXACTA (open={last_open_utc}, close≈{last_close_utc_minus1}).")
                    continue
                fila = cand.iloc[[-1]]

            fila = fila.iloc[0]  # Series de la vela cerrada exacta

            # 5) Señal SOLO de la vela cerrada
            señal = fila.get('Signal Final', None)
            precio = float(fila.get('Close', float('nan')))
            # Mostrar HORA DE CIERRE en CR (ya viene en df por fetch)
            fecha_cr = fila.get('Close time')

            # 6) Registrar estado como ya hacías (almacena la señal actual)
            estado_actual[symbol] = señal

            # 7) Envío a Telegram (si hay señal y cambió)
            if señal in ['BUY', 'SELL']:
                if estado_anterior.get(symbol) != señal:

                    if señal == 'BUY':
                        # ===== BUY: mensaje enriquecido con SL/TPs (R:R) =====
                        # Recortar DF hasta la vela cerrada para evitar look-ahead
                        df_recorte = df[df["Open time UTC"] <= last_open_utc].copy()

                        # Validar columnas necesarias
                        for col in ["High", "Low", "Close"]:
                            if col not in df_recorte.columns:
                                raise RuntimeError(f"Falta columna {col} para calcular SL/TP en {symbol}")

                        # Construir niveles (R:R)
                        levels = build_levels(
                            df=df_recorte,
                            side='BUY',
                            entry=precio,
                            rr_targets=RR_TARGETS,   # múltiplos de riesgo (1.0x, 1.5x, 1.75x)
                            sl_method=SL_METHOD,
                            window=SL_WINDOW,
                            left=SL_LEFT,
                            right=SL_RIGHT,
                            atr_k=ATR_K
                        )

                        # Mensaje enriquecido (fecha de CIERRE)
                        mensaje = format_signal_msg(
                            symbol=symbol,
                            side='BUY',
                            levels=levels,
                            ts_local_str=str(fecha_cr),
                            source_url=base
                        )

                    else:
                        # ===== SELL: mensaje simple (como antes) =====
                        emoji = "🔴"
                        mensaje = (
                            f"{emoji} NUEVA SEÑAL para {symbol}:\n"
                            f"📍 SELL\n"
                            f"💵 Precio: {precio:,.4f}\n"
                            f"🕒 {fecha_cr} (CR)\n"
                            f"🔗 base: {base}"
                        )

                    print(f"📢 Enviando: {mensaje}")
                    enviar_mensaje_telegram(mensaje)

                else:
                    print(f"⏭️ {symbol} señal repetida ({señal}) en la última vela cerrada. No se reenvía.")
            else:
                print(f"ℹ️ {symbol}: vela 4H cerrada SIN señal (OK).")

        except Exception as e:
            print(f"❌ Error procesando {symbol}: {e}")

    guardar_estado_actual(estado_actual)
    print("✅ Finalizado")

if __name__ == "__main__":
    main()
