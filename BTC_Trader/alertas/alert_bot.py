# alertas/alertas_bot.py
# Bot de Telegram para enviar señales con Entrada, SL por swing y TPs (1.0%, 1.5%, 1.75%)
# Usa solo la ÚLTIMA vela 4H CERRADA (UTC) y evita look-ahead.

import os
import sys
import requests
import pandas as pd
from datetime import timezone

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

def procesar_symbol(symbol: str) -> pd.DataFrame:
    """
    Descarga histórico 4H, calcula indicadores y la señal, y limpia consecutivas.
    NO decide si la vela está cerrada.
    """
    print(f"🔄 Procesando {symbol}")
    df = get_binance_4h_data(symbol)
    df = calculate_indicators(df)
    df = calcular_momentum_integral(df, window=6)
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')  # crea/actualiza 'Signal Final'
    return df

def main():
    print("🚀 Iniciando verificación de señales...")
    # Puedes ajustar esta lista por env: SYMBOLS="BTCUSDT,ETHUSDT,ADAUSDT,XRPUSDT,BNBUSDT"
    env_symbols = os.getenv("SYMBOLS")
    if env_symbols:
        symbols = [s.strip().upper() for s in env_symbols.split(",") if s.strip()]
    else:
        symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]  # default

    # Parámetros de niveles (ajustables por env)
    # SL method: "window" (simple) o "fractal" (más estricto). Ventana por defecto=5 velas.
    SL_METHOD = os.getenv("SL_METHOD", "window").lower()  # "window" | "fractal"
    SL_WINDOW = int(os.getenv("SL_WINDOW", "5"))
    SL_LEFT   = int(os.getenv("SL_LEFT", "2"))
    SL_RIGHT  = int(os.getenv("SL_RIGHT", "2"))
    ATR_K     = float(os.getenv("ATR_K", "0.0"))  # margen ATR opcional, ej. 0.2

    # TPs en %
    TP_PERCENTS = [float(x) for x in os.getenv("TP_PERCENTS", "1.0,1.5,1.75").split(",")]

    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    for symbol in symbols:
        try:
            # 1) Calcula indicadores normalmente (histórico)
            df = procesar_symbol(symbol)

            # 2) Confirma la ventana de la ÚLTIMA vela 4H CERRADA (UTC)
            last_open_ms, last_close_ms, base = _last_closed_for(symbol)
            last_open_utc = pd.to_datetime(last_open_ms, unit="ms", utc=True)

            # 3) Selecciona EXACTAMENTE esa vela por 'Open time UTC'
            fila = df[df["Open time UTC"] == last_open_utc]
            if fila.empty:
                # Fallback por posibles desalineaciones de segundos: busca ±60s
                try:
                    ms_series = (df["Open time UTC"].astype("int64") // 1_000_000)
                except Exception:
                    # Fallback alterno, por si cambia el dtype en alguna versión
                    ms_series = (df["Open time UTC"].view("int64") // 1_000_000)

                df["_delta_ms"] = ms_series - last_open_ms
                cand = df[df["_delta_ms"].abs() <= 60_000]
                if cand.empty:
                    print(f"⚠️ {symbol}: no encontré la fila de la vela cerrada (open_utc={last_open_utc}).")
                    continue
                fila = cand.iloc[[-1]]

            fila = fila.iloc[0]  # Series de la vela cerrada

            # 4) Señal SOLO de la vela cerrada
            señal = fila.get('Signal Final', None)
            precio = float(fila.get('Close', float('nan')))
            fecha_cr = fila.get('Open time')  # en CR (string/ts bonito para mostrar)

            # 5) Registrar estado como ya hacías (almacena la señal actual)
            estado_actual[symbol] = señal

            # 6) Envío a Telegram (si hay señal y cambió)
            if señal in ['BUY', 'SELL']:
                if estado_anterior.get(symbol) != señal:
                    # ----- NUEVO: SL/TPs -----
                    # Recortar DF hasta la vela cerrada para evitar look-ahead
                    df_recorte = df[df["Open time UTC"] <= last_open_utc].copy()

                    # Validar columnas necesarias
                    for col in ["High", "Low", "Close"]:
                        if col not in df_recorte.columns:
                            raise RuntimeError(f"Falta columna {col} para calcular SL/TP en {symbol}")

                    # Construir niveles (parámetros ajustables por env)
                    levels = build_levels(
                        df=df_recorte,
                        side=señal,
                        entry=precio,
                        tp_percents=TP_PERCENTS,
                        sl_method=SL_METHOD,  # "window" o "fractal"
                        window=SL_WINDOW,
                        left=SL_LEFT,
                        right=SL_RIGHT,
                        atr_k=ATR_K           # ej. 0.2 para despegar SL del swing
                    )

                    # Mensaje enriquecido
                    mensaje = format_signal_msg(
                        symbol=symbol,
                        side=señal,
                        levels=levels,
                        ts_local_str=str(fecha_cr),
                        source_url=base
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
