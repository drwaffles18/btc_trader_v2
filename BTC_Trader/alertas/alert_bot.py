import os
import sys
import requests
from datetime import datetime, timezone
import pandas as pd

#from utils.trading_executor import ejecutar_operacion

#print("🛠️ PATH del script:", os.path.dirname(__file__))
#print("🧭 Agregando a sys.path:", os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Agregar el path raíz para poder importar utils correctamente
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.binance_fetch import get_binance_4h_data
from signal_tracker import cargar_estado_anterior, guardar_estado_actual

# Token y chat ID desde variables de entorno
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def enviar_mensaje_telegram(mensaje):
    if not TOKEN or not CHAT_ID:
        print("❌ ERROR: TOKEN o CHAT_ID no definidos")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": mensaje
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        print("✅ Mensaje enviado correctamente")
    else:
        print(f"⚠️ Error al enviar mensaje: {response.text}")

def procesar_symbol(symbol):
    print(f"🔄 Procesando {symbol}")
    df = get_binance_4h_data(symbol)
    df = calculate_indicators(df)
    df = calcular_momentum_integral(df, window=6)
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')
    df['Signal Final'] = df['Momentum Signal']
    return df

def main():
    print("🚀 Iniciando verificación de señales...")
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"]
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    for symbol in symbols:
        try:
            df = procesar_symbol(symbol)
            # Obtener última fila
            ultima = df.dropna(subset=['Signal Final']).iloc[-1]
            
            # Verificar si la vela está cerrada
            hora_actual = datetime.now(timezone.utc)
            hora_ultima_vela = ultima['Open time'] + pd.Timedelta(hours=4)
            
            if hora_actual < hora_ultima_vela:
                print(f"⏳ La vela de {symbol} aún no está cerrada. Saltando.")
                continue
            señal = ultima['Signal Final']
            fecha = ultima['Open time']

            estado_actual[symbol] = señal

            if estado_anterior.get(symbol) != señal and señal in ['BUY', 'SELL']:
                emoji = "🟢" if señal == "BUY" else "🔴"
                mensaje = f"{emoji} NUEVA SEÑAL para {symbol}:\n📍 {señal}\n🕒 {fecha}"
                print(f"📢 Enviando: {mensaje}")
                enviar_mensaje_telegram(mensaje)
                # 🔁 Ejecutar trade automáticamente
                #ejecutar_operacion(symbol, señal, estado_anterior.get(symbol))
            else:
                print(f"⏭️ No hay nueva señal para {symbol} ({señal})")

        except Exception as e:
            print(f"❌ Error procesando {symbol}: {e}")

    guardar_estado_actual(estado_actual)
    print("✅ Finalizado")

if __name__ == "__main__":
    main()
