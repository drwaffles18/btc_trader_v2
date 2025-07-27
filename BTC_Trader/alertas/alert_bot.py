from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.binance_fetch import get_binance_4h_data
from signal_tracker import cargar_estado_anterior, guardar_estado_actual
import requests

# Token y chat ID (ya definidos)
TOKEN = "8376556528:AAElgljnyZ9DbBXBIPuLEKxkaZdbp-j8j38"
CHAT_ID = 7575887942

def enviar_mensaje_telegram(mensaje):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": mensaje
    }
    requests.post(url, data=payload)

def procesar_symbol(symbol):
    df = get_binance_4h_data(symbol)
    df = calculate_indicators(df)
    df = calcular_momentum_integral(df, window=6)
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')
    df['Signal Final'] = df['Momentum Signal']
    return df

def main():
    symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"]
    estado_anterior = cargar_estado_anterior()
    estado_actual = {}

    for symbol in symbols:
        df = procesar_symbol(symbol)
        ultima = df.dropna(subset=['Signal Final']).iloc[-1]
        señal = ultima['Signal Final']
        fecha = ultima['Open time']

        # Guardamos el estado actual
        estado_actual[symbol] = señal

        # Comparar con última señal enviada
        if estado_anterior.get(symbol) != señal and señal in ['BUY', 'SELL']:
            emoji = "🟢" if señal == "BUY" else "🔴"
            mensaje = f"{emoji} NUEVA SEÑAL para {symbol}:\n📍 {señal}\n🕒 {fecha}"
            enviar_mensaje_telegram(mensaje)

    guardar_estado_actual(estado_actual)

if __name__ == "__main__":
    main()
