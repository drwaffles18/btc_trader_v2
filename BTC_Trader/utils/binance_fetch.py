# --- utils/binance_fetch.py ---
# Este módulo extrae velas 4H de Binance
import requests
import pandas as pd

def get_binance_4h_data(symbol):
    # Usar el endpoint global (no .us) para disponer de todos los pares
    url = 'https://api.binance.com/api/v3/klines'
    params = {
        'symbol': symbol,
        'interval': '4h',
        'limit': 1000
    }
    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()

    cols = [
        'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close time', 'Quote asset volume', 'Number of trades',
        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
    ]
    df = pd.DataFrame(data, columns=cols)

    # Tiempos como tz-aware (Costa Rica) para que comparaciones funcionen bien
    df['Open time'] = pd.to_datetime(df['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Costa_Rica')

    # Numéricos
    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    return df
