# --- utils/binance_fetch.py ---
# Este módulo extrae velas 4H de Binance
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz

def get_binance_4h_data(symbol):
    url = 'https://api.binance.us/api/v3/klines'
    params = {
        'symbol': symbol,
        'interval': '4h',
        'limit': 1000
    }
    response = requests.get(url, params=params)
    data = response.json()
    df = pd.DataFrame(data, columns=[
        'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
        'Close time', 'Quote asset volume', 'Number of trades',
        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    )
    df['Open time'] = pd.to_datetime(df['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Costa_Rica')
    df[['Open', 'High', 'Low', 'Close']] = df[['Open', 'High', 'Low', 'Close']].astype(float)
    return df
