# utils/binance_fetch.py
import os
import time
import requests
import pandas as pd

BINANCE_HOSTS = [
    # respetar override por ENV si lo configuras en Railway
    os.getenv("BINANCE_BASE_URL"),
    "https://api.binance.com",
    # alternos (a veces evaden geobloqueo/reglas cloud)
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    # mirror de datos (históricos/market data público)
    "https://data-api.binance.vision",
]

HEADERS = {
    "User-Agent": "VictorTradingApp/1.0 (+https://railway.app)"
}

def _get_klines_from(host, symbol, interval="4h", limit=1000, timeout=20):
    base = host.rstrip("/")
    url = f"{base}/api/v3/klines"
    # data-api.binance.vision reusa la misma ruta /api/v3/klines
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    resp = requests.get(url, params=params, timeout=timeout, headers=HEADERS)
    # 451 = geoblock/legal. Levanta para que el caller pruebe el siguiente host.
    if resp.status_code == 451:
        raise requests.HTTPError("451 Unavailable for legal reasons", response=resp)
    resp.raise_for_status()
    return resp.json()

def get_binance_4h_data(symbol):
    last_exc = None
    for host in [h for h in BINANCE_HOSTS if h]:
        try:
            data = _get_klines_from(host, symbol, interval="4h", limit=1000)
            cols = [
                'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                'Close time', 'Quote asset volume', 'Number of trades',
                'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
            ]
            df = pd.DataFrame(data, columns=cols)
            # Tiempos tz-aware (Costa Rica)
            df['Open time'] = pd.to_datetime(df['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Costa_Rica')
            for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
                df[c] = pd.to_numeric(df[c], errors='coerce')
            return df
        except requests.HTTPError as e:
            last_exc = e
            # si es 451 o 403, intenta el siguiente host
            if e.response is not None and e.response.status_code in (451, 403):
                time.sleep(0.3)
                continue
            # si es otro tipo (429, 5xx), reintenta rápido con el mismo host 1 vez
            if e.response is not None and e.response.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.0)
                try:
                    data = _get_klines_from(host, symbol, interval="4h", limit=1000)
                    cols = [
                        'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
                        'Close time', 'Quote asset volume', 'Number of trades',
                        'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'
                    ]
                    df = pd.DataFrame(data, columns=cols)
                    df['Open time'] = pd.to_datetime(df['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('America/Costa_Rica')
                    for c in ['Open', 'High', 'Low', 'Close', 'Volume']:
                        df[c] = pd.to_numeric(df[c], errors='coerce')
                    return df
                except Exception as e2:
                    last_exc = e2
                    continue
        except Exception as e:
            last_exc = e
            time.sleep(0.2)
            continue
    # Si ninguno funcionó, vuelve a fallar con el último error
    raise last_exc if last_exc else RuntimeError("No se pudo obtener klines de Binance")
