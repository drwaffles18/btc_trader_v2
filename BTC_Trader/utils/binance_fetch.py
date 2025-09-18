# utils/binance_fetch.py
# Uso: las 4 monedas originales por binance.us; BNB por mirror (y fallback global)
import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

US_HOST   = (os.getenv("BINANCE_US_URL") or "https://api.binance.us").rstrip("/")
MIRROR    = (os.getenv("BINANCE_MIRROR_URL") or "https://data-api.binance.vision").rstrip("/")
GLOBAL_1  = (os.getenv("BINANCE_BASE_URL") or "https://api1.binance.com").rstrip("/")
GLOBAL_2  = "https://api2.binance.com"
GLOBAL_3  = "https://api3.binance.com"

# Tus 4 originales por el host viejo (como pediste)
US_SYMBOLS = {"BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"}
# BNB lo servimos por el mirror primero
BNB_SYMBOLS = {"BNBUSDT"}

_HEADERS = {"User-Agent": "VictorTradingApp/1.0 (+railway)"}

# Sesión con reintentos ligeros (para 429/5xx)
_session = requests.Session()
_retry = Retry(
    total=2,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

def _fetch_klines(base: str, symbol: str, interval: str, limit: int, timeout: int = 12):
    url = f"{base}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = _session.get(url, params=params, timeout=timeout, headers=_HEADERS)
    # 451/403: bloque legal/geo → probamos siguiente base sin romper
    if r.status_code in (451, 403):
        raise requests.HTTPError(f"{r.status_code} from {base}", response=r)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        # La API de error devuelve dict; no sirve para DataFrame de klines
        raise ValueError(f"Formato inesperado desde {base}: {data}")
    return data

def get_binance_4h_data(symbol: str, limit: int = 300) -> pd.DataFrame:
    """
    Para BTC/ETH/ADA/XRP usa binance.us (como el código viejo).
    Para BNB usa el mirror primero y luego globales (evita 451).
    """
    limit = max(50, min(int(limit), 1000))  # clamp

    # Orden de prueba por símbolo (lo que pediste)
    if symbol in US_SYMBOLS:
        bases = [US_HOST, MIRROR, GLOBAL_1, GLOBAL_2, GLOBAL_3]
    elif symbol in BNB_SYMBOLS:
        bases = [MIRROR, GLOBAL_1, GLOBAL_2, GLOBAL_3, US_HOST]  # mirror primero
    else:
        bases = [MIRROR, US_HOST, GLOBAL_1, GLOBAL_2, GLOBAL_3]

    last_exc = None
    for base in bases:
        try:
            data = _fetch_klines(base, symbol, "4h", limit)
            cols = [
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ]
            df = pd.DataFrame(data, columns=cols)
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")\
                                 .tz_localize("UTC").tz_convert("America/Costa_Rica")
            df = df.sort_values("Open time").reset_index(drop=True)
            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            return df
        except Exception as e:
            # si falla (451/403/429/5xx o formato), intentamos siguiente base
            last_exc = e
            continue

    # Si ninguna base funcionó, devolvemos el último error para debug
    raise last_exc or RuntimeError(f"No se pudo obtener klines para {symbol}")
