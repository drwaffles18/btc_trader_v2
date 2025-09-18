# utils/binance_fetch.py
import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Usa ENV para fijar host y evitar fallbacks lentos.
# En Railway: BINANCE_BASE_URL=https://api1.binance.com
BASE = (os.getenv("BINANCE_BASE_URL") or "https://api1.binance.com").rstrip("/")

# Sesión con reintentos ligeros para 429/5xx (rápido y seguro)
_session = requests.Session()
_retry = Retry(
    total=2,                # 2 reintentos máximo
    backoff_factor=0.5,     # 0.5s, 1s
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"])
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

_HEADERS = {"User-Agent": "VictorTradingApp/1.0 (+railway)"}

def get_binance_4h_data(symbol: str, limit: int = 300) -> pd.DataFrame:
    """
    Descarga velas 4h para 'symbol' desde BASE. Devuelve DataFrame con columnas:
    ['Open time','Open','High','Low','Close','Volume', ...]
    """
    # Clamp por seguridad (Binance permite hasta 1000)
    limit = max(10, min(int(limit), 1000))

    url = f"{BASE}/api/v3/klines"
    params = {"symbol": symbol, "interval": "4h", "limit": limit}

    resp = _session.get(url, params=params, timeout=12, headers=_HEADERS)
    resp.raise_for_status()
    data = resp.json()

    cols = [
        "Open time","Open","High","Low","Close","Volume",
        "Close time","Quote asset volume","Number of trades",
        "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
    ]
    df = pd.DataFrame(data, columns=cols)

    # Tiempos tz-aware (CR) + orden
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")\
                          .tz_localize("UTC").tz_convert("America/Costa_Rica")
    df = df.sort_values("Open time").reset_index(drop=True)

    # Numéricos
    for c in ["Open","High","Low","Close","Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df
