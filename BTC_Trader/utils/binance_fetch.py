# utils/binance_fetch.py
# Fetch de velas 4h SOLO con mirror público y binance.us (sin globales) + logging

import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

US_HOST = (os.getenv("BINANCE_US_URL") or "https://api.binance.us").rstrip("/")
MIRROR  = (os.getenv("BINANCE_MIRROR_URL") or "https://data-api.binance.vision").rstrip("/")

# Símbolos que normalmente están en .US (ajusta si quieres)
US_SYMBOLS   = {"BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"}   # sin BNB que esta en Mirror
MIRROR_FIRST = {"BNBUSDT"}               # estos primero al MIRROR

_HEADERS = {"User-Agent": "VictorTradingApp/1.0 (+railway)"}

# Reintentos ligeros para 429/5xx
_session = requests.Session()
_retry = Retry(
    total=2,
    backoff_factor=0.4,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET"]),
)
_adapter = HTTPAdapter(max_retries=_retry)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

# Cache: host que funcionó por símbolo
_PREFERRED_BASE = {}

def _bases_for(symbol: str):
    # orden por símbolo (sin globales)
    if symbol in MIRROR_FIRST:
        return [MIRROR, US_HOST]
    elif symbol in US_SYMBOLS:
        return [MIRROR, US_HOST]
    else:
        return [MIRROR, US_HOST]

def _fetch_klines(base: str, symbol: str, interval: str, limit: int, timeout: int = 12):
    url = f"{base}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = _session.get(url, params=params, timeout=timeout, headers=_HEADERS)
    # 451/403 = bloqueos → saltar a siguiente base
    if r.status_code in (451, 403):
        raise requests.HTTPError(f"{r.status_code} from {base}", response=r)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise ValueError(f"Formato inesperado desde {base}: {data}")
    return data

def get_binance_4h_data(symbol: str, limit: int = 300) -> pd.DataFrame:
    """
    Descarga velas 4h desde MIRROR/.US solamente.
    Memoriza el host exitoso por símbolo y loguea los intentos.
    """
    limit = max(50, min(int(limit), 1000))
    bases = _bases_for(symbol)

    # hint de host preferido
    hint = _PREFERRED_BASE.get(symbol)
    if hint and hint in bases:
        bases = [hint] + [b for b in bases if b != hint]

    last_exc = None
    print(f"[binance_fetch] {symbol} → probando bases en orden: {bases}")
    for base in [b for b in bases if b]:
        try:
            data = _fetch_klines(base, symbol, "4h", limit)
            cols = [
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ]
            df = pd.DataFrame(data, columns=cols)
            
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms", utc=True) \
                     .dt.tz_convert("America/Costa_Rica")

            
            df = df.sort_values("Open time").reset_index(drop=True)
            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            _PREFERRED_BASE[symbol] = base
            print(f"[binance_fetch] {symbol} ✓ usando base: {base}")
            return df

        except Exception as e:
            print(f"[binance_fetch] {symbol} ✗ fallo con {base}: {e}")
            last_exc = e
            continue

    # falló todo
    raise last_exc or RuntimeError(f"No se pudo obtener klines para {symbol}")

