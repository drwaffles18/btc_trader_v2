# utils/binance_fetch.py
# Fetch de velas 4h con routing por símbolo + fallback controlado y cache de host exitoso

import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Hosts configurables por ENV (puedes cambiarlos en Railway sin tocar código)
US_HOST   = (os.getenv("BINANCE_US_URL") or "https://api.binance.us").rstrip("/")
MIRROR    = (os.getenv("BINANCE_MIRROR_URL") or "https://data-api.binance.vision").rstrip("/")
G1        = (os.getenv("BINANCE_BASE_URL") or "https://api1.binance.com").rstrip("/")
G2        = "https://api2.binance.com"
G3        = "https://api3.binance.com"

# OJO: XRP/BNB típicamente no están en .us → mandarlos a mirror/global
US_SYMBOLS   = {"BTCUSDT", "ETHUSDT", "ADAUSDT"}  # NO XRP, NO BNB
MIRROR_FIRST = {"BNBUSDT", "XRPUSDT"}             # estos intentan mirror sí o sí

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

# Cache en memoria: host que funcionó por símbolo
_PREFERRED_BASE = {}

def _fetch_klines(base: str, symbol: str, interval: str, limit: int, timeout: int = 12):
    url = f"{base}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = _session.get(url, params=params, timeout=timeout, headers=_HEADERS)
    # 451/403 = geobloqueo/legal; no reintentar en el mismo host
    if r.status_code in (451, 403):
        raise requests.HTTPError(f"{r.status_code} from {base}", response=r)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        # Respuesta de error JSON (dict) → no sirve para DF
        raise ValueError(f"Formato inesperado desde {base}: {data}")
    return data

def _bases_for(symbol: str):
    # Siempre probar MIRROR primero: evita 451 y suele ser rápido
    if symbol in MIRROR_FIRST:
        return [MIRROR, US_HOST, G1, G2, G3]
    elif symbol in US_SYMBOLS:
        return [MIRROR, US_HOST, G1, G2, G3]
    else:
        return [MIRROR, US_HOST, G1, G2, G3]

def get_binance_4h_data(symbol: str, limit: int = 300) -> pd.DataFrame:
    """
    Descarga velas 4h para 'symbol'. Intenta en orden de bases adecuado y
    memoriza el host que funcionó para ese símbolo (durante el ciclo de vida del proceso).
    """
    limit = max(50, min(int(limit), 1000))  # clamp

    # Si ya sabemos qué host funciona para este símbolo, úsalo directo
    base_hint = _PREFERRED_BASE.get(symbol)
    bases = [base_hint] + _bases_for(symbol) if base_hint else _bases_for(symbol)

    last_exc = None
    for base in [b for b in bases if b]:
        try:
            data = _fetch_klines(base, symbol, "4h", limit)
            cols = [
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ]
            df = pd.DataFrame(data, columns=cols)
            # Tiempos tz-aware a Costa Rica
            df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")\
                                 .tz_localize("UTC").tz_convert("America/Costa_Rica")
            df = df.sort_values("Open time").reset_index(drop=True)
            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            # Memoriza el host exitoso para próximas llamadas
            _PREFERRED_BASE[symbol] = base
            return df

        except Exception as e:
            last_exc = e
            continue

    raise last_exc or RuntimeError(f"No se pudo obtener klines para {symbol}")
