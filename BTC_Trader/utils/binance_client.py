# utils/binance_client.py
import os
import time
from functools import lru_cache

try:
    from binance.client import Client
except Exception:
    Client = None

def _get_keys():
    key = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")
    return key, sec

@lru_cache(maxsize=1)
def get_client():
    """
    Singleton lazy:
    - NO ping aquí (ping es otra request)
    - Solo crea client una vez por proceso
    """
    key, sec = _get_keys()
    if not key or not sec or Client is None:
        return None
    return Client(key, sec)

# --- cache simple para symbol filters / exchange info ---
_EXCHANGE_INFO = {"ts": 0, "data": None}
_EXCHANGE_TTL_SEC = 6 * 3600  # 6h

def get_exchange_info_cached(client):
    now = time.time()
    if _EXCHANGE_INFO["data"] is not None and (now - _EXCHANGE_INFO["ts"] < _EXCHANGE_TTL_SEC):
        return _EXCHANGE_INFO["data"]
    info = client.get_exchange_info()   # 1 request, pero la haces raro (cada horas, no cada trade)
    _EXCHANGE_INFO["ts"] = now
    _EXCHANGE_INFO["data"] = info
    return info

def get_symbol_filters_cached(client, symbol: str):
    """
    Deriva stepSize/minNotional desde exchangeInfo cacheado.
    Evita get_symbol_info() repetitivo (caro por frecuencia).
    """
    info = get_exchange_info_cached(client)
    sym = symbol.upper()
    for s in info.get("symbols", []):
        if s.get("symbol") == sym:
            filters = {f["filterType"]: f for f in s.get("filters", [])}
            lot = filters.get("LOT_SIZE", {}) or {}
            mn  = filters.get("MIN_NOTIONAL", {}) or {}
            return {
                "step": float(lot.get("stepSize", 0) or 0),
                "min_notional": float(mn.get("minNotional", 0) or 0),
            }
    return {"step": 0.0, "min_notional": 0.0}
