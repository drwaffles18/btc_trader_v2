# utils/binance_session.py
import os
import time

try:
    from binance.client import Client
except Exception:
    Client = None

_client = None
_enabled = None
_last_init_err = None

def binance_enabled() -> bool:
    global _enabled
    if _enabled is not None:
        return _enabled
    key = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")
    _enabled = bool(key and sec and Client is not None)
    return _enabled

def get_client():
    """
    Lazy singleton. Crea el client SOLO si:
      - hay keys
      - binance lib existe
    NO hace ping aquí para no gastar weight en arranque.
    """
    global _client, _last_init_err

    if not binance_enabled():
        return None

    if _client is not None:
        return _client

    key = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

    try:
        _client = Client(key, sec)
        return _client
    except Exception as e:
        _last_init_err = str(e)
        _client = None
        return None

def get_last_init_error():
    return _last_init_err

def looks_like_ban(err: Exception) -> bool:
    s = str(err)
    return ("code=-1003" in s) or ("IP banned" in s) or ("Way too much request weight" in s)

def sleep_on_ban(err: Exception):
    """
    Binance a veces te da el 'until <epoch_ms>'.
    Si lo encuentras, duérmete un poquito (opcional).
    """
    s = str(err)
    # parsing ultra simple
    try:
        # "... until 1772649357204."
        token = "until "
        if token in s:
            ms = int(s.split(token, 1)[1].split(".", 1)[0].strip())
            now_ms = int(time.time() * 1000)
            wait_ms = max(0, ms - now_ms)
            # cap: no dormir infinito en runtime, solo 30s aquí
            time.sleep(min(wait_ms / 1000.0, 30.0))
    except Exception:
        pass
