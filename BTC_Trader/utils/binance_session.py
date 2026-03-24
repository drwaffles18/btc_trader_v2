# utils/binance_session.py
import os
import time

try:
    from binance.client import Client
    _client_import_error = None
except Exception as e:
    Client = None
    _client_import_error = str(e)

_client = None
_client_key_fingerprint = None
_last_init_err = None


def _current_creds():
    key = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")
    return key, sec


def binance_enabled() -> bool:
    key, sec = _current_creds()
    return bool(key and sec and Client is not None)


def get_client():
    """
    Lazy singleton, pero sin cachear 'enabled' en falso para siempre.
    Revalida credenciales e import cada vez.
    """
    global _client, _client_key_fingerprint, _last_init_err

    key, sec = _current_creds()

    if Client is None:
        _last_init_err = f"Binance Client import failed: {_client_import_error}"
        return None

    if not key or not sec:
        _last_init_err = "Missing BINANCE_API_KEY / BINANCE_API_SECRET"
        return None

    # fingerprint simple para recrear client si cambian credenciales
    fp = f"{key[:6]}::{len(sec)}"

    if _client is not None and _client_key_fingerprint == fp:
        return _client

    try:
        _client = Client(key, sec)
        _client_key_fingerprint = fp
        _last_init_err = None
        return _client
    except Exception as e:
        _last_init_err = str(e)
        _client = None
        _client_key_fingerprint = None
        return None


def get_last_init_error():
    return _last_init_err


def looks_like_ban(err: Exception) -> bool:
    s = str(err)
    return ("code=-1003" in s) or ("IP banned" in s) or ("Way too much request weight" in s)


def sleep_on_ban(err: Exception):
    s = str(err)
    try:
        token = "until "
        if token in s:
            ms = int(s.split(token, 1)[1].split(".", 1)[0].strip())
            now_ms = int(time.time() * 1000)
            wait_ms = max(0, ms - now_ms)
            time.sleep(min(wait_ms / 1000.0, 30.0))
    except Exception:
        pass
