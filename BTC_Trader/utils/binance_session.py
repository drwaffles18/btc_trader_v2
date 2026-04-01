# utils/binance_session.py
import os
import time
from typing import Optional

try:
    from binance.client import Client
    _client_import_error = None
except Exception as e:
    Client = None
    _client_import_error = str(e)

_client = None
_client_key_fingerprint = None
_last_init_err = None
_banned_until_ms = 0


def _now_ms() -> int:
    return int(time.time() * 1000)


def _current_creds():
    key = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
    sec = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")
    return key, sec


def _looks_like_ban_message(msg: Optional[str]) -> bool:
    s = str(msg or "")
    s_low = s.lower()
    return (
        "code=-1003" in s
        or "ip banned" in s_low
        or "way too much request weight" in s_low
        or "banned until" in s_low
    )


def _extract_ban_until_ms(msg: Optional[str]) -> Optional[int]:
    s = str(msg or "")
    if "banned until" not in s.lower():
        return None

    try:
        # toma todos los dígitos después de "banned until"
        part = s.lower().split("banned until", 1)[1]
        digits = "".join(ch for ch in part if ch.isdigit())
        if digits:
            return int(digits)
    except Exception:
        pass

    return None


def _mark_ban_from_message(msg: Optional[str]) -> None:
    global _banned_until_ms

    if not _looks_like_ban_message(msg):
        return

    until_ms = _extract_ban_until_ms(msg)
    if until_ms is not None:
        _banned_until_ms = max(_banned_until_ms, until_ms)
    else:
        # fallback conservador 10 min
        _banned_until_ms = max(_banned_until_ms, _now_ms() + 10 * 60 * 1000)


def binance_enabled() -> bool:
    key, sec = _current_creds()
    return bool(key and sec and Client is not None)


def ban_active() -> bool:
    return _now_ms() < _banned_until_ms


def get_banned_until_ms() -> int:
    return _banned_until_ms


def get_retry_after_sec() -> int:
    return max(0, int((_banned_until_ms - _now_ms()) / 1000))


def get_client():
    """
    Lazy singleton:
    - Revalida credenciales cada vez
    - Si hay ban activo, no intenta recrear client
    - Si la inicialización falla por -1003, marca ban y deja error claro
    """
    global _client, _client_key_fingerprint, _last_init_err

    if ban_active():
        _last_init_err = (
            f"Binance ban active | retry_after_sec={get_retry_after_sec()} "
            f"| until_ms={_banned_until_ms}"
        )
        return None

    key, sec = _current_creds()

    if Client is None:
        _last_init_err = f"Binance Client import failed: {_client_import_error}"
        return None

    if not key or not sec:
        _last_init_err = "Missing BINANCE_API_KEY / BINANCE_API_SECRET"
        return None

    fp = f"{key[:6]}::{len(sec)}"

    if _client is not None and _client_key_fingerprint == fp:
        return _client

    try:
        _client = Client(key, sec)
        _client_key_fingerprint = fp
        _last_init_err = None
        return _client

    except Exception as e:
        msg = str(e)
        _last_init_err = msg
        _mark_ban_from_message(msg)
        _client = None
        _client_key_fingerprint = None
        return None


def get_last_init_error():
    return _last_init_err


def looks_like_ban(err: Exception) -> bool:
    return _looks_like_ban_message(str(err))


def sleep_on_ban(err: Exception):
    msg = str(err)
    _mark_ban_from_message(msg)

    if ban_active():
        wait_sec = min(get_retry_after_sec(), 30)
        if wait_sec > 0:
            time.sleep(wait_sec)
