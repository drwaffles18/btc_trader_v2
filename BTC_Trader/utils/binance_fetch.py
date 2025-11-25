# utils/binance_fetch.py
# Fetch de velas 4h con mirror público y binance.us + utilidades para pedir la ÚLTIMA vela cerrada (UTC)

import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

US_HOST = (os.getenv("BINANCE_US_URL") or "https://api.binance.us").rstrip("/")
MIRROR  = (os.getenv("BINANCE_MIRROR_URL") or "https://data-api.binance.vision").rstrip("/")

# Símbolos que normalmente están en .US (tu config actual incluye XRPUSDT)
US_SYMBOLS   = {"BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"}   # sin BNB que está en Mirror
MIRROR_FIRST = {"BNBUSDT"}                                     # estos primero al MIRROR

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

# =========================
# Selección de bases (orden por símbolo) + preferencia aprendida
# =========================
def _bases_for(symbol: str):
    # Mantén tu orden por símbolo (sin globales)
    if symbol in MIRROR_FIRST:
        bases = [MIRROR, US_HOST]
    elif symbol in US_SYMBOLS:
        bases = [MIRROR, US_HOST]  # histórico primero; cambiamos a .US primero en la función de “última cerrada”
    else:
        bases = [MIRROR, US_HOST]

    # Reubica hint de host preferido si existe
    hint = _PREFERRED_BASE.get(symbol)
    if hint and hint in bases:
        bases = [hint] + [b for b in bases if b != hint]
    return bases

def bases_para(symbol: str):
    """
    Para la última vela cerrada en vivo, preferimos .US para symbols soportados
    y dejamos MIRROR como fallback. Mantiene también el hint preferido.
    """
    if symbol in US_SYMBOLS:
        bases = [US_HOST, MIRROR]
    else:
        bases = [MIRROR, US_HOST]
    hint = _PREFERRED_BASE.get(symbol)
    if hint and hint in bases:
        bases = [hint] + [b for b in bases if b != hint]
    return bases

# =========================
# Fetch histórico (tu función original)
# =========================
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

def get_binance_4h_data(symbol: str, limit: int = 300, preferred_base: str = None) -> pd.DataFrame:
    """
    Descarga velas 4h desde MIRROR/.US.
    Si preferred_base está presente y soportada, la prueba primero para alinear cortes de vela.
    """
    limit = max(50, min(int(limit), 1000))
    bases = _bases_for(symbol)

    # NUEVO: probar primero la base preferida (la misma con la que confirmaste la última cerrada)
    if preferred_base and preferred_base in bases:
        bases = [preferred_base] + [b for b in bases if b != preferred_base]

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

            # 1) Numéricos
            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            # 2) Tiempos (UTC)
            df["Open time UTC"]  = pd.to_datetime(df["Open time"],  unit="ms", utc=True)
            df["Close time UTC"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

            # 3) Tiempos en CR (visual)
            df["Open time"]  = df["Open time UTC"].dt.tz_convert("America/Costa_Rica")
            df["Close time"] = df["Close time UTC"].dt.tz_convert("America/Costa_Rica")

            # 4) Orden
            df = df.sort_values("Open time UTC").reset_index(drop=True)

            _PREFERRED_BASE[symbol] = base
            print(f"[binance_fetch] {symbol} ✓ usando base: {base}")
            return df

        except Exception as e:
            print(f"[binance_fetch] {symbol} ✗ fallo con {base}: {e}")
            last_exc = e
            continue

    raise last_exc or RuntimeError(f"No se pudo obtener klines para {symbol}")

def get_binance_5m_data(symbol: str, limit: int = 900, preferred_base: str = None) -> pd.DataFrame:
    """
    Descarga velas 5m desde MIRROR/.US.
    - limit≈900 → ~3 días de histórico (3d * 24h * 12 velas/h = 864)
    - preferred_base: mismo concepto que en 4h para alinear con la base usada en la "última cerrada".
    """
    # Limitamos por seguridad
    limit = max(100, min(int(limit), 1000))
    bases = _bases_for(symbol)

    # Probar primero la base preferida (la usada en la última cerrada)
    if preferred_base and preferred_base in bases:
        bases = [preferred_base] + [b for b in bases if b != preferred_base]

    last_exc = None
    print(f"[binance_fetch] {symbol} 5m → probando bases en orden: {bases}")
    for base in [b for b in bases if b]:
        try:
            data = _fetch_klines(base, symbol, "5m", limit)
            cols = [
                "Open time","Open","High","Low","Close","Volume",
                "Close time","Quote asset volume","Number of trades",
                "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
            ]
            df = pd.DataFrame(data, columns=cols)

            # 1) Numéricos
            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            # 2) Tiempos (UTC)
            df["Open time UTC"]  = pd.to_datetime(df["Open time"],  unit="ms", utc=True)
            df["Close time UTC"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

            # 3) Tiempos en CR (visual)
            df["Open time"]  = df["Open time UTC"].dt.tz_convert("America/Costa_Rica")
            df["Close time"] = df["Close time UTC"].dt.tz_convert("America/Costa_Rica")

            # 4) Orden por tiempo
            df = df.sort_values("Open time UTC").reset_index(drop=True)

            _PREFERRED_BASE[symbol] = base
            print(f"[binance_fetch] {symbol} 5m ✓ usando base: {base}")
            return df

        except Exception as e:
            print(f"[binance_fetch] {symbol} 5m ✗ fallo con {base}: {e}")
            last_exc = e
            continue

    raise last_exc or RuntimeError(f"No se pudo obtener klines 5m para {symbol}")


# =========================
# NUEVO: ajuste para 5 minutos!
# =========================


FIVE_MIN_MS = 5 * 60 * 1000  # 5 minutos en ms

def _floor_interval_utc(ts_ms: int, interval_ms: int) -> int:
    return (ts_ms // interval_ms) * interval_ms

def last_closed_window_5m(server_time_ms: int):
    """
    Última vela 5m CERRADA (UTC): [open_ms, close_ms)
    """
    last_close = _floor_interval_utc(server_time_ms, FIVE_MIN_MS)
    last_open  = last_close - FIVE_MIN_MS
    return last_open, last_close



# =========================
# NUEVO: pedir EXPLÍCITAMENTE la ÚLTIMA vela 4h CERRADA (UTC)
# =========================
FOUR_H_MS = 4 * 60 * 60 * 1000

def _floor_4h_utc(ts_ms: int) -> int:
    return (ts_ms // FOUR_H_MS) * FOUR_H_MS

def last_closed_window(server_time_ms: int):
    """
    Devuelve [open_ms, close_ms) de la ÚLTIMA vela 4H CERRADA en UTC,
    usando la hora del servidor de Binance.
    """
    last_close = _floor_4h_utc(server_time_ms)
    last_open  = last_close - FOUR_H_MS
    return last_open, last_close

def fetch_last_closed_kline(symbol: str, base_url: str, session=None):
    """
    Pide exactamente la última vela 4H CERRADA vía REST,
    apuntando con endTime=last_close-1 para evitar velas en formación.
    Devuelve: (kline, last_open_ms, last_close_ms, server_time_ms)
    """
    s = session or _session

    # 1) Hora del servidor (UTC)
    r = s.get(f"{base_url}/api/v3/time", headers=_HEADERS, timeout=5)
    r.raise_for_status()
    server_time_ms = r.json()["serverTime"]

    # 2) Ventana de la vela cerrada
    last_open, last_close = last_closed_window(server_time_ms)

    # 3) Kline cerrada apuntando a endTime=last_close-1 (inclusivo)
    params = dict(symbol=symbol, interval="4h", limit=1, endTime=last_close - 1)
    r = s.get(f"{base_url}/api/v3/klines", params=params, headers=_HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"[{symbol}] Sin datos de kline desde {base_url}")

    k = data[0]
    k_open = int(k[0])
    if k_open != last_open:
        raise RuntimeError(f"[{symbol}] {base_url} devolvió open={k_open}, esperado={last_open}")

    # hint de base preferida al tener éxito
    _PREFERRED_BASE[symbol] = base_url
    return k, last_open, last_close, server_time_ms

def fetch_last_closed_kline_5m(symbol: str, base_url: str, session=None):
    """
    Pide exactamente la última vela 5m CERRADA vía REST,
    apuntando con endTime=last_close-1 para evitar velas en formación.
    Devuelve: (kline, last_open_ms, last_close_ms, server_time_ms)
    """
    s = session or _session

    # 1) Hora del servidor (UTC)
    r = s.get(f"{base_url}/api/v3/time", headers=_HEADERS, timeout=5)
    r.raise_for_status()
    server_time_ms = r.json()["serverTime"]

    # 2) Ventana de la vela cerrada 5m
    last_open, last_close = last_closed_window_5m(server_time_ms)

    # 3) Kline cerrada apuntando a endTime=last_close-1 (inclusivo)
    params = dict(symbol=symbol, interval="5m", limit=1, endTime=last_close - 1)
    r = s.get(f"{base_url}/api/v3/klines", params=params, headers=_HEADERS, timeout=10)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise RuntimeError(f"[{symbol}] Sin datos de kline 5m desde {base_url}")

    k = data[0]
    k_open = int(k[0])
    if k_open != last_open:
        raise RuntimeError(f"[{symbol}] {base_url} 5m devolvió open={k_open}, esperado={last_open}")

    # hint de base preferida
    _PREFERRED_BASE[symbol] = base_url
    return k, last_open, last_close, server_time_ms

# =============================================================
# NUEVO: Descarga histórico completo 5m entre fechas
# =============================================================
import time
from datetime import datetime, timezone

def get_binance_5m_data_between(symbol: str, start_dt: str, end_dt: str = None):
    """
    Descarga TODO el histórico 5m entre start_dt y end_dt usando paginación automática.

    Ejemplo:
        df = get_binance_5m_data_between("BTCUSDT", "2024-12-01 00:00:00")

    Parámetros:
        symbol   : símbolo de Binance ej. "BTCUSDT"
        start_dt : fecha inicial en formato "YYYY-MM-DD HH:MM:SS"
        end_dt   : fecha final. Si es None → usa hora actual del servidor Binance

    Retorna:
        pandas.DataFrame con TODAS las velas entre start_dt y end_dt
    """

    # 1) Convertir fechas a milisegundos UTC
    start_ms = int(pd.Timestamp(start_dt, tz="UTC").timestamp() * 1000)

    # 2) Si no se da end_dt, pedir hora de servidor Binance
    if end_dt is None:
        r = _session.get(f"{US_HOST}/api/v3/time", timeout=5, headers=_HEADERS)
        r.raise_for_status()
        server_time = r.json()["serverTime"]
        end_ms = int(server_time)
    else:
        end_ms = int(pd.Timestamp(end_dt, tz="UTC").timestamp() * 1000)

    print(f"[binance_fetch] HISTÓRICO {symbol} 5m → desde {start_dt} hasta {pd.to_datetime(end_ms, unit='ms')}")
    print(f"[binance_fetch] bases: {_bases_for(symbol)}")

    frames = []
    fetch_size = 1000
    current_start = start_ms

    bases = _bases_for(symbol)

    while current_start < end_ms:

        current_end = min(current_start + fetch_size * FIVE_MIN_MS, end_ms)

        # Intento de fetch con fallback bases
        last_exc = None
        data = None

        for base in bases:
            try:
                url = f"{base}/api/v3/klines"
                params = {
                    "symbol": symbol,
                    "interval": "5m",
                    "startTime": current_start,
                    "endTime": current_end - 1,
                    "limit": 1000
                }

                resp = _session.get(url, params=params, timeout=10, headers=_HEADERS)

                # Saltar bloqueos
                if resp.status_code in (451, 403):
                    raise requests.HTTPError(f"{resp.status_code} from {base}", response=resp)

                resp.raise_for_status()
                data = resp.json()

                if isinstance(data, list):
                    _PREFERRED_BASE[symbol] = base
                    break  # éxito

                raise ValueError(f"Formato inesperado: {data}")

            except Exception as e:
                print(f"[binance_fetch] {symbol} 5m ✗ fallo con {base}: {e}")
                last_exc = e
                continue

        if data is None:
            raise last_exc

        if len(data) == 0:
            # No más velas en este rango → terminar
            break

        # Convertir a DataFrame
        cols = [
            "Open time","Open","High","Low","Close","Volume",
            "Close time","Quote asset volume","Number of trades",
            "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
        ]
        df = pd.DataFrame(data, columns=cols)

        # 1) Numéricos
        for c in ["Open","High","Low","Close","Volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # 2) Tiempos UTC
        df["Open time UTC"]  = pd.to_datetime(df["Open time"], unit="ms", utc=True)
        df["Close time UTC"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

        # 3) Tiempos CR
        df["Open time"]  = df["Open time UTC"].dt.tz_convert("America/Costa_Rica")
        df["Close time"] = df["Close time UTC"].dt.tz_convert("America/Costa_Rica")

        frames.append(df)

        # Avanzar puntero
        last_close_ms = int(df["Close time UTC"].iloc[-1].timestamp() * 1000)
        current_start = last_close_ms + FIVE_MIN_MS

        # Pausa ligera para no golpear el API
        time.sleep(0.1)

    if not frames:
        raise RuntimeError(f"No se obtuvo historial 5m para {symbol}")

    final_df = pd.concat(frames, ignore_index=True)
    final_df = final_df.sort_values("Open time UTC").reset_index(drop=True)

    print(f"[binance_fetch] ✓ obtenido histórico completo: {len(final_df)} velas.")
    return final_df







