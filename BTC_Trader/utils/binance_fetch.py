import os
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import pytz

# ============================================================
# CONFIGURACIÓN GLOBAL
# ============================================================

US_HOST = (os.getenv("BINANCE_US_URL") or "https://api.binance.us").rstrip("/")
MIRROR  = (os.getenv("BINANCE_MIRROR_URL") or "https://data-api.binance.vision").rstrip("/")
API_BINANCE = "https://api.binance.com"

US_SYMBOLS   = {"BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"}
MIRROR_FIRST = {"BNBUSDT"}

_HEADERS = {"User-Agent": "VictorTradingApp/1.0 (+railway)"}

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

_PREFERRED_BASE = {}



# ============================================================
# SELECTORES DE BASES (4H y “última vela cerrada”)
# ============================================================

def _bases_for(symbol: str):
    """Bases solo para histórico 4H y fallback general."""
    if symbol in MIRROR_FIRST:
        bases = [MIRROR, US_HOST]
    elif symbol in US_SYMBOLS:
        bases = [MIRROR, US_HOST]
    else:
        bases = [MIRROR, US_HOST]

    hint = _PREFERRED_BASE.get(symbol)
    if hint and hint in bases:
        bases = [hint] + [b for b in bases if b != hint]

    return bases


def bases_para(symbol: str):
    """
    Para la última vela CERRADA (5m / 4h)
    Funcionan bien US_HOST y MIRROR.
    """
    if symbol in US_SYMBOLS:
        bases = [US_HOST, MIRROR]
    else:
        bases = [MIRROR, US_HOST]

    hint = _PREFERRED_BASE.get(symbol)
    if hint and hint in bases:
        bases = [hint] + [b for b in bases if b != hint]

    return bases



# ============================================================
# FETCH HISTÓRICO SIMPLE (4H / 5M — pero 5M NO SE USA AQUÍ)
# ============================================================

def _fetch_klines(base: str, symbol: str, interval: str, limit: int, timeout: int = 12):
    url = f"{base}/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = _session.get(url, params=params, timeout=timeout, headers=_HEADERS)

    if r.status_code in (451, 403):
        raise requests.HTTPError(f"{r.status_code} from {base}", response=r)

    r.raise_for_status()
    data = r.json()

    if not isinstance(data, list):
        raise ValueError(f"Formato inesperado desde {base}: {data}")

    return data



# ============================================================
# HISTÓRICO 4H (permanece igual)
# ============================================================

def get_binance_4h_data(symbol: str, limit: int = 300, preferred_base: str = None) -> pd.DataFrame:
    limit = max(50, min(int(limit), 1000))
    bases = _bases_for(symbol)

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

            for c in ["Open","High","Low","Close","Volume"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

            df["Open time UTC"]  = pd.to_datetime(df["Open time"],  unit="ms", utc=True)
            df["Close time UTC"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

            df["Open time"]  = df["Open time UTC"].dt.tz_convert("America/Costa_Rica")
            df["Close time"] = df["Close time UTC"].dt.tz_convert("America/Costa_Rica")

            df = df.sort_values("Open time UTC").reset_index(drop=True)

            _PREFERRED_BASE[symbol] = base
            print(f"[binance_fetch] {symbol} ✓ usando base: {base}")
            return df

        except Exception as e:
            print(f"[binance_fetch] {symbol} ✗ fallo con {base}: {e}")
            last_exc = e

    raise last_exc or RuntimeError(f"No se pudo obtener klines para {symbol}")



# ============================================================
# ÚLTIMA VELA CERRADA (5m)
# ============================================================

FIVE_MIN_MS = 5 * 60 * 1000

def _floor_interval_utc(ts_ms: int, interval_ms: int) -> int:
    return (ts_ms // interval_ms) * interval_ms

def last_closed_window_5m(server_time_ms: int):
    last_close = _floor_interval_utc(server_time_ms, FIVE_MIN_MS)
    last_open  = last_close - FIVE_MIN_MS
    return last_open, last_close


def fetch_last_closed_kline_5m(symbol: str, base_url: str, session=None):
    s = session or _session

    r = s.get(f"{base_url}/api/v3/time", headers=_HEADERS, timeout=5)
    r.raise_for_status()
    server_time_ms = r.json()["serverTime"]

    last_open, last_close = last_closed_window_5m(server_time_ms)

    params = dict(symbol=symbol, interval="5m", limit=1, endTime=last_close - 1)
    r = s.get(f"{base_url}/api/v3/klines", params=params, headers=_HEADERS, timeout=10)
    r.raise_for_status()

    data = r.json()
    if not data:
        raise RuntimeError(f"[{symbol}] Sin datos de kline 5m desde {base_url}")

    k = data[0]
    k_open = int(k[0])

    if k_open != last_open:
        raise RuntimeError(f"[{symbol}] {base_url} devolvió open={k_open}, esperado={last_open}")

    _PREFERRED_BASE[symbol] = base_url
    return k, last_open, last_close, server_time_ms



# ============================================================
# *** HISTÓRICO 5M ENTRE FECHAS (VERSIÓN ESTABLE) ***
# ============================================================

def get_binance_5m_data_between(symbol: str, start_dt: str, end_dt: str = None, preferred_base=None):
    """
    Descarga histórico EXACTO 5m entre start_dt y end_dt.
    SOLO DESDE Binance Vision (SIN GEO RESTRICCIONES).
    """

    # === 1) convertir fechas ===
    start_ms = int(pd.Timestamp(start_dt, tz="UTC").timestamp() * 1000)

    # === 2) Determinar end_ms ===
    if end_dt is None:
        # USAR SIEMPRE BINANCE VISION (NO tiene bloqueos)
        base_for_time = MIRROR  
        r = _session.get(f"{base_for_time}/api/v3/time", timeout=5, headers=_HEADERS)
        r.raise_for_status()
        end_ms = int(r.json()["serverTime"])
    else:
        end_ms = int(pd.Timestamp(end_dt, tz="UTC").timestamp() * 1000)

    print(f"[binance_fetch] HISTÓRICO {symbol} 5m → desde {start_dt} hasta {pd.to_datetime(end_ms, unit='ms')}")
    print(f"[binance_fetch] base: [{MIRROR}] (forzado)")

    # ========== USAR SOLO BINANCE VISION ==========
    bases = [MIRROR]

    frames = []
    fetch_size = 1000
    current_start = start_ms

    while current_start < end_ms:

        current_end = min(current_start + fetch_size * FIVE_MIN_MS, end_ms)
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
                    "limit": 1000,
                }

                resp = _session.get(url, params=params, timeout=10, headers=_HEADERS)
                resp.raise_for_status()
                data = resp.json()
                break

            except Exception as e:
                print(f"[binance_fetch] {symbol} 5m ✗ fallo con {base}: {e}")
                last_exc = e

        if data is None:
            raise last_exc

        if len(data) == 0:
            break

        cols = [
            "Open time","Open","High","Low","Close","Volume",
            "Close time","Quote asset volume","Number of trades",
            "Taker buy base asset volume","Taker buy quote asset volume","Ignore"
        ]
        df = pd.DataFrame(data, columns=cols)

        for c in ["Open","High","Low","Close","Volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        df["Open time UTC"]  = pd.to_datetime(df["Open time"],  unit="ms", utc=True)
        df["Close time UTC"] = pd.to_datetime(df["Close time"], unit="ms", utc=True)

        df["Open time"]  = df["Open time UTC"].dt.tz_convert("America/Costa_Rica")
        df["Close time"] = df["Close time UTC"].dt.tz_convert("America/Costa_Rica")

        frames.append(df)

        last_close_ms = int(df["Close time UTC"].iloc[-1].timestamp() * 1000)
        current_start = last_close_ms + FIVE_MIN_MS

        time.sleep(0.08)

    if not frames:
        raise RuntimeError(f"No se obtuvo historial 5m para {symbol}")

    final_df = pd.concat(frames, ignore_index=True)
    final_df = final_df.sort_values("Open time UTC").reset_index(drop=True)

    print(f"[binance_fetch] ✓ obtenido histórico consistente: {len(final_df)} velas.")
    return final_df


