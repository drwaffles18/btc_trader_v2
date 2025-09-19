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

def get_binance_4h_data(symbol: str, limit: int = 300) -> pd.DataFrame:
    """
    Descarga velas 4h desde MIRROR/.US solamente (histórico).
    Memoriza el host exitoso por símbolo y loguea los intentos.
    """
    limit = max(50, min(int(limit), 1000))
    bases = _bases_for(symbol)

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

            # Convierte a hora local CR para visualización
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
