# scripts/repair_gaps.py

import pandas as pd
import pytz
import requests
from datetime import timedelta

CR = pytz.timezone("America/Costa_Rica")
BINANCE_BASE_URL = "https://api.binance.com"


def append_row(ws, df_new: pd.DataFrame):
    """
    Inserta una o varias filas al final del sheet.
    (Duplicada aquí para mantener modularidad.
     Si prefieres, puedes moverla a un utils común.)
    """
    next_row = len(ws.get_all_values()) + 1
    ws.update(
        values=df_new.astype(str).values.tolist(),
        range_name=f"A{next_row}"
    )


def _fetch_klines_5m_binance(symbol: str, start_open_utc: pd.Timestamp, end_open_utc: pd.Timestamp):
    """
    Descarga velas 5m desde Binance entre start_open_utc (incluida)
    y end_open_utc (excluida), usando la API pública /api/v3/klines.

    Trabaja con tiempos de APERTURA en UTC.
    """
    start_ms = int(start_open_utc.timestamp() * 1000)
    end_ms = int(end_open_utc.timestamp() * 1000)

    params = {
        "symbol": symbol,
        "interval": "5m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000
    }

    resp = requests.get(f"{BINANCE_BASE_URL}/api/v3/klines", params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    return data  # lista de klines


def repair_gaps(symbol: str, ws, last_open_utc: pd.Timestamp, k_open_utc: pd.Timestamp):
    """
    Repara candelas faltantes para un símbolo dado.

    Parámetros:
    - symbol:    "BTCUSDT", "ETHUSDT", etc.
    - ws:        worksheet de Google Sheets correspondiente al símbolo.
    - last_open_utc: open time (UTC) de la ÚLTIMA vela que ya tienes en el sheet.
    - k_open_utc:    open time (UTC) de la vela MÁS RECIENTE reportada por Binance
                     (la que update_incremental va a insertar al final).

    Este método:
      1) Calcula cuántas velas FALTAN entre last_open_utc y k_open_utc.
      2) Descarga esas velas intermedias desde Binance.
      3) Las inserta en el sheet en formato local (-06:00).
      4) NO inserta la vela de k_open_utc (esa la maneja update_incremental).
    """
    # ----------------------------------------------------
    # 1) Calcular cuántas velas faltan
    # ----------------------------------------------------
    delta_sec = (k_open_utc - last_open_utc).total_seconds()
    missing_candles = int(delta_sec // 300)  # 300 seg = 5 min

    # missing_candles == 1  => todo bien, solo viene la nueva vela
    # missing_candles > 1   => hay velas intermedias que faltan
    if missing_candles <= 1:
        print(f"   ✓ {symbol}: no hay gaps que reparar (missing_candles={missing_candles})")
        return

    # Queremos solo las intermedias: entre last_open_utc y k_open_utc
    # Ej: last_open=20:05, k_open=20:20  => missing_candles=3
    # Velas que faltan: 20:10 y 20:15  => son missing_candles - 1
    first_missing_open_utc = last_open_utc + pd.Timedelta(minutes=5)
    last_missing_open_utc = k_open_utc  # EXCLUIMOS k_open_utc en la descarga

    print(
        f"   ⚠️  {symbol}: faltan {missing_candles - 1} candelas "
        f"entre {first_missing_open_utc} y {last_missing_open_utc}"
    )

    # ----------------------------------------------------
    # 2) Descargar velas faltantes desde Binance
    # ----------------------------------------------------
    try:
        klines = _fetch_klines_5m_binance(
            symbol=symbol,
            start_open_utc=first_missing_open_utc,
            end_open_utc=last_missing_open_utc
        )
    except Exception as e:
        print(f"   ❌ Error al descargar klines faltantes desde Binance para {symbol}: {e}")
        return

    if not klines:
        print(f"   ⚠️  {symbol}: Binance no devolvió klines para el rango de gaps.")
        return

    # ----------------------------------------------------
    # 3) Convertir y preparar filas para insertar
    # ----------------------------------------------------
    rows = []

    for k in klines:
        # Estructura kline Binance:
        # [0] openTime, [1] open, [2] high, [3] low, [4] close,
        # [5] volume, [6] closeTime, ... (ignoramos el resto)

        open_ms = k[0]
        close_ms = k[6]

        open_utc = pd.to_datetime(open_ms, unit="ms", utc=True)
        close_utc_raw = pd.to_datetime(close_ms, unit="ms", utc=True)

        # Convertimos ambos a hora local Costa Rica
        open_local = open_utc.tz_convert(CR)
        close_local = close_utc_raw.tz_convert(CR)

        # Ajustamos close al XX:XX:59.999000
        close_local = close_local - pd.Timedelta(milliseconds=1)

        row = {
            "Open time":  open_local.isoformat(" "),
            "Open":       float(k[1]),
            "High":       float(k[2]),
            "Low":        float(k[3]),
            "Close":      float(k[4]),
            "Volume":     float(k[5]),
            "Close time": close_local.isoformat(" ")
        }
        rows.append(row)

    if not rows:
        print(f"   ⚠️  {symbol}: no se construyeron filas para los gaps.")
        return

    df_new = pd.DataFrame(rows)

    # ----------------------------------------------------
    # 4) Insertar al final del sheet
    # ----------------------------------------------------
    append_row(ws, df_new)

    print(f"   ✅ {symbol}: reparadas {len(rows)} candelas faltantes.")
