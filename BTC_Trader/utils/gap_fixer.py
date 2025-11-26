# utils/gap_fixer.py
import pandas as pd
import pytz
from utils.binance_fetch import fetch_last_closed_kline_5m


CR = pytz.timezone("America/Costa_Rica")

def fix_gaps(df, symbol, base):
    """
    Verifica e inserta velas faltantes ENTRE la última vela del sheet
    y la última vela real cerrada en Binance.
    """
    if df.empty:
        return pd.DataFrame()

    # Última vela en el sheet → local → UTC
    last_sheet_close_local = pd.to_datetime(df["Close time"].max())
    if last_sheet_close_local.tzinfo is None:
        last_sheet_close_utc = last_sheet_close_local.tz_localize(CR).tz_convert("UTC")
    else:
        last_sheet_close_utc = last_sheet_close_local.tz_convert("UTC")

    # Última vela real cerrada en Binance
    _, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
    binance_last_close_utc = pd.to_datetime(close_ms - 1, unit="ms", utc=True)

    expected_next_utc = last_sheet_close_utc + pd.Timedelta(minutes=5)

    # si expected >= binance_last_close → no hay gaps
    if expected_next_utc >= binance_last_close_utc:
        print(f"   ✓ {symbol}: no hay gaps reales.")
        return pd.DataFrame()

    print(f"   ⚠️ {symbol}: detectamos gaps REALES entre {expected_next_utc} y {binance_last_close_utc}")

    # Descargar solo velas que EXISTEN (no futuras)
    from utils.binance_fetch import get_range_klines_5m
    klines = get_range_klines_5m(
        symbol,
        start_utc=expected_next_utc,
        end_utc=binance_last_close_utc
    )

    if klines.empty:
        print(f"   ⚠️ {symbol}: Binance devolvió 0 velas → probablemente un error temporal.")
        return pd.DataFrame()

    # Convertir al formato del sheet
    rows = []
    for _, row in klines.iterrows():
        open_utc = row["Open time UTC"]
        close_utc = row["Close time UTC"]

        open_local = open_utc.tz_convert(CR)
        close_local = close_utc.tz_convert(CR) - pd.Timedelta(milliseconds=1)

        rows.append({
            "Open time":  open_local.isoformat(" "),
            "Open":       row["Open"],
            "High":       row["High"],
            "Low":        row["Low"],
            "Close":      row["Close"],
            "Volume":     row["Volume"],
            "Close time": close_local.isoformat(" ")
        })

    return pd.DataFrame(rows)
