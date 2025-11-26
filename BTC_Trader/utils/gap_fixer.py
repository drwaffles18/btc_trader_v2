# utils/gap_fixer.py
import pandas as pd
import pytz
from utils.binance_fetch import fetch_last_closed_kline_5m

CR = pytz.timezone("America/Costa_Rica")


def fix_gaps(df, symbol, base):
    if df.empty:
        return pd.DataFrame()

    last_close_local = pd.to_datetime(df["Close time"].max())

    if last_close_local.tzinfo is None:
        last_close_utc = last_close_local.tz_localize(CR).tz_convert("UTC")
    else:
        last_close_utc = last_close_local.tz_convert("UTC")

    _, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
    binance_last_close_utc = pd.to_datetime(close_ms - 1, unit="ms", utc=True)

    expected_next_utc = last_close_utc + pd.Timedelta(minutes=5)

    if expected_next_utc >= binance_last_close_utc:
        print(f"   ✓ {symbol}: no hay gaps.")
        return pd.DataFrame()

    print(f"   ⚠️ {symbol}: gaps detectados entre {expected_next_utc} y {binance_last_close_utc}")

    from utils.binance_fetch import get_range_klines_5m
    klines = get_range_klines_5m(symbol, start_utc=expected_next_utc, end_utc=binance_last_close_utc)

    if klines.empty:
        print(f"   ⚠️ {symbol}: Binance devolvió 0 velas.")
        return pd.DataFrame()

    rows = []
    for _, r in klines.iterrows():
        open_local = r["Open time UTC"].tz_convert(CR)

        close_local = (
            r["Close time UTC"].tz_convert(CR)
            - pd.Timedelta(milliseconds=1)
        )

        close_local = close_local.replace(microsecond=999000)

        # FORZAR TEXTO → Google Sheets no toca nada
        open_str  = "'" + open_local.isoformat(" ")
        close_str = "'" + close_local.isoformat(" ")

        rows.append({
            "Open time":  open_str,
            "Open":       r["Open"],
            "High":       r["High"],
            "Low":        r["Low"],
            "Close":      r["Close"],
            "Volume":     r["Volume"],
            "Close time": close_str
        })

    return pd.DataFrame(rows)
