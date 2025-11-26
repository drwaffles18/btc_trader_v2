# utils/gap_fixer.py
import pandas as pd
import pytz
from utils.binance_fetch import fetch_last_closed_kline_5m

CR = pytz.timezone("America/Costa_Rica")


def fix_gaps(df, symbol, base):
    """
    Verifica e inserta velas faltantes ENTRE la última vela del sheet
    y la última vela real cerrada en Binance.

    Retorna:
        - DataFrame con filas adicionales para insertar
        - DataFrame vacío si no hay gaps
    """
    if df.empty:
        return pd.DataFrame()

    # ============================
    # 1. Última vela en el sheet
    # ============================
    last_sheet_close_local = pd.to_datetime(df["Close time"].max())

    # Si viene sin timezone → asumir CR
    if last_sheet_close_local.tzinfo is None:
        last_sheet_close_utc = last_sheet_close_local.tz_localize(CR).tz_convert("UTC")
    else:
        last_sheet_close_utc = last_sheet_close_local.tz_convert("UTC")

    # ============================
    # 2. Última vela REAL en Binance
    # ============================
    _, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
    binance_last_close_utc = pd.to_datetime(close_ms - 1, unit="ms", utc=True)

    # Expected next open (5m después del último close del sheet)
    expected_next_utc = last_sheet_close_utc + pd.Timedelta(minutes=5)

    # Si expected >= real → no hay gaps
    if expected_next_utc >= binance_last_close_utc:
        print(f"   ✓ {symbol}: no hay gaps reales.")
        return pd.DataFrame()

    print(f"   ⚠️ {symbol}: detectados gaps entre {expected_next_utc} y {binance_last_close_utc}")

    # ============================
    # 3. Descargar velas faltantes
    # ============================
    try:
        from utils.binance_fetch import get_range_klines_5m
        klines = get_range_klines_5m(
            symbol,
            start_utc=expected_next_utc,
            end_utc=binance_last_close_utc
        )
    except Exception as e:
        print(f"   ❌ Error descargando gaps de {symbol}: {e}")
        return pd.DataFrame()

    if klines.empty:
        print(f"   ⚠️ {symbol}: Binance devolvió 0 velas → error temporal probable.")
        return pd.DataFrame()

    # ============================
    # 4. Convertir al formato del sheet
    # ============================
    rows = []
    for _, row in klines.iterrows():
        open_utc = row["Open time UTC"]
        close_utc = row["Close time UTC"]

        # Convertir a hora local Costa Rica
        open_local = open_utc.tz_convert(CR)

        # Cierre exacto con .999000 y offset correcto
        close_local = (
            close_utc.tz_convert(CR)
            - pd.Timedelta(milliseconds=1)
        ).astimezone(CR)

        # Forzar microsegundos EXACTOS para estandarizar
        close_local = close_local.replace(microsecond=999000)

        # Formato EXACTO como en tu histórico
        open_str = open_local.strftime("%Y-%m-%d %H:%M:%S.%f%z").replace("-0600", "-06:00")
        close_str = close_local.strftime("%Y-%m-%d %H:%M:%S.%f%z").replace("-0600", "-06:00")

        rows.append({
            "Open time":  open_str,
            "Open":       row["Open"],
            "High":       row["High"],
            "Low":        row["Low"],
            "Close":      row["Close"],
            "Volume":     row["Volume"],
            "Close time": close_str,
        })

    return pd.DataFrame(rows)
