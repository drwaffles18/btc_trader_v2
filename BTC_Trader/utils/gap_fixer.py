# utils/gap_fixer.py
import pandas as pd
import pytz

CR = pytz.timezone("America/Costa_Rica")


def fix_gaps(symbol, df_sheet, last_close_utc, next_open_utc, ws, preferred_base):
    """
    Rellena TODAS las velas faltantes entre:
        last_close_utc → next_open_utc (sin incluir esta última)

    Este módulo:
    - Descarga velas reales de Binance
    - Ajusta timezone a CR
    - Ajusta close time a .999000
    - Formatea EXACTAMENTE como el histórico
    - NO permite que Google Sheets toque el formato
    """

    # Calcular el primer open que falta (redondeado a 5m)
    expected_open = last_close_utc + pd.Timedelta(milliseconds=1)
    expected_open = expected_open.floor("5min")

    # Si no hay velas en el hueco, terminar
    if expected_open >= next_open_utc:
        print(f"   ✓ {symbol}: sin gaps.")
        return

    print(f"   ⚠️ {symbol}: gaps detectados entre {expected_open} → {next_open_utc}")

    # ===============================
    # Descarga del rango de klines
    # ===============================
    from utils.binance_fetch import get_binance_5m_data_between

    start_str = expected_open.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")
    end_str   = next_open_utc.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")

    try:
        df_missing = get_binance_5m_data_between(symbol, start_str, end_str)
    except Exception as e:
        print(f"   ❌ Error descargando velas faltantes: {e}")
        return

    # Filtro exacto de rango
    df_missing = df_missing[
        (df_missing["Open time UTC"] >= expected_open) &
        (df_missing["Open time UTC"] < next_open_utc)
    ].copy()

    if df_missing.empty:
        print(f"   ⚠️ {symbol}: rango vacío en Binance.")
        return

    # ===============================
    # Formateo EXACTO como histórico
    # ===============================

    def fmt_open(dt):
        """Open time → formato exacto YYYY-MM-DD HH:MM:SS-06:00"""
        return dt.isoformat(" ")

    def fmt_close(dt):
        """Close time ajustado a .999000 EXACTO"""
        dt_adj = dt - pd.Timedelta(milliseconds=1)
        dt_adj = dt_adj.replace(microsecond=999000)
        return dt_adj.isoformat(" ")

    df_missing["Open time"] = df_missing["Open time"].apply(fmt_open)
    df_missing["Close time"] = df_missing["Close time"].apply(fmt_close)

    df_missing = df_missing[[
        "Open time", "Open", "High", "Low", "Close", "Volume", "Close time"
    ]]

    print(f"   ➕ Se agregarán {len(df_missing)} velas faltantes...")

    # ===============================
    # Inserción al Google Sheet
    # ===============================
    append_rows(ws, df_missing)



# =====================================================
# Necesitamos esta función para poder insertar múltiples filas
# =====================================================

def append_rows(ws, df_new):
    """Inserta múltiples filas preservando strings EXACTOS."""
    next_row = len(ws.get_all_values()) + 1

    # No convertir strings → Google Sheets no toca el formato
    values = df_new.applymap(lambda x: x if isinstance(x, str) else str(x)).values.tolist()

    end_row = next_row + len(values) - 1
    ws.update(values=values, range_name=f"A{next_row}:G{end_row}")
