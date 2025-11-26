import sys
import os
import pandas as pd
import pytz

# Fix paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT)

from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import (
    fetch_last_closed_kline_5m,
    bases_para,
    get_binance_5m_data_between,
)

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
MAX_KEEP = 1200  # 🔥 límite global de filas

CR = pytz.timezone("America/Costa_Rica")


# =====================================================
# Helpers
# =====================================================

def ensure_capacity(ws, required_last_row: int) -> None:
    """
    Asegura que la hoja tenga al menos `required_last_row` filas en el grid.
    Si no, agrega las filas necesarias con ws.add_rows().
    """
    current_rows = ws.row_count  # tamaño del grid (no datos)

    if required_last_row > current_rows:
        extra = required_last_row - current_rows
        print(f"[ensure_capacity] Ampliando hoja: {current_rows} → {current_rows + extra} filas")
        ws.add_rows(extra)


def purge_old_rows(ws, max_keep: int = 1200) -> None:
    """
    Mantiene como máximo `max_keep` filas con datos (incluyendo encabezado).

    Mantiene:
      - Fila 1: encabezado
      - Fila 2: primer dato reciente
      - Desde fila 3 borra todo lo extra si excede max_keep
    """
    used_rows = len(ws.col_values(1))  # número real de filas usadas

    if used_rows <= max_keep:
        return

    excess = used_rows - max_keep
    start_index = 3
    end_index = start_index + excess - 1

    print(f"[purge_old_rows] Podando filas {start_index} → {end_index} (total usadas: {used_rows})")

    ws.delete_rows(start_index, end_index)


def append_rows(ws, df, max_keep: int = 1200) -> None:
    """
    Inserta las filas de `df` al final de la hoja y aplica poda automática.
    """
    if df is None or df.empty:
        print("[append_rows] DataFrame vacío, no se agrega nada")
        return

    values = df.values.tolist()

    used_rows = len(ws.col_values(1))
    next_row = used_rows + 1 if used_rows > 0 else 1
    end_row = next_row + len(values) - 1

    # 1. Garantizar espacio
    ensure_capacity(ws, end_row)

    # 2. Escribir filas
    range_name = f"A{next_row}:G{end_row}"
    print(f"[append_rows] Insertando {len(values)} filas en {range_name}")
    ws.update(range_name=range_name, values=values)

    # 3. Podar si es necesario
    purge_old_rows(ws, max_keep=max_keep)


# =====================================================
# GAP FIXER — versión FINAL
# =====================================================

def fix_gaps(symbol, df_sheet, last_close_utc, next_open_utc, ws, preferred_base, max_keep=1200):
    """
    Descarga TODAS las velas faltantes entre last_close_utc y next_open_utc.
    """
    expected_open = last_close_utc + pd.Timedelta(milliseconds=1)
    expected_open = expected_open.floor("5min")

    if expected_open >= next_open_utc:
        print(f"   ✓ {symbol}: sin gaps.")
        return

    print(f"   ⚠️ {symbol}: Hay gaps → descargando velas reales...")

    start_str = expected_open.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")
    end_str   = next_open_utc.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")

    df_missing = get_binance_5m_data_between(
        symbol,
        start_str,
        end_str,
        preferred_base=preferred_base
    )

    df_missing = df_missing[
        (df_missing["Open time UTC"] >= expected_open) &
        (df_missing["Open time UTC"] < next_open_utc)
    ].copy()

    if df_missing.empty:
        print(f"   ⚠️ {symbol}: no se recibieron velas faltantes.")
        return

    # Reconstrucción EXACTA para Google Sheets
    df_missing["Open time"] = df_missing["Open time"].dt.strftime("%Y-%m-%d %H:%M:%S%z")

    df_missing["Close time"] = (
        df_missing["Close time UTC"].dt.tz_convert("America/Costa_Rica")
        - pd.Timedelta(milliseconds=1)
    ).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    df_missing = df_missing[
        ["Open time", "Open", "High", "Low", "Close", "Volume", "Close time"]
    ]

    print(f"   ➕ {symbol}: agregando {len(df_missing)} velas faltantes...")

    append_rows(ws, df_missing, max_keep=max_keep)


# =====================================================
# MAIN
# =====================================================

def main():
    print("🔄 Iniciando actualización incremental con gap fixing...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ Procesando {symbol}...")

        df_sheet = load_symbol_df(symbol)

        last_close_local = pd.to_datetime(df_sheet["Close time"].max())

        if pd.isna(last_close_local):
            last_close_local = pd.Timestamp("2000-01-01 00:00:00", tz=CR)

        if last_close_local.tzinfo is None:
            last_close_utc = last_close_local.tz_localize(CR).tz_convert("UTC")
        else:
            last_close_utc = last_close_local.tz_convert("UTC")

        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        # Obtener la última vela real
        preferred_base = None
        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, server_ms = fetch_last_closed_kline_5m(symbol, base)
                preferred_base = base
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        if preferred_base is None:
            print(f"❌ {symbol}: no se pudo obtener la última vela cerrada.")
            continue

        k_open_utc  = pd.to_datetime(open_ms,  unit="ms", utc=True)
        k_close_utc = pd.to_datetime(close_ms, unit="ms", utc=True)

        # 1) FIX GAPS
        fix_gaps(symbol, df_sheet, last_close_utc, k_open_utc, ws, preferred_base, max_keep=MAX_KEEP)

        # 2) Agregar vela nueva
        if k_close_utc <= last_close_utc:
            print(f"   ✓ No hay vela nueva por agregar.")
            continue

        open_local  = k_open_utc.tz_convert(CR)
        close_local = k_close_utc.tz_convert(CR) - pd.Timedelta(milliseconds=1)

        row = {
            "Open time":  open_local.isoformat(" "),
            "Open":       float(kline[1]),
            "High":       float(kline[2]),
            "Low":        float(kline[3]),
            "Close":      float(kline[4]),
            "Volume":     float(kline[5]),
            "Close time": close_local.isoformat(" ")
        }

        append_rows(ws, pd.DataFrame([row]), max_keep=MAX_KEEP)

        print(f"   ✓ Vela agregada: {row['Open time']} → {row['Close time']}")

    print("\n🎉 Incremental completado sin gaps, sin velas falsas y sin duplicados.")


if __name__ == "__main__":
    main()
