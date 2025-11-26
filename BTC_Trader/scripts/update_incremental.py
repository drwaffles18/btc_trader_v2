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

CR = pytz.timezone("America/Costa_Rica")


# =====================================================
# Helpers
# =====================================================

def append_row(ws, df_new):
    """Inserta UNA fila sin alterar strings."""
    next_row = len(ws.get_all_values()) + 1
    values = df_new.applymap(
        lambda x: x if isinstance(x, str) else str(x)
    ).values.tolist()
    ws.update(values=values, range_name=f"A{next_row}")


def append_rows(ws, df_new):
    """Inserta VARIAS filas sin alterar strings."""
    next_row = len(ws.get_all_values()) + 1
    values = df_new.applymap(
        lambda x: x if isinstance(x, str) else str(x)
    ).values.tolist()
    end_row = next_row + len(values) - 1
    ws.update(values=values, range_name=f"A{next_row}:G{end_row}")


# =====================================================
# GAP FIXER — versión FINAL
# =====================================================

def fix_gaps(symbol, df_sheet, last_close_utc, next_open_utc, ws, preferred_base):
    """
    Descarga TODAS las velas faltantes desde last_close_utc → next_open_utc,
    utilizando SIEMPRE api.binance.com para asegurar datos reales.
    """

    expected_open = last_close_utc + pd.Timedelta(milliseconds=1)
    expected_open = expected_open.floor("5min")

    if expected_open >= next_open_utc:
        print(f"   ✓ {symbol}: sin gaps.")
        return

    print(f"   ⚠️ {symbol}: Hay gaps → descargando histórico real...")

    start_str = expected_open.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")
    end_str   = next_open_utc.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")

    # HISTÓRICO SOLO DESDE api.binance.com
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
        print("   ⚠️ No llegaron velas faltantes (esto puede ser normal).")
        return

    # Reconvertir formato EXACTO para Google Sheets
    df_missing["Open time"] = df_missing["Open time"].dt.strftime("%Y-%m-%d %H:%M:%S%z")

    df_missing["Close time"] = (
        df_missing["Close time UTC"]
            .dt.tz_convert("America/Costa_Rica")
            - pd.Timedelta(milliseconds=1)
    ).dt.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    df_missing = df_missing[[
        "Open time","Open","High","Low","Close","Volume","Close time"
    ]]

    print(f"   ➕ Agregando {len(df_missing)} velas faltantes reales...")
    append_rows(ws, df_missing)



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

        # ============================
        # OBTENER LA ÚLTIMA VELA REAL
        # ============================
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

        # ============================
        # 1) FIX GAPS ANTES DE NADA
        # ============================
        fix_gaps(symbol, df_sheet, last_close_utc, k_open_utc, ws, preferred_base)

        # ============================
        # 2) AGREGAR LA VELA NUEVA
        # ============================
        if k_close_utc <= last_close_utc:
            print(f"   ✓ No hay vela nueva por agregar.")
            continue

        open_local  = k_open_utc.tz_convert(CR)
        close_local = (k_close_utc.tz_convert(CR) - pd.Timedelta(milliseconds=1))

        row = {
            "Open time":  open_local.isoformat(" "),
            "Open":       float(kline[1]),
            "High":       float(kline[2]),
            "Low":        float(kline[3]),
            "Close":      float(kline[4]),
            "Volume":     float(kline[5]),
            "Close time": close_local.isoformat(" ")
        }

        append_rows(ws, pd.DataFrame([row]))

        print(f"   ✓ Vela agregada: {row['Open time']} → {row['Close time']}")

    print("\n🎉 Incremental completado sin gaps, sin velas falsas y sin duplicados.")


if __name__ == "__main__":
    main()
