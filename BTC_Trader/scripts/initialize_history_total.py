# scripts/initialize_history_total.py
# Descarga solo 900 velas (3 días) por símbolo y las sube a Google Sheets

import pandas as pd
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT)

from utils.binance_fetch import get_binance_5m_data
from utils.google_client import get_gsheet_client

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

LIMIT_5M = 900  # 3 días


def df_to_sheet(df, ws):
    ws.clear()
    ws.update(values=[df.columns.tolist()], range_name="A1")
    ws.update(values=df.astype(str).values.tolist(), range_name="A2")


def main():
    print("🔥 Cargando histórico COMPACTO de 3 días (900 velas)")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ Descargando {symbol}...")

        # Descargar solo 900 velas
        df = get_binance_5m_data(symbol, limit=LIMIT_5M)

        try:
            ws = sh.worksheet(symbol)
        except:
            ws = sh.add_worksheet(title=symbol, rows="2000", cols="20")

        df_to_sheet(df, ws)
        print(f"   ✓ Guardado {symbol}: {len(df)} filas")

    print("\n🎉 Histórico compacto cargado en Google Sheets.")


if __name__ == "__main__":
    main()
