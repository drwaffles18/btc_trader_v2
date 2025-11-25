import pandas as pd
from utils.binance_fetch import fetch_5m_historical_range
from utils.google_client import get_gsheet_client
import os

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
START_DATE = "2024-12-01 00:00:00"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

def df_to_sheet(df, ws):
    ws.clear()
    ws.update([df.columns.tolist()] + df.astype(str).values.tolist())

def main():
    print("🔥 Iniciando descarga TOTAL 5m desde 2024-12-01...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ Descargando {symbol}...")
        df = fetch_5m_historical_range(symbol, START_DATE)

        try:
            ws = sh.worksheet(symbol)
        except:
            ws = sh.add_worksheet(title=symbol, rows="100000", cols="20")

        df_to_sheet(df, ws)
        print(f"   ✓ Guardado {symbol} ({len(df)} filas)")

    print("\n🎉 Histórico completo cargado en Google Sheets.")

if __name__ == "__main__":
    main()
