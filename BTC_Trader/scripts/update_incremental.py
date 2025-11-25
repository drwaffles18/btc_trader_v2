import pandas as pd
import time
from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import fetch_last_closed_kline_5m
from utils.binance_fetch import bases_para
import os

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

def append_rows(ws, df_new):
    existing = len(ws.get_all_values())
    ws.update(f"A{existing+1}", df_new.astype(str).values.tolist())

def main():
    print("🔄 Iniciando actualización incremental...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ {symbol}")

        df = load_symbol_df(symbol)
        last_close = df["Close time"].max()

        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        k_close_time = pd.to_datetime(close_ms, unit="ms", utc=True).tz_convert("America/Costa_Rica")

        if k_close_time <= last_close:
            print("   ✓ Sin nuevas velas.")
            continue

        row = {
            "Open time": pd.to_datetime(kline[0], unit="ms", utc=True).tz_convert("America/Costa_Rica"),
            "Open": float(kline[1]),
            "High": float(kline[2]),
            "Low": float(kline[3]),
            "Close": float(kline[4]),
            "Volume": float(kline[5]),
            "Close time": k_close_time,
        }

        df_new = pd.DataFrame([row])
        append_rows(ws, df_new)

        print(f"   ✓ Agregada vela nueva {k_close_time}")

    print("\n🎉 Incremental completado.")

if __name__ == "__main__":
    main()
