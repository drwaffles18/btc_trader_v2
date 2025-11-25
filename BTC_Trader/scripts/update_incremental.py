import sys
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT)

import pandas as pd
from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import fetch_last_closed_kline_5m, bases_para

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]


def append_rows(ws, df_new: pd.DataFrame):
    existing = len(ws.get_all_values())
    ws.update(f"A{existing+1}", df_new.astype(str).values.tolist())


def main():
    print("🔄 Iniciando actualización incremental...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ {symbol}")

        # 1) Cargar histórico actual y tomar el ÚLTIMO OPEN TIME
        df = load_symbol_df(symbol)
        last_open = df["Open time"].max()

        try:
            ws = sh.worksheet(symbol)
        except Exception:
            raise RuntimeError(f"❌ La hoja {symbol} no existe en Google Sheets.")

        # 2) Pedir la última vela 5m cerrada a Binance
        kline = None
        for base in bases_para(symbol):
            try:
                k, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                kline = k
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        if kline is None:
            print("   ❌ No se pudo obtener la última vela.")
            continue

        # 3) Tiempos en zona horaria CR
        k_open_time = pd.to_datetime(kline[0], unit="ms", utc=True).tz_convert("America/Costa_Rica")
        k_close_time = pd.to_datetime(kline[6], unit="ms", utc=True).tz_convert("America/Costa_Rica")

        # 4) Si ya tengo una vela con ese OPEN TIME, no hago nada
        if k_open_time <= last_open:
            print(f"   ✓ Sin nuevas velas (último open ya presente: {last_open})")
            continue

        # 5) Construir fila nueva
        row = {
            "Open time": k_open_time,
            "Open": float(kline[1]),
            "High": float(kline[2]),
            "Low": float(kline[3]),
            "Close": float(kline[4]),
            "Volume": float(kline[5]),
            "Close time": k_close_time,
        }

        df_new = pd.DataFrame([row])
        append_rows(ws, df_new)

        print(f"   ✓ Agregada vela nueva (open={k_open_time}, close={k_close_time})")

    print("\n🎉 Incremental completado.")


if __name__ == "__main__":
    main()
