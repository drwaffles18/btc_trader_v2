import sys
import os
import pandas as pd

# Fix import paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT)

from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import fetch_last_closed_kline_5m, bases_para

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]


def normalize_utc(ms):
    """Convierte milisegundos UTC a datetime UTC y redondea a vela 5m."""
    return pd.to_datetime(ms, unit="ms", utc=True).dt.floor("5min")


def main():
    print("🔄 Iniciando actualización incremental...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ {symbol}")

        # 1. Cargar dataframe actual desde Google Sheets
        df = load_symbol_df(symbol)

        # USAR SIEMPRE Close time UTC
        if "Close time UTC" not in df.columns:
            raise RuntimeError(f"❌ La hoja {symbol} no contiene 'Close time UTC'. Revisar histórico.")

        last_close_utc = df["Close time UTC"].max()

        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        # 2. Intentar descargar la última vela desde Binance
        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        # 3. Normalizar timestamps recibidos
        k_open_utc = normalize_utc(open_ms)
        k_close_utc = normalize_utc(close_ms)

        # 4. Comparación anti-duplicados usando UTC
        if k_close_utc <= last_close_utc:
            print(f"   ✓ No hay velas nuevas (última = {last_close_utc}, incremental = {k_close_utc}).")
            continue

        # 5. Construir fila nueva
        row = {
            "Open time UTC": k_open_utc,
            "Open": float(kline[1]),
            "High": float(kline[2]),
            "Low": float(kline[3]),
            "Close": float(kline[4]),
            "Volume": float(kline[5]),
            "Close time UTC": k_close_utc
        }

        df_new = pd.DataFrame([row])

        # 6. Insertar al final
        next_row = len(ws.get_all_values()) + 1
        ws.update(f"A{next_row}", df_new.astype(str).values.tolist())

        print(f"   ✓ Agregada vela nueva: {k_close_utc}")

    print("\n🎉 Incremental completado sin duplicados.")


if __name__ == "__main__":
    main()
