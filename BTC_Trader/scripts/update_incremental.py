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

# ==========================================
# Normalizar timestamps correctamente
# ==========================================
def normalize_utc(ms):
    """Convierte timestamp en ms → UTC redondeado a 5m."""
    ts = pd.to_datetime(ms, unit="ms", utc=True)
    return ts.floor("5min")


def append_rows(ws, df_new):
    next_row = len(ws.get_all_values()) + 1
    ws.update(f"A{next_row}", df_new.astype(str).values.tolist())


def main():
    print("🔄 Iniciando actualización incremental...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ {symbol}")

        df = load_symbol_df(symbol)

        # Normalizar último close en el sheet
        last_close = normalize_utc(df["Close time"].max())

        # abrir hoja
        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        # intentar descargar desde varias bases
        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        # normalizar timestamps Binance
        k_open_utc = normalize_utc(open_ms)
        k_close_utc = normalize_utc(close_ms)

        # anti-duplicados estrictos
        if k_close_utc <= last_close:
            print(f"   ✓ No hay velas nuevas (última = {last_close}, incremental = {k_close_utc}).")
            continue

        # construir la fila
        row = {
            "Open time": k_open_utc,
            "Open": float(kline[1]),
            "High": float(kline[2]),
            "Low": float(kline[3]),
            "Close": float(kline[4]),
            "Volume": float(kline[5]),
            "Close time": k_close_utc
        }

        df_new = pd.DataFrame([row])
        append_rows(ws, df_new)

        print(f"   ✓ Agregada nueva vela: {k_close_utc}")

    print("\n🎉 Incremental completado sin duplicados.")


if __name__ == "__main__":
    main()
