import sys
import os
import pandas as pd

# Fix paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT)

from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import fetch_last_closed_kline_5m, bases_para

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

# =================================================
# Helpers
# =================================================

def normalize_utc(ms):
    """Convierte timestamp en ms → UTC redondeado a 5m."""
    ts = pd.to_datetime(ms, unit="ms", utc=True)
    return ts.floor("5min")


def append_row(ws, df_new):
    """Inserta una fila al final del sheet."""
    next_row = len(ws.get_all_values()) + 1

    # nuevo formato: values first, luego range
    ws.update(
        values=df_new.astype(str).values.tolist(),
        range_name=f"A{next_row}"
    )


# =================================================
# MAIN
# =================================================

def main():
    print("🔄 Iniciando actualización incremental...")

    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    for symbol in SYMBOLS:
        print(f"\n➡️ {symbol}")

        df = load_symbol_df(symbol)

        # último close en el sheet, normalizado
        last_close = normalize_utc(df["Close time"].max())

        # abrir hoja
        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        # intentar cada base de Binance
        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        # Convertir timestamps Binance → UTC
        k_open_utc = normalize_utc(open_ms)
        k_close_utc = normalize_utc(close_ms)

        # Anti-duplicado real
        if k_close_utc <= last_close:
            print(f"   ✓ No hay velas nuevas (última = {last_close}, incremental = {k_close_utc}).")
            continue

        # ==========================================================
        # Convertir UTC → CST (-06:00)
        # ==========================================================
        k_open_local = k_open_utc.tz_convert("America/Costa_Rica")
        k_close_local = k_close_utc.tz_convert("America/Costa_Rica")

        # ==========================================================
        # Ajuste del close time: 19:15:00 → 19:14:59.999000
        # ==========================================================
        k_close_local = k_close_local - pd.Timedelta(milliseconds=1)

        # ==========================================================
        # Construir fila EXACTA como el histórico completo
        # ==========================================================

        row = {
            # Local time (SIN timezone) — igual que histórico completo
            "Open time": k_open_local.strftime("%Y-%m-%d %H:%M:%S"),
            "Open": float(kline[1]),
            "High": float(kline[2]),
            "Low": float(kline[3]),
            "Close": float(kline[4]),
            "Volume": float(kline[5]),
        
            # Local close time con microsegundos — igual que histórico completo
            "Close time": k_close_local.strftime("%Y-%m-%d %H:%M:%S.%f"),
        
            # UTC extra columns — igual que histórico completo
            #"Open time UTC": k_open_utc.strftime("%Y-%m-%d %H:%M:%S.%f%z"), #no es algo necesario
            #"Close time UTC": k_close_utc.strftime("%Y-%m-%d %H:%M:%S.%f%z"), #no es algo necesario
        }



        df_new = pd.DataFrame([row])
        append_row(ws, df_new)

        print(f"   ✓ Agregada nueva vela: {k_open_local} → {k_close_local}")

    print("\n🎉 Incremental completado sin duplicados.")


if __name__ == "__main__":
    main()
