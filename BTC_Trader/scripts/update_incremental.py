import sys
import os
import pandas as pd
import pytz

# Fix paths
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT)

from utils.google_client import get_gsheet_client
from utils.load_from_sheets import load_symbol_df
from utils.binance_fetch import fetch_last_closed_kline_5m, bases_para

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

CR = pytz.timezone("America/Costa_Rica")   # zona horaria de tu histórico

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

        # último close en el sheet (local, pero lo convertimos a UTC para comparar)
        last_close_local = pd.to_datetime(df["Close time"].max())
        
        # ==================================
        # Fix: manejar NaT (sheet vacío)
        # ==================================
        if pd.isna(last_close_local):
            last_close_local = pd.Timestamp("2000-01-01 00:00:00")
        
        # =============================
        # Manejo robusto de timezone
        # =============================
        if last_close_local.tzinfo is None:
            # naive → asumimos que es hora local Costa Rica
            last_close_utc = last_close_local.tz_localize(CR).tz_convert("UTC")
        else:
            # ya tiene timezone → convertir directamente
            last_close_utc = last_close_local.tz_convert("UTC")




        try:
            ws = sh.worksheet(symbol)
        except:
            raise RuntimeError(f"❌ La hoja {symbol} no existe.")

        # Intentar descargar desde las bases
        for base in bases_para(symbol):
            try:
                kline, open_ms, close_ms, _ = fetch_last_closed_kline_5m(symbol, base)
                break
            except Exception as e:
                print(f"   ✗ {base} falló: {e}")
                continue

        # timestamps de Binance → UTC
        k_open_utc  = normalize_utc(open_ms)
        k_close_utc = normalize_utc(close_ms)

        # Si no es nueva, salir
        if k_close_utc <= last_close_utc:
            print(f"   ✓ No hay velas nuevas (última = {last_close_utc}, incremental = {k_close_utc})")
            continue

        # -----------------------------------------
        # Convertir UTC → hora local de Costa Rica
        # -----------------------------------------
        k_open_local = k_open_utc.tz_convert(CR)
        k_close_local = k_close_utc.tz_convert(CR)

        # Ajustar el close time a XX:XX:59.999000
        k_close_local = (k_close_local - pd.Timedelta(milliseconds=1))

        # -----------------------------------------
        # ENTRAR AL GSHEET EXACTAMENTE COMO EL HISTÓRICO
        # -----------------------------------------
        row = {
            "Open time":  k_open_local.strftime("%Y-%m-%d %H:%M:%S"),
            "Open":       float(kline[1]),
            "High":       float(kline[2]),
            "Low":        float(kline[3]),
            "Close":      float(kline[4]),
            "Volume":     float(kline[5]),
            "Close time": k_close_local.strftime("%Y-%m-%d %H:%M:%S.%f")
        }

        df_new = pd.DataFrame([row])
        append_row(ws, df_new)

        print(f"   ✓ Vela agregada: {row['Open time']} → {row['Close time']}")

    print("\n🎉 Incremental completado sin duplicados.")

if __name__ == "__main__":
    main()
