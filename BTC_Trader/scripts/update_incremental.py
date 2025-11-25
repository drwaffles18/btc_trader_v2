# scripts/update_incremental.py

import os
import sys
import pandas as pd

# Importamos binance_fetch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.binance_fetch import get_binance_5m_data

# === CONFIGURACIÓN ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MAX_ROWS = 900   # Mantener 3 días


def load_existing(symbol):
    """
    Carga el CSV del símbolo y valida columnas necesarias.
    """
    path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")

    if not os.path.exists(path):
        print(f"⚠ No existe historial para {symbol}.")
        return None, path

    df = pd.read_csv(path)

    # Convertir timestamps importantes
    if "Close time UTC" not in df.columns:
        raise RuntimeError(f"{symbol}: Falta columna 'Close time UTC' en el CSV.")

    df["Close time UTC"] = pd.to_datetime(df["Close time UTC"], utc=True)

    df.sort_values("Close time UTC", inplace=True)

    return df, path



def update_symbol(symbol):
    print(f"\n🔄 Actualizando {symbol}...")

    df_old, path = load_existing(symbol)
    if df_old is None:
        return

    # Última vela registrada
    last_close = df_old["Close time UTC"].iloc[-1]
    last_close_ms = int(last_close.timestamp() * 1000)

    print(f"Última vela registrada: {last_close}")

    # =============================
    # DESCARGA INCREMENTAL
    # =============================
    print(f"Solicitando velas nuevas desde {last_close_ms + 1} ...")

    df_new = get_binance_5m_data(
        symbol,
        start_ms=last_close_ms + 1
    )

    if df_new.empty:
        print("✔ No hay velas nuevas.")
        return

    # Convertir timestamps
    df_new["Close time UTC"] = pd.to_datetime(df_new["Close time UTC"], utc=True)

    # Filtrar duplicados
    df_new = df_new[df_new["Close time UTC"] > last_close]

    if df_new.empty:
        print("✔ Las velas recibidas eran duplicadas.")
        return

    print(f"📈 Velas nuevas recibidas: {len(df_new)}")

    # =============================
    # CONCATENAR Y LIMITAR A 900
    # =============================
    df_final = pd.concat([df_old, df_new], ignore_index=True)

    if len(df_final) > MAX_ROWS:
        df_final = df_final.iloc[-MAX_ROWS:]

    df_final.to_csv(path, index=False)

    print(f"💾 Guardado → {path}")


def main():
    print("\n🚀 Ejecutando actualización incremental 5m...\n")

    os.makedirs(DATA_DIR, exist_ok=True)

    for symbol in SYMBOLS:
        update_symbol(symbol)

    print("\n🎉 Incremental finalizado.\n")


if __name__ == "__main__":
    main()
