# scripts/update_incremental.py

import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Importamos el módulo de Binance
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.binance_fetch import get_binance_5m_data

# === CONFIG ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
MAX_ROWS = 900   # 3 días de histórico = 900 velas 5m


def load_existing(symbol):
    """
    Carga el CSV existente del historial.
    Si no existe, devuelve None.
    """
    path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")
    if not os.path.exists(path):
        print(f"⚠ No existe historial previo para {symbol}. Saltando...")
        return None, path

    df = pd.read_csv(path)

    # Convertir timestamps
    df["Open time"] = pd.to_datetime(df["Open time"])
    df.sort_values("Open time", inplace=True)

    return df, path


def update_symbol(symbol):
    print(f"\n🔄 Actualizando {symbol}")

    df_old, path = load_existing(symbol)

    if df_old is None:
        return

    # Última vela histórica
    last_ts = df_old["Open time"].max()
    print(f"Última vela histórica → {last_ts}")

    # Binance necesita timestamp en milisegundos
    start_ms = int(last_ts.timestamp() * 1000)

    # Descargar nuevas velas DESDE la última timestamp
    print("Descargando velas nuevas desde Binance...")
    df_new = get_binance_5m_data(symbol, start_ms=start_ms)

    if df_new.empty:
        print("✔ No hay velas nuevas.")
        return

    # Convertir timestamp
    df_new["Open time"] = pd.to_datetime(df_new["Open time"])
    df_new.sort_values("Open time", inplace=True)

    # Quitar la vela duplicada (típico)
    df_new = df_new[df_new["Open time"] > last_ts]

    if df_new.empty:
        print("✔ Solo llegó una vela duplicada. Nada que agregar.")
        return

    print(f"📈 Nuevas velas encontradas: {len(df_new)}")

    # Concatenar
    df_full = pd.concat([df_old, df_new], ignore_index=True)

    # Mantener máximo 900 filas (3 días)
    if len(df_full) > MAX_ROWS:
        df_full = df_full.iloc[-MAX_ROWS:]

    # Guardar
    df_full.to_csv(path, index=False)

    print(f"💾 Guardado actualizado → {path}")


def main():
    print("\n🚀 Ejecutando actualización incremental de 5m...\n")

    # Asegurar directorio
    os.makedirs(DATA_DIR, exist_ok=True)

    for symbol in SYMBOLS:
        update_symbol(symbol)

    print("\n🎉 Incremental finalizado.\n")


if __name__ == "__main__":
    main()
