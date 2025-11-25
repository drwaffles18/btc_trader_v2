# scripts/update_history.py
# Actualiza el histórico de velas 5m añadiendo sólo velas nuevas.

import os
import sys
import pandas as pd
from datetime import datetime

# Asegurar import relativo al root del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.binance_fetch import get_binance_5m_data

# === CONFIGURACIÓN ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def load_existing(symbol: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")
    if not os.path.exists(path):
        print(f"⚠️ No existe {path}, no se puede actualizar.")
        return None

    df = pd.read_csv(path)
    # Convertir timestamps en caso de ser string
    df["Open time UTC"] = pd.to_datetime(df["Open time UTC"], utc=True)
    df["Close time UTC"] = pd.to_datetime(df["Close time UTC"], utc=True)
    return df


def main():
    print("🔄 Iniciando actualización incremental de histórico 5m...\n")

    os.makedirs(DATA_DIR, exist_ok=True)

    for symbol in SYMBOLS:
        print(f"\n================ {symbol} ================")

        df_old = load_existing(symbol)
        if df_old is None:
            print(f"⏭️ Saltando {symbol} porque no hay archivo previo.")
            continue

        # Última vela guardada
        last_ts = df_old["Close time UTC"].max()
        last_ms = int(last_ts.timestamp() * 1000)

        print(f"📌 Última vela guardada: {last_ts}  (ms={last_ms})")

        try:
            # Descargar velas nuevas
            df_new = get_binance_5m_data(
                symbol,
                start_ms=last_ms + 1   # para evitar incluir duplicado
            )

            if df_new.empty:
                print("⏭️ No hay velas nuevas.")
                continue

            # Remover velas duplicadas basadas en timestamp
            df_combined = pd.concat([df_old, df_new], ignore_index=True)
            df_combined = df_combined.drop_duplicates(
                subset=["Open time UTC"], keep="first"
            )
            df_combined = df_combined.sort_values("Open time UTC")

            # Guardar CSV actualizado
            output_path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")
            df_combined.to_csv(output_path, index=False)

            print(f"✅ Actualizado: {len(df_new)} velas nuevas añadidas.")
            print(f"💾 Guardado en {output_path}")

        except Exception as e:
            print(f"❌ Error actualizando {symbol}: {e}")

    print("\n🎉 Finalizado: histórico incremental actualizado.")


if __name__ == "__main__":
    main()
