# scripts/initialize_history.py
# Descarga 3 días de velas 5m de Binance y guarda un archivo por símbolo en /data

import os
import sys
import pandas as pd

# Asegurar import relativo al root del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.binance_fetch import get_binance_5m_data

# === CONFIGURACIÓN ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
HISTORY_LIMIT_5M = 900  # 3 días de velas

# 🚀 Muy importante: usar el volumen real
DATA_DIR = "/data"


def main():
    print("🔥 Iniciando descarga de histórico 5m (3 días)...\n")

    # Crear directorio /data si no existe
    os.makedirs(DATA_DIR, exist_ok=True)

    for symbol in SYMBOLS:
        try:
            print(f"➡️ Descargando {symbol}...")
            df = get_binance_5m_data(symbol, limit=HISTORY_LIMIT_5M)

            # Guardar a CSV dentro del volumen /data
            output_path = os.path.join(DATA_DIR, f"{symbol}_5m.csv")
            df.to_csv(output_path, index=False)

            print(f"   ✓ Guardado en {output_path}\n")

        except Exception as e:
            print(f"   ❌ Error descargando {symbol}: {e}\n")

    print("🎉 Finalizado. Los archivos están en /data")


if __name__ == "__main__":
    main()
