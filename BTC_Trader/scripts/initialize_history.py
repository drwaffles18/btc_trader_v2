# scripts/initialize_history.py
# Descarga 3 días de velas 5m de Binance y guarda un archivo por símbolo en Google Sheets

import os
import sys
import pandas as pd

# Asegurar import relativo al root del proyecto
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.binance_fetch import get_binance_5m_data
from utils.google_sheets import write_sheet

# === CONFIGURACIÓN ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
HISTORY_LIMIT_5M = 900  # 3 días de velas

# GOOGLE SHEETS
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]


def main():
    print("🔥 Iniciando carga de histórico 5m hacia Google Sheets...\n")

    for symbol in SYMBOLS:
        try:
            print(f"➡️ Descargando {symbol}...")
            df = get_binance_5m_data(symbol, limit=HISTORY_LIMIT_5M)

            # Nombre de la pestaña en Google Sheets
            sheet_name = f"{symbol}_5m"

            # Escribir DataFrame completo a Google Sheets
            write_sheet(SHEET_ID, sheet_name, df)

            print(f"   ✓ Guardado en pestaña: {sheet_name}\n")

        except Exception as e:
            print(f"   ❌ Error descargando/escribiendo {symbol}: {e}\n")

    print("🎉 Histórico cargado completamente en Google Sheets.")


if __name__ == "__main__":
    main()
