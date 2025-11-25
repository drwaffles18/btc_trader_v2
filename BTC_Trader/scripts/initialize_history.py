# scripts/initialize_history_total.py
# Descarga TODAS las velas 5m desde diciembre 2023 a Google Sheets

import os
import sys
import pandas as pd
from datetime import datetime, timedelta

# Importar módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.google_sheets import write_sheet
from utils.binance_fetch import get_binance_5m_data_between  # la crearemos

SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
SHEET_ID = os.environ["GOOGLE_SHEET_ID"]

# Fecha inicial (modifícala si querés)
START_DATE = "2024-12-01 00:00:00"


def main():
    print("🔥 Iniciando descarga TOTAL de histórico 5m...\n")

    for symbol in SYMBOLS:
        print(f"➡ Descargando {symbol} desde {START_DATE} hasta hoy...")

        df = get_binance_5m_data_between(symbol, START_DATE)

        sheet_name = f"{symbol}_5m"
        write_sheet(SHEET_ID, sheet_name, df)

        print(f"   ✓ Guardado en pestaña: {sheet_name} ({len(df)} velas)\n")

    print("🎉 Histórico TOTAL cargado correctamente en Google Sheets.")


if __name__ == "__main__":
    main()
