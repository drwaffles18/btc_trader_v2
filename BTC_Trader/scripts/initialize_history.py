import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# === Importar funciones de Binance ===
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.binance_fetch import get_binance_5m_data_between

# === Configuración ===
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
START_DATE = "2024-12-01 00:00:00"

# === Credenciales ===
SERVICE_JSON = os.getenv("GOOGLE_CREDS_BASE64")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

if SERVICE_JSON is None:
    raise RuntimeError("Falta GOOGLE_SERVICE_ACCOUNT_JSON")

if SHEET_ID is None:
    raise RuntimeError("Falta GOOGLE_SHEET_ID")

# Crear credenciales para Google Sheets
creds = Credentials.from_service_account_info(
    eval(SERVICE_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)


def upload_symbol(symbol, df):
    """
    Sube el dataframe df a una hoja con nombre symbol.
    Si la hoja existe, la borra y la recrea.
    """
    try:
        ws = sh.worksheet(symbol)
        sh.del_worksheet(ws)
    except:
        pass

    ws = sh.add_worksheet(title=symbol, rows="5", cols="20")

    # Subir encabezados
    ws.update("A1", [df.columns.tolist()])

    # Subir valores
    ws.update("A2", df.astype(str).values.tolist())

    print(f"✓ {symbol}: {len(df)} velas subidas.")


def main():
    print("\n🔥 === DESCARGA COMPLETA DE HISTÓRICO 5M ===\n")

    for symbol in SYMBOLS:
        print(f"\n➡️ Bajando histórico completo de {symbol}…")
        df = get_binance_5m_data_between(symbol, START_DATE)
        upload_symbol(symbol, df)

    print("\n🎉 Histórico inicial listo en Google Sheets.\n")


if __name__ == "__main__":
    main()
