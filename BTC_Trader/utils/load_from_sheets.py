import os
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# ============================
# Google Sheets — Inicialización
# ============================

SERVICE_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
SHEET_ID     = os.getenv("GOOGLE_SHEET_ID")

if SERVICE_JSON is None:
    raise RuntimeError("Falta la variable GOOGLE_SERVICE_ACCOUNT_JSON")

if SHEET_ID is None:
    raise RuntimeError("Falta la variable GOOGLE_SHEET_ID")

# Crear credenciales
creds = Credentials.from_service_account_info(
    eval(SERVICE_JSON),
    scopes=["https://www.googleapis.com/auth/spreadsheets"]
)

gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)


# ============================
# Cargar un símbolo
# ============================
def load_symbol_df(symbol: str) -> pd.DataFrame:
    """
    Lee la hoja del símbolo desde Google Sheets
    y devuelve un DataFrame ordenado por Open time UTC.
    """

    try:
        ws = sh.worksheet(symbol)
    except Exception:
        raise RuntimeError(f"No existe la hoja '{symbol}' en Google Sheets.")

    data = ws.get_all_records()

    if len(data) == 0:
        raise RuntimeError(f"La hoja {symbol} está vacía.")

    df = pd.DataFrame(data)

    # Asegurar el orden correcto por tiempo
    if "Open time UTC" in df.columns:
        df["Open time UTC"] = pd.to_datetime(df["Open time UTC"], utc=True)
        df = df.sort_values("Open time UTC").reset_index(drop=True)

    return df


# ============================
# Cargar todos los símbolos
# ============================
def load_all_symbols(symbols=None):
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

    result = {}
    for sym in symbols:
        result[sym] = load_symbol_df(sym)

    return result
