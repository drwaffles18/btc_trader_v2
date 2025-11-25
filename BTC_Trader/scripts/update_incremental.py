import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# Importar función de Binance
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.binance_fetch import get_binance_5m_data_between

# Símbolos
SYMBOLS = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

# Credenciales
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


# ===========================================================
# Función para leer la última vela desde Google Sheets
# ===========================================================
def get_last_open_time_ms(ws):
    """
    Obtiene el valor UTC de la última vela en la hoja.
    Retorna last_close_time_ms (int)
    """
    data = ws.get_all_records()
    if len(data) == 0:
        return None

    df = pd.DataFrame(data)

    # Asegurar columna existente
    if "Open time UTC" not in df.columns:
        raise RuntimeError(f"La hoja {ws.title} no tiene la columna 'Open time UTC'")

    last_ts = pd.to_datetime(df["Open time UTC"].iloc[-1], utc=True)
    return int(last_ts.timestamp() * 1000)


# ===========================================================
# Función para agregar nuevas velas al Google Sheet
# ===========================================================
def append_new_rows(ws, df_new):
    """
    Agrega nuevas velas al Google Sheet sin duplicar
    """
    if df_new.empty:
        print(f"   → No hay velas nuevas.")
        return

    # Convertir a listas de strings
    values = df_new.astype(str).values.tolist()

    # Append al final del sheet
    ws.append_rows(values, value_input_option="RAW")

    print(f"   ✓ {len(df_new)} velas nuevas agregadas.")


# ===========================================================
# MAIN DEL INCREMENTAL
# ===========================================================
def main():
    print("\n🔄 === INCREMENTAL 5M ===\n")

    # Hora actual del servidor se obtiene dentro de get_binance_5m_data_between()
    for symbol in SYMBOLS:
        print(f"\n➡️ Procesando {symbol}…")

        try:
            ws = sh.worksheet(symbol)
        except Exception as e:
            print(f"❌ No existe la hoja {symbol}: {e}")
            continue

        # 1. Obtener la última hora
        last_open_ms = get_last_open_time_ms(ws)

        if last_open_ms is None:
            print(f"❌ La hoja {symbol} no tiene datos. Debe correr initialize_history_total.py primero.")
            continue

        # Convertir a fecha string para usar en get_between
        last_dt = pd.to_datetime(last_open_ms, unit="ms", utc=True)
        start_dt_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")

        print(f"   Última vela en hoja: {start_dt_str} UTC")

        # 2. Obtener velas nuevas (después de la última vela)
        df_new = get_binance_5m_data_between(
            symbol,
            start_dt_str
        )

        # 3. Remover la primera fila (porque es la última ya existente)
        df_new = df_new.iloc[1:].copy()

        # 4. Append a Google Sheets
        append_new_rows(ws, df_new)

    print("\n🎉 Incremental completado.\n")


if __name__ == "__main__":
    main()
