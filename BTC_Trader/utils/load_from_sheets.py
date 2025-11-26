import pandas as pd
from utils.google_client import get_gsheet_client
import os

SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

def load_symbol_df(symbol: str):
    client = get_gsheet_client()
    sh = client.open_by_key(SHEET_ID)

    ws = sh.worksheet(symbol)

    data = ws.get_all_records()

    if not data:
        raise RuntimeError(f"❌ La hoja {symbol} está vacía en Google Sheets.")

    df = pd.DataFrame(data)

    # Convertir tiempos
    df["Open time"] = pd.to_datetime(df["Open time"], errors="coerce")
    df["Close time"] = pd.to_datetime(df["Close time"], errors="coerce")

    # Convertir numéricos
    numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
    for c in numeric_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ordenar correctamente
    df = df.sort_values("Open time").reset_index(drop=True)

    return df

def append_trade_row(ws, row_dict):
    """
    Agrega una fila al final de la hoja Trades.
    row_dict = {
        "trade_id": ...,
        "symbol": ...,
        "side": ...,
        "qty": ...,
        "entry_price": ...,
        "entry_time": ...,
        "exit_price": ...,
        "exit_time": ...,
        "profit_usdt": ...,
        "status": ...
    }
    """
    values = [[row_dict.get(c, "") for c in [
        "trade_id",
        "symbol",
        "side",
        "qty",
        "entry_price",
        "entry_time",
        "exit_price",
        "exit_time",
        "profit_usdt",
        "status"
    ]]]

    next_row = len(ws.col_values(1)) + 1
    ws.update(range_name=f"A{next_row}:J{next_row}", values=values)
