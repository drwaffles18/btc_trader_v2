import os
import base64
import json
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


def get_credentials():
    """Decodifica las credenciales BASE64 desde Railway."""
    b64 = os.environ.get("GOOGLE_CREDS_BASE64")
    if not b64:
        raise ValueError("NO se encontró GOOGLE_CREDS_BASE64 en variables de entorno.")

    creds_json = json.loads(base64.b64decode(b64))
    creds = Credentials.from_service_account_info(creds_json)
    return creds


def read_sheet(sheet_id: str, sheet_name: str) -> pd.DataFrame:
    """Lee una hoja completa y la devuelve como DataFrame."""
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    result = service.spreadsheets().values().get(
        spreadsheetId=sheet_id,
        range=sheet_name
    ).execute()

    values = result.get("values", [])
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]

    return pd.DataFrame(rows, columns=header)


def write_sheet(sheet_id: str, sheet_name: str, df: pd.DataFrame):
    """Sobrescribe toda la hoja con un DataFrame."""
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    # Convertir DF a matriz para Sheets
    body = {
        "values": [df.columns.tolist()] + df.values.tolist()
    }

    service.spreadsheets().values().clear(
        spreadsheetId=sheet_id,
        range=sheet_name
    ).execute()

    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=sheet_name,
        valueInputOption="RAW",
        body=body
    ).execute()


def append_rows(sheet_id: str, sheet_name: str, rows: list):
    """Agrega nuevas filas al final de la hoja."""
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)

    body = {"values": rows}

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=sheet_name,
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()
