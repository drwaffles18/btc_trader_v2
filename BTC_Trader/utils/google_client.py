import os
import json
import gspread
from google.oauth2.service_account import Credentials

# === Cargar credenciales del environment ===
def get_gsheet_client():
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        raise RuntimeError("❌ GOOGLE_SERVICE_ACCOUNT_JSON no está configurado en Railway.")

    info = json.loads(raw_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    return client
