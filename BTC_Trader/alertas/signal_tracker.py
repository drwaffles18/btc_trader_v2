import json
from pathlib import Path

ARCHIVO_ESTADO = "ultima_senal.json"

def cargar_estado_anterior():
    if Path(ARCHIVO_ESTADO).exists():
        with open(ARCHIVO_ESTADO, "r") as f:
            return json.load(f)
    return {}

def guardar_estado_actual(estado):
    with open(ARCHIVO_ESTADO, "w") as f:
        json.dump(estado, f)
