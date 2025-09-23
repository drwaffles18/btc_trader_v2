# signal_tracker.py

import json
import os
from pathlib import Path
from typing import Dict, Any

# Permite setear por env; por defecto apunta a /data (volume en Railway)
ARCHIVO_ESTADO = os.getenv("STATE_PATH", "/data/ultima_senal.json")

def cargar_estado_anterior() -> Dict[str, Any]:
    p = Path(ARCHIVO_ESTADO)
    if p.exists():
        try:
            with open(p, "r") as f:
                data = json.load(f)
                # Estructura esperada: { "BTCUSDT": {"signal":"BUY","last_close_ms": 1727040000000}, ... }
                return data if isinstance(data, dict) else {}
        except Exception:
            return {}
    return {}

def guardar_estado_actual(estado: Dict[str, Any]) -> None:
    p = Path(ARCHIVO_ESTADO)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(estado, f)
