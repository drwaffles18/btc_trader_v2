# ==========================================================
# signal_tracker.py
# Estado persistente de señales (IRONCLAD)
# ==========================================================

import json
import os
from pathlib import Path
from typing import Dict, Any

# Permite setear por env; por defecto apunta a /data (volume en Railway)
ARCHIVO_ESTADO = os.getenv("STATE_PATH", "/data/ultima_senal.json")


# ----------------------------------------------------------
# Helpers
# ----------------------------------------------------------

def _estado_valido(data: Any) -> Dict[str, Dict[str, Any]]:
    """
    Valida y normaliza la estructura del estado.
    Esperado:
      {
        "BTCUSDT": {"signal": "BUY|SELL|None", "last_close_ms": int},
        ...
      }
    """
    if not isinstance(data, dict):
        return {}

    estado_limpio = {}
    for symbol, info in data.items():
        if not isinstance(symbol, str) or not isinstance(info, dict):
            continue

        signal = info.get("signal")
        last_close_ms = info.get("last_close_ms")

        if signal not in ("BUY", "SELL", None):
            signal = None

        try:
            last_close_ms = int(last_close_ms)
        except Exception:
            last_close_ms = 0

        estado_limpio[symbol] = {
            "signal": signal,
            "last_close_ms": last_close_ms
        }

    return estado_limpio


# ----------------------------------------------------------
# API pública
# ----------------------------------------------------------

def cargar_estado_anterior() -> Dict[str, Dict[str, Any]]:
    p = Path(ARCHIVO_ESTADO)
    if not p.exists():
        return {}

    try:
        with open(p, "r") as f:
            data = json.load(f)
        return _estado_valido(data)
    except Exception as e:
        # Nunca rompemos el bot por estado corrupto
        print(f"⚠️ Estado previo inválido, ignorando: {e}", flush=True)
        return {}


def guardar_estado_actual(estado: Dict[str, Dict[str, Any]]) -> None:
    """
    Guarda el estado de forma atómica para evitar archivos corruptos.
    """
    p = Path(ARCHIVO_ESTADO)
    p.parent.mkdir(parents=True, exist_ok=True)

    estado_limpio = _estado_valido(estado)

    tmp_path = p.with_suffix(".tmp")
    try:
        with open(tmp_path, "w") as f:
            json.dump(estado_limpio, f)
        tmp_path.replace(p)  # operación atómica en el filesystem
    except Exception as e:
        print(f"❌ Error guardando estado: {e}", flush=True)
