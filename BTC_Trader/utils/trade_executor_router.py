# =============================================================
# 🔀 Router de ejecución de trades (Spot / Margin)
# Victor + GPT — 2025
# -------------------------------------------------------------
# - Decide dinámicamente si usar Spot o Margin
# - Basado en variable de entorno USE_MARGIN
# - No modifica los ejecutores existentes
# =============================================================

import os

# -----------------------------
# 1) Variable de entorno
# -----------------------------
# Si NO está definida → False (modo seguro)
USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"

# -----------------------------
# 2) Importar ejecutores
# -----------------------------
# Executor Spot (actual y estable)
from utils.trade_executor_v2 import route_signal as spot_route_signal

# Executor Margin (lo construiremos luego)
try:
    from utils.trade_executor_margin import route_signal as margin_route_signal
except Exception:
    margin_route_signal = None


# -----------------------------
# 3) Router Principal
# -----------------------------
def route_signal(signal):
    """
    Router universal:
    - Si USE_MARGIN=True → usa Margin
    - Si USE_MARGIN=False → usa Spot
    """

    if USE_MARGIN:
        if margin_route_signal is None:
            print("⚠️ USE_MARGIN=True pero executor_margin no está listo → usando spot.")
            return spot_route_signal(signal)

        print("🔵 Router: ejecutando via MARGIN")
        return margin_route_signal(signal)

    # Caso normal (seguro)
    print("🟢 Router: ejecutando via SPOT")
    return spot_route_signal(signal)
