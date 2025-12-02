# =============================================================
# 🔀 Router universal de ejecución de trades (Spot / Margin)
# Victor + GPT — 2025
# -------------------------------------------------------------
# - Decide dinámicamente si usar Spot o Margin
# - Basado en la variable de entorno USE_MARGIN
# - Totalmente compatible con:
#       * trade_executor_v2 (SPOT)
#       * trade_executor_margin (MARGIN)
# -------------------------------------------------------------
# - No modifica los ejecutores existentes.
# - El bot solo debe importar: route_signal()
# =============================================================

import os

# =============================================================
# 1) Variable de entorno
# =============================================================
USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"
print(f"🔧 [Router] USE_MARGIN = {USE_MARGIN}")

# =============================================================
# 2) Importar ejecutores reales
# =============================================================

# ---------- SPOT Executor ----------
try:
    from utils.trade_executor_v2 import (
        handle_buy_signal as spot_buy,
        handle_sell_signal as spot_sell,
    )
    SPOT_READY = True
except Exception as e:
    print(f"❌ [Router] Error importando Spot executor: {e}")
    SPOT_READY = False


# ---------- MARGIN Executor ----------
try:
    from utils.trade_executor_margin import (
        handle_margin_buy_signal as margin_buy,
        handle_margin_sell_signal as margin_sell,
    )
    MARGIN_READY = True
except Exception as e:
    print(f"⚠️ [Router] Margin executor NO disponible aún: {e}")
    MARGIN_READY = False


# =============================================================
# 3) Router principal
# =============================================================

def route_signal(signal: dict):
    """
    Señal universal del bot:
    - En BUY llama al buy correcto (spot/margin)
    - En SELL llama al sell correcto
    """

    side = signal.get("side", "").upper()
    symbol = signal.get("symbol")

    if not symbol or side not in ["BUY", "SELL"]:
        return {"status": "IGNORED", "detail": "Signal inválida"}

    # ---------------------------------------------------------
    # 🟣 MODO MARGIN
    # ---------------------------------------------------------
    if USE_MARGIN:
        if not MARGIN_READY:
            print("⚠️ [Router] USE_MARGIN=True pero margin executor no está disponible → usando SPOT")
        else:
            print(f"🟣 [Router] Ejecutando vía MARGIN → {side} {symbol}")

            if side == "BUY":
                return margin_buy(symbol)
            elif side == "SELL":
                return margin_sell(symbol)

    # ---------------------------------------------------------
    # 🟢 MODO SPOT (seguro por defecto)
    # ---------------------------------------------------------
    if not SPOT_READY:
        return {"status": "ERROR", "detail": "Spot executor no disponible"}

    print(f"🟢 [Router] Ejecutando vía SPOT → {side} {symbol}")

    if side == "BUY":
        return spot_buy(symbol)
    elif side == "SELL":
        return spot_sell(symbol)

    return {"status": "IGNORED", "detail": "side no soportado"}
