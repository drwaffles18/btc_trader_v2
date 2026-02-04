# =============================================================
# 🔀 Router universal de ejecución de trades (Spot / Margin)
# Victor + GPT — BNB-only hardening
# -------------------------------------------------------------
# - Decide dinámicamente si usar Spot o Margin (USE_MARGIN)
# - BLOQUEA símbolos no permitidos (ALLOWED_SYMBOLS / TRADE_SYMBOL)
# - Compatible con:
#       * utils.trade_executor_v2 (SPOT)
#       * utils.trade_executor_margin (MARGIN)
# =============================================================

import os

# =============================================================
# 1) Variables de entorno
# =============================================================

USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"
DRY_RUN    = os.getenv("DRY_RUN", "false").lower() == "true"

# ✅ Nuevo: control de símbolos permitidos
TRADE_SYMBOL = os.getenv("TRADE_SYMBOL", "BNBUSDT").upper()

# Si ALLOWED_SYMBOLS no existe, se usa solo TRADE_SYMBOL.
# Ejemplos:
#   ALLOWED_SYMBOLS=BNBUSDT
#   ALLOWED_SYMBOLS=BNBUSDT,BTCUSDT  (si un día quisieras permitir más)
env_allowed = os.getenv("ALLOWED_SYMBOLS", "").strip()
if env_allowed:
    ALLOWED_SYMBOLS = {s.strip().upper() for s in env_allowed.split(",") if s.strip()}
else:
    ALLOWED_SYMBOLS = {TRADE_SYMBOL}

# Para “modo estricto”: si true, bloquea cualquier symbol fuera de ALLOWED_SYMBOLS
STRICT_ALLOWED_SYMBOLS = os.getenv("STRICT_ALLOWED_SYMBOLS", "true").lower() == "true"

print(f"🔧 [Router] USE_MARGIN={USE_MARGIN} | DRY_RUN={DRY_RUN}", flush=True)
print(f"🔒 [Router] ALLOWED_SYMBOLS={sorted(ALLOWED_SYMBOLS)} | STRICT_ALLOWED_SYMBOLS={STRICT_ALLOWED_SYMBOLS}", flush=True)

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
    print(f"❌ [Router] Error importando Spot executor: {e}", flush=True)
    SPOT_READY = False

# ---------- MARGIN Executor ----------
try:
    from utils.trade_executor_margin import (
        handle_margin_buy_signal as margin_buy,
        handle_margin_sell_signal as margin_sell,
    )
    MARGIN_READY = True
except Exception as e:
    print(f"⚠️ [Router] Margin executor NO disponible aún: {e}", flush=True)
    MARGIN_READY = False

# =============================================================
# 3) Helpers
# =============================================================

def _symbol_allowed(symbol: str) -> bool:
    if not symbol:
        return False
    symbol = symbol.upper()
    if not STRICT_ALLOWED_SYMBOLS:
        return True
    return symbol in ALLOWED_SYMBOLS

# =============================================================
# 4) Router principal
# =============================================================

def route_signal(signal: dict):
    """
    Señal universal del bot:
    - Valida símbolo permitido
    - En BUY llama al buy correcto (spot/margin)
    - En SELL llama al sell correcto
    """

    side = (signal.get("side", "") or "").upper()
    symbol = (signal.get("symbol", "") or "").upper()

    # ---------------------------------------------------------
    # 🛑 GLOBAL DRY_RUN
    # ---------------------------------------------------------
    if DRY_RUN:
        print("🛑 [Router] GLOBAL DRY_RUN → trading, alerts y sheets DESACTIVADOS", flush=True)
        return {"status": "DRY_RUN_BLOCKED"}

    # ---------------------------------------------------------
    # ✅ Validación básica
    # ---------------------------------------------------------
    if not symbol or side not in ["BUY", "SELL"]:
        return {"status": "IGNORED", "detail": "Signal inválida", "symbol": symbol, "side": side}

    # ---------------------------------------------------------
    # 🔒 Bloqueo de símbolos no permitidos
    # ---------------------------------------------------------
    if not _symbol_allowed(symbol):
        print(f"⛔ [Router] BLOCKED_SYMBOL → {symbol} (permitidos={sorted(ALLOWED_SYMBOLS)})", flush=True)
        return {
            "status": "BLOCKED_SYMBOL",
            "symbol": symbol,
            "allowed": sorted(ALLOWED_SYMBOLS),
            "detail": "Symbol no permitido por configuración"
        }

    # ---------------------------------------------------------
    # 🟣 MODO MARGIN
    # ---------------------------------------------------------
    if USE_MARGIN:
        if not MARGIN_READY:
            print("⚠️ [Router] USE_MARGIN=True pero margin executor no está disponible → usando SPOT", flush=True)
        else:
            print(f"🟣 [Router] Ejecutando vía MARGIN → {side} {symbol}", flush=True)
            if side == "BUY":
                return margin_buy(symbol)
            else:
                return margin_sell(symbol)

    # ---------------------------------------------------------
    # 🟢 MODO SPOT (seguro por defecto)
    # ---------------------------------------------------------
    if not SPOT_READY:
        return {"status": "ERROR", "detail": "Spot executor no disponible"}

    print(f"🟢 [Router] Ejecutando vía SPOT → {side} {symbol}", flush=True)
    if side == "BUY":
        return spot_buy(symbol)
    else:
        return spot_sell(symbol)
