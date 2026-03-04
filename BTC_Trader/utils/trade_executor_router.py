# =============================================================
# utils/trade_executor_router.py
# 🔀 Router universal de ejecución (Spot / Margin)
# -------------------------------------------------------------
# Objetivo:
# - Enrutar señales BUY/SELL hacia Spot o Margin según USE_MARGIN
# - En modo estricto, bloquear símbolos fuera de ALLOWED_SYMBOLS
# - NO inicializa Binance aquí (sin pings, sin requests)
#
# Entradas:
#   route_signal({
#       "symbol": "BNBUSDT",
#       "side": "BUY" | "SELL",
#       "context": { ... opcional ... }
#   })
# =============================================================

import os
from typing import Dict, Any

# =============================================================
# 1) ENV
# =============================================================

USE_MARGIN = os.getenv("USE_MARGIN", "false").lower() == "true"
DRY_RUN    = os.getenv("DRY_RUN", "false").lower() == "true"

TRADE_SYMBOL = (os.getenv("TRADE_SYMBOL") or "BNBUSDT").strip().upper()

env_allowed = (os.getenv("ALLOWED_SYMBOLS") or "").strip()
if env_allowed:
    ALLOWED_SYMBOLS = {s.strip().upper() for s in env_allowed.split(",") if s.strip()}
else:
    ALLOWED_SYMBOLS = {TRADE_SYMBOL}

# Asegurar que el TRADE_SYMBOL SIEMPRE esté permitido
ALLOWED_SYMBOLS.add(TRADE_SYMBOL)

STRICT_TRADE_SYMBOL    = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"
STRICT_ALLOWED_SYMBOLS = os.getenv("STRICT_ALLOWED_SYMBOLS", "false").lower() == "true"
STRICT_MODE = STRICT_TRADE_SYMBOL or STRICT_ALLOWED_SYMBOLS

print("==================================================", flush=True)
print(f"🔧 [Router] USE_MARGIN={USE_MARGIN} | DRY_RUN={DRY_RUN}", flush=True)
print(f"🎯 [Router] TRADE_SYMBOL={TRADE_SYMBOL}", flush=True)
print(
    f"🔒 [Router] ALLOWED_SYMBOLS={sorted(ALLOWED_SYMBOLS)} | STRICT_MODE={STRICT_MODE} "
    f"(STRICT_TRADE_SYMBOL={STRICT_TRADE_SYMBOL}, STRICT_ALLOWED_SYMBOLS={STRICT_ALLOWED_SYMBOLS})",
    flush=True
)
print("==================================================", flush=True)

# =============================================================
# 2) IMPORT EXECUTORS (sin side-effects)
#    IMPORTANTE: Estos módulos NO deben hacer Binance calls al import.
# =============================================================

from utils.trade_executor_spot import handle_spot_signal
from utils.trade_executor_margin_exec import handle_margin_signal

# =============================================================
# 3) HELPERS
# =============================================================

def _symbol_allowed(symbol: str) -> bool:
    if not symbol:
        return False

    s = symbol.strip().upper()

    if not STRICT_MODE:
        return True

    return s in ALLOWED_SYMBOLS

# =============================================================
# 4) ROUTER
# =============================================================

def route_signal(signal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Señal universal del bot.
    Espera:
      signal = {
        "symbol": "BNBUSDT",
        "side": "BUY" | "SELL",
        "context": {... opcional ...}
      }
    """
    # ---------------------------
    # A) DRY_RUN global
    # ---------------------------
    if DRY_RUN:
        print("🛑 [Router] GLOBAL DRY_RUN → trading desactivado", flush=True)
        return {"status": "DRY_RUN_BLOCKED"}

    # ---------------------------
    # B) Validación básica
    # ---------------------------
    side = (signal.get("side", "") or "").strip().upper()
    symbol = (signal.get("symbol", "") or "").strip().upper()
    context = signal.get("context", {}) or {}

    if side not in ("BUY", "SELL") or not symbol:
        return {"status": "IGNORED", "detail": "Signal inválida", "symbol": symbol, "side": side}

    # ---------------------------
    # C) Guardrail símbolos
    # ---------------------------
    if not _symbol_allowed(symbol):
        print(f"⛔ [Router] BLOCKED_SYMBOL → {symbol} (permitidos={sorted(ALLOWED_SYMBOLS)})", flush=True)
        return {
            "status": "BLOCKED_SYMBOL",
            "symbol": symbol,
            "allowed": sorted(ALLOWED_SYMBOLS),
            "detail": "Symbol no permitido por configuración"
        }

    # ---------------------------
    # D) Enrutamiento
    # ---------------------------
    if USE_MARGIN:
        print(f"🟣 [Router] MARGIN → {side} {symbol}", flush=True)
        return handle_margin_signal(symbol=symbol, side=side, context=context)

    print(f"🟢 [Router] SPOT → {side} {symbol}", flush=True)
    return handle_spot_signal(symbol=symbol, side=side, context=context)
