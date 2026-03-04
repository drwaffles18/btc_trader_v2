# =============================================================
# 🔀 Router universal de ejecución de trades (Spot / Margin)
# Victor + GPT — BNB-only hardening
# -------------------------------------------------------------
# - Decide dinámicamente si usar Spot o Margin (USE_MARGIN)
# - BLOQUEA símbolos no permitidos (ALLOWED_SYMBOLS / TRADE_SYMBOL)
# - Soporta modo estricto vía:
#       * STRICT_TRADE_SYMBOL (Railway actual)
#       * STRICT_ALLOWED_SYMBOLS (opcional legacy)
# - Compatible con:
#       * utils.trade_executor_v2 (SPOT)
#       * utils.trade_executor_margin (MARGIN)
# =============================================================
# utils/trade_executor_router.py
import os

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

ALLOWED_SYMBOLS.add(TRADE_SYMBOL)

STRICT_TRADE_SYMBOL    = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"
STRICT_ALLOWED_SYMBOLS = os.getenv("STRICT_ALLOWED_SYMBOLS", "false").lower() == "true"
STRICT_MODE = STRICT_TRADE_SYMBOL or STRICT_ALLOWED_SYMBOLS

print(f"🔧 [Router] USE_MARGIN={USE_MARGIN} | DRY_RUN={DRY_RUN}", flush=True)
print(f"🎯 [Router] TRADE_SYMBOL={TRADE_SYMBOL}", flush=True)
print(f"🔒 [Router] ALLOWED_SYMBOLS={sorted(ALLOWED_SYMBOLS)} | STRICT_MODE={STRICT_MODE} "
      f"(STRICT_TRADE_SYMBOL={STRICT_TRADE_SYMBOL}, STRICT_ALLOWED_SYMBOLS={STRICT_ALLOWED_SYMBOLS})", flush=True)

# =============================================================
# 2) IMPORTS (NO lazy imports)
#    Importar está OK siempre que los módulos NO llamen Binance en import-time.
# =============================================================
try:
    from utils.trade_executor_v2 import handle_buy_signal as spot_buy, handle_sell_signal as spot_sell
    SPOT_READY = True
except Exception as e:
    print(f"❌ [Router] Spot executor import error: {e}", flush=True)
    SPOT_READY = False

try:
    from utils.trade_executor_margin import handle_margin_buy_signal as margin_buy, handle_margin_sell_signal as margin_sell
    MARGIN_READY = True
except Exception as e:
    print(f"⚠️ [Router] Margin executor import error: {e}", flush=True)
    MARGIN_READY = False

# =============================================================
# 3) GUARDS
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
def route_signal(signal: dict):
    """
    Input:
      signal = {"symbol": "BNBUSDT", "side": "BUY"|"SELL", "ctx": {...} (opcional)}
    """
    side = (signal.get("side") or "").strip().upper()
    symbol = (signal.get("symbol") or "").strip().upper()

    # --- Global dry run ---
    if DRY_RUN:
        print("🛑 [Router] DRY_RUN → trading bloqueado", flush=True)
        return {"status": "DRY_RUN_BLOCKED"}

    # --- Validación básica ---
    if side not in ("BUY", "SELL") or not symbol:
        return {"status": "IGNORED", "detail": "Signal inválida", "symbol": symbol, "side": side}

    # --- Guardrails de símbolo ---
    if not _symbol_allowed(symbol):
        print(f"⛔ [Router] BLOCKED_SYMBOL → {symbol} (permitidos={sorted(ALLOWED_SYMBOLS)})", flush=True)
        return {"status": "BLOCKED_SYMBOL", "symbol": symbol, "allowed": sorted(ALLOWED_SYMBOLS)}

    # --- Margin preferido si está habilitado ---
    if USE_MARGIN and MARGIN_READY:
        print(f"🟣 [Router] MARGIN → {side} {symbol}", flush=True)
        return margin_buy(symbol, ctx=signal.get("ctx")) if side == "BUY" else margin_sell(symbol, ctx=signal.get("ctx"))

    # --- Fallback a spot ---
    if not SPOT_READY:
        return {"status": "ERROR", "detail": "Spot executor no disponible"}

    if USE_MARGIN and not MARGIN_READY:
        print("⚠️ [Router] USE_MARGIN=True pero margin no disponible → fallback SPOT", flush=True)

    print(f"🟢 [Router] SPOT → {side} {symbol}", flush=True)
    return spot_buy(symbol, ctx=signal.get("ctx")) if side == "BUY" else spot_sell(symbol, ctx=signal.get("ctx"))
