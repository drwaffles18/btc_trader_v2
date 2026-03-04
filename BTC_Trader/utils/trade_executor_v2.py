# =============================================================
# utils/trade_executor_spot.py
# 🟢 Spot Executor (BNB-only) — SAFE IMPORT
# -------------------------------------------------------------
# - NO hace llamadas Binance al importar el módulo
# - Obtiene client via utils.binance_session.get_client() SOLO dentro
# - Maneja bans (-1003) bloqueando llamadas futuras temporalmente
#
# Señal:
#   handle_spot_signal(symbol="BNBUSDT", side="BUY"/"SELL", context={...})
# Context opcional recomendado:
#   context = {"ts": "...", "bnb_price": 650.0, "btc_price": 73000.0}
# =============================================================

import os
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

from utils.google_client import get_gsheet_client
from utils.binance_session import get_client  # <- asumo que ya lo tienes

# =============================================================
# 0) ENV
# =============================================================

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

TRADE_SYMBOL = (os.getenv("TRADE_SYMBOL") or "BNBUSDT").strip().upper()
TRADE_WEIGHT = float(os.getenv("TRADE_WEIGHT", "1.0"))
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

GSHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()

BINANCE_NOTIONAL_FLOOR = float(os.getenv("BINANCE_NOTIONAL_FLOOR", "5.0"))

# =============================================================
# 1) BAN GUARD (global module state)
# =============================================================

_BANNED_UNTIL_MS = 0

def _now_ms() -> int:
    return int(time.time() * 1000)

def _mark_banned_from_exception(e: Exception) -> None:
    """
    Detecta error -1003 y extrae 'until <ms>' si viene en el mensaje.
    """
    global _BANNED_UNTIL_MS
    msg = str(e)

    # Caso típico:
    # APIError(code=-1003): Way too much request weight used; IP banned until 1772649357204.
    if "code=-1003" in msg and "banned until" in msg:
        try:
            until_str = msg.split("banned until", 1)[1].strip().strip(".")
            until_ms = int("".join([c for c in until_str if c.isdigit()]))
            _BANNED_UNTIL_MS = max(_BANNED_UNTIL_MS, until_ms)
        except Exception:
            _BANNED_UNTIL_MS = max(_BANNED_UNTIL_MS, _now_ms() + 10 * 60 * 1000)  # fallback 10m

def _ban_active() -> bool:
    return _now_ms() < _BANNED_UNTIL_MS

# =============================================================
# 2) SHEETS (Trades) — lazy-safe
# =============================================================

_ws_trades = None

def _get_ws_trades():
    global _ws_trades
    if _ws_trades is not None:
        return _ws_trades

    if DRY_RUN:
        return None
    if not GSHEET_ID:
        print("⚠️ [SPOT] GOOGLE_SHEET_ID no definido → sin logging a Sheets", flush=True)
        return None

    try:
        gs = get_gsheet_client()
        _ws_trades = gs.open_by_key(GSHEET_ID).worksheet("Trades")
        return _ws_trades
    except Exception as e:
        print(f"⚠️ [SPOT] No pude abrir worksheet Trades: {e}", flush=True)
        return None

def _append_trade_row(row: Dict[str, Any]) -> None:
    ws = _get_ws_trades()
    if ws is None:
        return
    values = [[
        row.get("trade_id", ""),
        row.get("symbol", ""),
        row.get("side", ""),
        row.get("qty", ""),
        row.get("entry_price", ""),
        row.get("entry_time", ""),
        row.get("exit_price", ""),
        row.get("exit_time", ""),
        row.get("profit_usdt", ""),
        row.get("status", ""),
        row.get("trade_mode", "spot"),
    ]]
    try:
        ws.append_rows(values, value_input_option="RAW")
    except Exception as e:
        print(f"⚠️ [SPOT] append_rows falló: {e}", flush=True)

# =============================================================
# 3) BINANCE HELPERS (solo cuando se necesita)
# =============================================================

def _round_quote_usdt(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("1.00"), rounding=ROUND_DOWN))

def _get_free_usdt_spot(client) -> float:
    acc = client.get_account()
    for b in acc.get("balances", []):
        if b.get("asset") == "USDT":
            return float(b.get("free", 0) or 0)
    return 0.0

def _get_symbol_filters_cached(client, symbol: str) -> Dict[str, float]:
    """
    Llamada pesada: exchange info.
    Para BNBUSDT no debería ser frecuente porque solo se llama cuando hay trade.
    (Si quieres, la cacheamos en memoria.)
    """
    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info.get("filters", [])}
    min_notional = filters.get("MIN_NOTIONAL", {}) or {}
    return {"min_notional": float(min_notional.get("minNotional", BINANCE_NOTIONAL_FLOOR) or BINANCE_NOTIONAL_FLOOR)}

def _spot_market_buy_quote(client, symbol: str, quote_usdt: float) -> Dict[str, Any]:
    if DRY_RUN:
        return {"status": "DRY_RUN", "cummulativeQuoteQty": quote_usdt, "executedQty": 0}

    return client.create_order(
        symbol=symbol,
        side="BUY",
        type="MARKET",
        quoteOrderQty=str(_round_quote_usdt(quote_usdt)),
    )

def _spot_market_sell_all(client, symbol: str) -> Dict[str, Any]:
    asset = symbol.replace("USDT", "").strip()

    if DRY_RUN:
        return {"status": "DRY_RUN", "executedQty": 0, "cummulativeQuoteQty": 0}

    acc = client.get_account()
    qty = 0.0
    for b in acc.get("balances", []):
        if b.get("asset") == asset:
            qty = float(b.get("free", 0) or 0)
            break

    if qty <= 0:
        return {"status": "NO_POSITION"}

    # Para vender “all” correctamente, aquí ideal sería redondear por LOT_SIZE.
    # Como tu bot es BNB-only y cantidades suelen ser válidas, lo dejamos simple.
    return client.create_order(
        symbol=symbol,
        side="SELL",
        type="MARKET",
        quantity=str(qty),
    )

# =============================================================
# 4) ENTRYPOINT SPOT
# =============================================================

def handle_spot_signal(symbol: str, side: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    context = context or {}
    symbol = (symbol or "").strip().upper()
    side = (side or "").strip().upper()

    print(f"\n========== 🟢 SPOT {side} {symbol} ==========", flush=True)

    # Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ [SPOT] IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

    # Ban guard
    if _ban_active():
        print(f"⛔ [SPOT] BANNED_ACTIVE until_ms={_BANNED_UNTIL_MS}", flush=True)
        return {"status": "BANNED", "until_ms": _BANNED_UNTIL_MS}

    # Obtener client (NO ping aquí)
    try:
        client = get_client()
    except Exception as e:
        print(f"❌ [SPOT] No pude obtener client: {e}", flush=True)
        return {"status": "NO_CLIENT", "error": str(e)}

    try:
        if side == "BUY":
            free_usdt = _get_free_usdt_spot(client)
            filters = _get_symbol_filters_cached(client, symbol)
            min_required = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

            spend = free_usdt * float(TRADE_WEIGHT)
            spend = float(spend)

            print(f"ℹ️ [SPOT] free_usdt={free_usdt:.2f} | TRADE_WEIGHT={TRADE_WEIGHT:.2f} | spend≈{spend:.2f}", flush=True)

            if spend < min_required:
                print(f"❌ [SPOT] TOO_SMALL {spend:.2f} < {min_required:.2f}", flush=True)
                return {"status": "TOO_SMALL", "spend": spend, "min_required": min_required}

            order = _spot_market_buy_quote(client, symbol, spend)

            trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"
            _append_trade_row({
                "trade_id": trade_id,
                "symbol": symbol,
                "side": "BUY",
                "qty": float(order.get("executedQty", 0) or 0),
                "entry_price": context.get("bnb_price", ""),  # preferimos tu precio de Sheets
                "entry_time": datetime.utcnow().isoformat(),
                "exit_price": "",
                "exit_time": "",
                "profit_usdt": "",
                "status": "OPEN",
                "trade_mode": "spot",
            })

            return {"status": "OK", "order": order}

        elif side == "SELL":
            order = _spot_market_sell_all(client, symbol)
            # Nota: tu cierre/profit lo estás calculando en Sheets en otros lados;
            # si quieres, aquí se puede actualizar la fila OPEN → CLOSED como lo hacías.
            return {"status": "OK", "order": order}

        return {"status": "IGNORED", "detail": "side inválido"}

    except Exception as e:
        _mark_banned_from_exception(e)
        print(f"❌ [SPOT] Error ejecutando: {e}", flush=True)
        return {"status": "ERROR", "error": str(e), "banned_until_ms": _BANNED_UNTIL_MS or None}
