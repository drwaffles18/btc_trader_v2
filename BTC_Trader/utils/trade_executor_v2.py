# =============================================================
# utils/trade_executor_v2.py
# 🟢 Spot Executor (BNB-only) — SAFE IMPORT
# -------------------------------------------------------------
# - NO hace llamadas Binance al importar el módulo
# - Obtiene client via utils.binance_session.get_client() SOLO dentro
# - Maneja bans (-1003) bloqueando llamadas futuras temporalmente
# - BUY crea fila OPEN en Trades
# - SELL cierra la última fila OPEN en Trades (no agrega fila nueva)
# =============================================================

import os
import time
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

from utils.google_client import get_gsheet_client
from utils.binance_session import get_client

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
# 1) BAN GUARD
# =============================================================

_BANNED_UNTIL_MS = 0

def _now_ms() -> int:
    return int(time.time() * 1000)

def _mark_banned_from_exception(e: Exception) -> None:
    global _BANNED_UNTIL_MS
    msg = str(e)

    if "code=-1003" in msg and "banned until" in msg:
        try:
            until_str = msg.split("banned until", 1)[1].strip().strip(".")
            until_ms = int("".join([c for c in until_str if c.isdigit()]))
            _BANNED_UNTIL_MS = max(_BANNED_UNTIL_MS, until_ms)
        except Exception:
            _BANNED_UNTIL_MS = max(_BANNED_UNTIL_MS, _now_ms() + 10 * 60 * 1000)

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
        row.get("trade_mode", "SPOT"),
    ]]
    try:
        ws.append_rows(values, value_input_option="RAW")
    except Exception as e:
        print(f"⚠️ [SPOT] append_rows falló: {e}", flush=True)

def _find_last_open_trade_row(symbol: str, trade_mode: str = "SPOT") -> Optional[Dict[str, Any]]:
    """
    Busca la última fila OPEN para symbol/trade_mode.
    Retorna:
      {
        "row_number": int,
        "trade_id": str,
        "qty": float,
        "entry_price": float,
        "entry_time": str,
      }
    """
    ws = _get_ws_trades()
    if ws is None:
        return None

    try:
        records = ws.get_all_records()
    except Exception as e:
        print(f"⚠️ [SPOT] get_all_records falló buscando OPEN trade: {e}", flush=True)
        return None

    if not records:
        return None

    for idx in range(len(records) - 1, -1, -1):
        r = records[idx]

        if (
            str(r.get("symbol", "")).strip().upper() == symbol.upper()
            and str(r.get("trade_mode", "")).strip().upper() == trade_mode.upper()
            and str(r.get("status", "")).strip().upper() == "OPEN"
        ):
            try:
                qty = float(r.get("qty", 0) or 0)
            except Exception:
                qty = 0.0

            try:
                entry_price = float(r.get("entry_price", 0) or 0)
            except Exception:
                entry_price = 0.0

            return {
                "row_number": idx + 2,  # records arranca en fila 2 real
                "trade_id": r.get("trade_id", ""),
                "qty": qty,
                "entry_price": entry_price,
                "entry_time": r.get("entry_time", ""),
            }

    return None

def _update_trade_close(
    row_number: int,
    exit_price: Optional[float],
    exit_time: str,
    profit_usdt: Optional[float],
    status: str = "CLOSED",
) -> None:
    """
    Actualiza columnas G:J en la fila OPEN existente:
      G exit_price
      H exit_time
      I profit_usdt
      J status
    """
    ws = _get_ws_trades()
    if ws is None:
        return

    values = [[
        "" if exit_price is None else exit_price,
        exit_time,
        "" if profit_usdt is None else profit_usdt,
        status,
    ]]

    try:
        ws.update(range_name=f"G{row_number}:J{row_number}", values=values)
    except Exception as e:
        print(f"⚠️ [SPOT] update close row falló en fila {row_number}: {e}", flush=True)

# =============================================================
# 3) HELPERS
# =============================================================

def _round_quote_usdt(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("1.00"), rounding=ROUND_DOWN))

def _round_6(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN))

def _round_step(value: float, step: float) -> float:
    if step == 0:
        return value
    dec_val = Decimal(str(value))
    dec_step = Decimal(str(step))
    rounded = (dec_val // dec_step) * dec_step
    precision = int(round(-math.log(step, 10), 0)) if step < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)

def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat()

def _make_trade_id(symbol: str) -> str:
    return f"{symbol}_{datetime.utcnow().timestamp()}"

def _extract_fill_price(order: Dict[str, Any], fallback_price: Optional[float] = None) -> Optional[float]:
    """
    Intenta sacar el precio real desde fills.
    Fallback:
      cummulativeQuoteQty / executedQty
      luego fallback_price
    """
    try:
        fills = order.get("fills", []) or []
        if fills:
            prices = []
            qtys = []
            for f in fills:
                p = float(f.get("price", 0) or 0)
                q = float(f.get("qty", 0) or 0)
                if p > 0 and q > 0:
                    prices.append(p)
                    qtys.append(q)

            if prices and qtys and sum(qtys) > 0:
                return sum(p * q for p, q in zip(prices, qtys)) / sum(qtys)
    except Exception:
        pass

    try:
        cq = float(order.get("cummulativeQuoteQty", 0) or 0)
        eq = float(order.get("executedQty", 0) or 0)
        if cq > 0 and eq > 0:
            return cq / eq
    except Exception:
        pass

    return fallback_price

def _result(
    status: str,
    executed: bool,
    order: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    trade_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    out = {
        "status": status,
        "executed": executed,
        "order": order,
        "error": error,
        "trade_id": trade_id,
    }
    if detail:
        out.update(detail)
    return out

# =============================================================
# 4) BINANCE HELPERS
# =============================================================

def _get_free_usdt_spot(client) -> float:
    acc = client.get_account()
    for b in acc.get("balances", []):
        if b.get("asset") == "USDT":
            return float(b.get("free", 0) or 0)
    return 0.0

def _get_free_asset_spot(client, asset: str) -> float:
    acc = client.get_account()
    for b in acc.get("balances", []):
        if b.get("asset") == asset:
            return float(b.get("free", 0) or 0)
    return 0.0

def _get_symbol_filters_cached(client, symbol: str) -> Dict[str, float]:
    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info.get("filters", [])}
    lot = filters.get("LOT_SIZE", {}) or {}
    min_notional = filters.get("MIN_NOTIONAL", {}) or {}

    step = float(lot.get("stepSize", 0) or 0)
    mn = float(min_notional.get("minNotional", BINANCE_NOTIONAL_FLOOR) or BINANCE_NOTIONAL_FLOOR)

    return {
        "step": step,
        "min_notional": mn,
    }

def _spot_market_buy_quote(client, symbol: str, quote_usdt: float) -> Dict[str, Any]:
    if DRY_RUN:
        return {"status": "DRY_RUN", "cummulativeQuoteQty": quote_usdt, "executedQty": 0}

    return client.create_order(
        symbol=symbol,
        side="BUY",
        type="MARKET",
        quoteOrderQty=str(_round_quote_usdt(quote_usdt)),
    )

def _spot_market_sell_qty(client, symbol: str, qty: float) -> Dict[str, Any]:
    if DRY_RUN:
        return {"status": "DRY_RUN", "executedQty": qty, "cummulativeQuoteQty": 0}

    return client.create_order(
        symbol=symbol,
        side="SELL",
        type="MARKET",
        quantity=str(qty),
    )

# =============================================================
# 5) ENTRYPOINT SPOT
# =============================================================

def handle_spot_signal(symbol: str, side: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    context = context or {}
    symbol = (symbol or "").strip().upper()
    side = (side or "").strip().upper()

    print(f"\n========== 🟢 SPOT {side} {symbol} ==========", flush=True)

    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ [SPOT] IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return _result("IGNORED_SYMBOL", executed=False, detail={"symbol": symbol, "trade_symbol": TRADE_SYMBOL})

    if _ban_active():
        print(f"⛔ [SPOT] BANNED_ACTIVE until_ms={_BANNED_UNTIL_MS}", flush=True)
        return _result("BANNED", executed=False, detail={"until_ms": _BANNED_UNTIL_MS})

    try:
        client = get_client()
        if client is None:
            return _result("NO_CLIENT", executed=False, error="get_client() returned None")
    except Exception as e:
        print(f"❌ [SPOT] No pude obtener client: {e}", flush=True)
        return _result("NO_CLIENT", executed=False, error=str(e))

    try:
        if side == "BUY":
            free_usdt = _get_free_usdt_spot(client)
            filters = _get_symbol_filters_cached(client, symbol)
            min_required = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

            spend = float(free_usdt * float(TRADE_WEIGHT))

            print(f"ℹ️ [SPOT] free_usdt={free_usdt:.2f} | TRADE_WEIGHT={TRADE_WEIGHT:.2f} | spend≈{spend:.2f}", flush=True)

            if spend < min_required:
                print(f"❌ [SPOT] TOO_SMALL {spend:.2f} < {min_required:.2f}", flush=True)
                return _result("TOO_SMALL", executed=False, detail={"spend": spend, "min_required": min_required})

            order = _spot_market_buy_quote(client, symbol, spend)

            trade_id = _make_trade_id(symbol)
            _append_trade_row({
                "trade_id": trade_id,
                "symbol": symbol,
                "side": "BUY",
                "qty": float(order.get("executedQty", 0) or 0),
                "entry_price": context.get("bnb_price", ""),
                "entry_time": _utcnow_iso(),
                "exit_price": "",
                "exit_time": "",
                "profit_usdt": "",
                "status": "OPEN",
                "trade_mode": "SPOT",
            })

            return _result("OK", executed=True, order=order, trade_id=trade_id)

        elif side == "SELL":
            asset = symbol.replace("USDT", "").strip()
            qty_avail = _get_free_asset_spot(client, asset)
            print(f"ℹ️ [SPOT] {asset} free≈{qty_avail:.8f}", flush=True)

            if qty_avail <= 0:
                return _result("NO_POSITION", executed=False)

            filters = _get_symbol_filters_cached(client, symbol)
            qty_clean = _round_step(qty_avail, filters["step"])

            if qty_clean <= 0:
                return _result(
                    "INVALID_QTY",
                    executed=False,
                    detail={"qty_avail": qty_avail, "qty_clean": qty_clean}
                )

            # Buscar trade OPEN antes de vender
            open_trade = _find_last_open_trade_row(symbol=symbol, trade_mode="SPOT")
            if open_trade is None:
                print(f"⚠️ [SPOT] No encontré trade OPEN para cerrar en Sheets ({symbol})", flush=True)

            # Ejecutar SELL real
            order = _spot_market_sell_qty(client, symbol, qty_clean)

            # Precio y tiempo reales de salida
            exit_price = _extract_fill_price(order, fallback_price=context.get("bnb_price"))
            exit_time = _utcnow_iso()

            profit_usdt = None
            trade_id_to_return = None

            if open_trade is not None:
                entry_price = float(open_trade.get("entry_price", 0) or 0)
                entry_qty = float(open_trade.get("qty", 0) or 0)
                trade_id_to_return = open_trade.get("trade_id")

                if exit_price is not None and entry_price > 0 and entry_qty > 0:
                    profit_usdt = (float(exit_price) - entry_price) * entry_qty

                _update_trade_close(
                    row_number=open_trade["row_number"],
                    exit_price=exit_price,
                    exit_time=exit_time,
                    profit_usdt=profit_usdt,
                    status="CLOSED",
                )
            else:
                # fallback extremo: dejar registro aparte
                fallback_trade_id = _make_trade_id(symbol)
                fallback_row = {
                    "trade_id": fallback_trade_id,
                    "symbol": symbol,
                    "side": "SELL",
                    "qty": float(order.get("executedQty", 0) or 0),
                    "entry_price": "",
                    "entry_time": "",
                    "exit_price": "" if exit_price is None else exit_price,
                    "exit_time": exit_time,
                    "profit_usdt": "" if profit_usdt is None else profit_usdt,
                    "status": "CLOSED_NO_OPEN_FOUND",
                    "trade_mode": "SPOT",
                }
                _append_trade_row(fallback_row)
                trade_id_to_return = fallback_trade_id

            return _result("OK", executed=True, order=order, trade_id=trade_id_to_return)

        return _result("IGNORED", executed=False, detail={"detail": "side inválido"})

    except Exception as e:
        _mark_banned_from_exception(e)
        print(f"❌ [SPOT] Error ejecutando: {e}", flush=True)
        return _result("ERROR", executed=False, error=str(e), detail={"banned_until_ms": _BANNED_UNTIL_MS or None})
