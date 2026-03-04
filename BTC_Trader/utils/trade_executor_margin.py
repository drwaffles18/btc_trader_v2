# =============================================================
# utils/trade_executor_margin_exec.py
# 🟣 Cross Margin Executor (BNB-only) — SAFE IMPORT
# -------------------------------------------------------------
# - NO calls Binance en import
# - Usa get_client() SOLO dentro
# - Ban-guard para -1003
# - Para equity BTC→USDT usa btc_price de context (Sheets) si existe
# =============================================================

import os
import time
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

from utils.google_client import get_gsheet_client
from utils.binance_session import get_client  # <- tu sesión ya creada

# =============================================================
# 0) ENV
# =============================================================

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

TRADE_SYMBOL = (os.getenv("TRADE_SYMBOL") or "BNBUSDT").strip().upper()
TRADE_WEIGHT = float(os.getenv("TRADE_WEIGHT", "1.0"))
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))
MIN_MARGIN_LEVEL  = float(os.getenv("MIN_MARGIN_LEVEL", "1.50"))

BINANCE_NOTIONAL_FLOOR = float(os.getenv("BINANCE_NOTIONAL_FLOOR", "5.0"))
SAFE_NOTIONAL_FACTOR   = float(os.getenv("SAFE_NOTIONAL_FACTOR", "0.9995"))

GSHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()

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
# 2) SHEETS Trades (lazy-safe)
# =============================================================

_ws_trades = None

def _get_ws_trades():
    global _ws_trades
    if _ws_trades is not None:
        return _ws_trades
    if DRY_RUN:
        return None
    if not GSHEET_ID:
        print("⚠️ [MARGIN] GOOGLE_SHEET_ID no definido → sin logging a Sheets", flush=True)
        return None
    try:
        gs = get_gsheet_client()
        _ws_trades = gs.open_by_key(GSHEET_ID).worksheet("Trades")
        return _ws_trades
    except Exception as e:
        print(f"⚠️ [MARGIN] No pude abrir worksheet Trades: {e}", flush=True)
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
        row.get("trade_mode", "MARGIN"),
    ]]
    try:
        ws.append_rows(values, value_input_option="RAW")
    except Exception as e:
        print(f"⚠️ [MARGIN] append_rows falló: {e}", flush=True)

# =============================================================
# 3) Helpers numéricos
# =============================================================

def _round_usdt_2(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("1.00"), rounding=ROUND_DOWN))

def _round_6(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN))

# =============================================================
# 4) BINANCE MARGIN HELPERS (solo en ejecución)
# =============================================================

def _get_margin_account(client) -> Dict[str, Any]:
    return client.get_margin_account()

def _get_margin_level(client) -> float:
    acc = _get_margin_account(client)
    assets = float(acc.get("totalAssetOfBtc", 0) or 0)
    liab   = float(acc.get("totalLiabilityOfBtc", 0) or 0)
    return 99.0 if liab == 0 else assets / liab

def _get_margin_free_usdt(client) -> float:
    acc = _get_margin_account(client)
    for a in acc.get("userAssets", []):
        if a.get("asset") == "USDT":
            return float(a.get("free", 0) or 0)
    return 0.0

def _get_margin_free_asset(client, asset: str) -> float:
    acc = _get_margin_account(client)
    for a in acc.get("userAssets", []):
        if a.get("asset") == asset:
            return float(a.get("free", 0) or 0)
    return 0.0

def _get_margin_equity_usdt(client, btc_price_from_context: Optional[float]) -> float:
    """
    Equity en USDT = totalNetAssetOfBtc * BTCUSDT.
    Para NO pedir precio a Binance, usamos btc_price_from_context (Sheets).
    """
    acc = _get_margin_account(client)
    btc_equity = float(acc.get("totalNetAssetOfBtc", 0) or 0)

    if btc_price_from_context and btc_price_from_context > 0:
        return btc_equity * float(btc_price_from_context)

    # Fallback (si no pasas btc_price): usar ticker Binance (1 request)
    t = client.get_symbol_ticker(symbol="BTCUSDT")
    btc_price = float(t.get("price", 0) or 0)
    return btc_equity * btc_price

def _get_symbol_filters(client, symbol: str) -> Dict[str, float]:
    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info.get("filters", [])}
    lot = filters.get("LOT_SIZE", {}) or {}
    min_notional = filters.get("MIN_NOTIONAL", {}) or {}
    step = float(lot.get("stepSize", 0) or 0)
    mn   = float(min_notional.get("minNotional", BINANCE_NOTIONAL_FLOOR) or BINANCE_NOTIONAL_FLOOR)
    return {"step": step, "min_notional": mn}

def _round_step(value: float, step: float) -> float:
    if step == 0:
        return value
    dec_val = Decimal(str(value))
    dec_step = Decimal(str(step))
    rounded = (dec_val // dec_step) * dec_step
    # aproximación decimales
    precision = int(round(-math.log(step, 10), 0)) if step < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)

def _borrow_usdt_if_needed(client, required_usdt: float) -> Dict[str, Any]:
    free = _get_margin_free_usdt(client)
    missing = max(0.0, required_usdt - free)

    print(f"💳 [MARGIN] free_usdt={free:.6f} required={required_usdt:.6f} missing={missing:.6f}", flush=True)

    if missing <= 0.0:
        return {"status": "NO_BORROW"}

    missing_clean = _round_6(missing)

    if DRY_RUN:
        print(f"💤 [MARGIN] DRY_RUN borrow USDT {missing_clean}", flush=True)
        return {"status": "DRY_RUN_BORROW", "amount": missing_clean}

    return client.create_margin_loan(asset="USDT", amount=str(missing_clean))

def _repay_all_usdt(client) -> Dict[str, Any]:
    acc = _get_margin_account(client)
    borrowed = 0.0
    interest = 0.0
    for a in acc.get("userAssets", []):
        if a.get("asset") == "USDT":
            borrowed = float(a.get("borrowed", 0) or 0)
            interest = float(a.get("interest", 0) or 0)
            break

    debt = borrowed + interest
    if debt <= 0:
        return {"status": "NO_DEBT"}

    debt_clean = _round_6(debt)
    print(f"💰 [MARGIN] repay USDT debt={debt_clean:.6f}", flush=True)

    if DRY_RUN:
        return {"status": "DRY_RUN_REPAY", "debt": debt_clean}

    return client.repay_margin_loan(asset="USDT", amount=str(debt_clean))

def _margin_buy_quote(client, symbol: str, quote_usdt: float) -> Dict[str, Any]:
    if DRY_RUN:
        return {"status": "DRY_RUN", "cummulativeQuoteQty": quote_usdt, "executedQty": 0}

    return client.create_margin_order(
        symbol=symbol,
        side="BUY",
        type="MARKET",
        quoteOrderQty=str(_round_usdt_2(quote_usdt)),
        isIsolated="FALSE",
    )

def _margin_sell_qty(client, symbol: str, qty: float) -> Dict[str, Any]:
    if DRY_RUN:
        return {"status": "DRY_RUN", "executedQty": qty, "cummulativeQuoteQty": 0}

    return client.create_margin_order(
        symbol=symbol,
        side="SELL",
        type="MARKET",
        quantity=str(qty),
        isIsolated="FALSE",
    )

# =============================================================
# 5) ENTRYPOINT MARGIN
# =============================================================

def handle_margin_signal(symbol: str, side: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    context = context or {}
    symbol = (symbol or "").strip().upper()
    side   = (side or "").strip().upper()

    print(f"\n========== 🟣 MARGIN {side} {symbol} ==========", flush=True)

    # Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ [MARGIN] IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

    # Ban guard
    if _ban_active():
        print(f"⛔ [MARGIN] BANNED_ACTIVE until_ms={_BANNED_UNTIL_MS}", flush=True)
        return {"status": "BANNED", "until_ms": _BANNED_UNTIL_MS}

    # Obtener client
    try:
        client = get_client()
    except Exception as e:
        print(f"❌ [MARGIN] No pude obtener client: {e}", flush=True)
        return {"status": "NO_CLIENT", "error": str(e)}

    try:
        if side == "BUY":
            # 1) Guardrail risk
            mlevel = _get_margin_level(client)
            print(f"📊 [MARGIN] margin_level={mlevel:.2f} (min={MIN_MARGIN_LEVEL})", flush=True)
            if mlevel < MIN_MARGIN_LEVEL:
                return {"status": "RISK_MARGIN_LEVEL", "margin_level": mlevel}

            # 2) Equity (USDT) usando BTC price de context (Sheets)
            btc_price = context.get("btc_price", None)
            equity_usdt = _get_margin_equity_usdt(client, btc_price)

            if equity_usdt <= 0:
                return {"status": "NO_MARGIN_COLLATERAL", "equity_usdt": equity_usdt}

            base_target = equity_usdt * float(TRADE_WEIGHT)
            target_notional = base_target * float(MARGIN_MULTIPLIER)

            filters = _get_symbol_filters(client, symbol)
            min_required = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

            clean = _round_usdt_2(target_notional)
            safe  = _round_usdt_2(clean * float(SAFE_NOTIONAL_FACTOR))

            print(
                f"🧮 [MARGIN] equity={equity_usdt:.2f} base_target={base_target:.2f} "
                f"target≈{target_notional:.2f} clean={clean:.2f} safe={safe:.2f} min={min_required:.2f}",
                flush=True
            )

            if safe < min_required:
                return {"status": "TOO_SMALL", "safe": safe, "min_required": min_required}

            # 3) Borrow si hace falta
            borrow_res = _borrow_usdt_if_needed(client, safe)
            if str(borrow_res).startswith("{") is False:
                # create_margin_loan devuelve dict; aquí solo para evitar edge-cases
                pass

            # 4) Ejecutar BUY
            order = _margin_buy_quote(client, symbol, safe)

            trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"
            _append_trade_row({
                "trade_id": trade_id,
                "symbol": symbol,
                "side": "BUY",
                "qty": float(order.get("executedQty", 0) or 0),
                "entry_price": context.get("bnb_price", ""),
                "entry_time": datetime.utcnow().isoformat(),
                "exit_price": "",
                "exit_time": "",
                "profit_usdt": "",
                "status": "OPEN",
                "trade_mode": "MARGIN",
            })

            return {"status": "OK", "order": order}

        elif side == "SELL":
            asset = symbol.replace("USDT", "").strip()
            qty_avail = _get_margin_free_asset(client, asset)
            print(f"ℹ️ [MARGIN] {asset} free≈{qty_avail:.8f}", flush=True)
            if qty_avail <= 0:
                _repay_all_usdt(client)
                return {"status": "NO_POSITION_MARGIN"}

            filters = _get_symbol_filters(client, symbol)
            qty_clean = _round_step(qty_avail, filters["step"])

            if qty_clean <= 0:
                return {"status": "INVALID_QTY", "qty_avail": qty_avail, "qty_clean": qty_clean}

            order = _margin_sell_qty(client, symbol, qty_clean)

            # Repagar deuda
            _repay_all_usdt(client)

            return {"status": "OK", "order": order}

        return {"status": "IGNORED", "detail": "side inválido"}

    except Exception as e:
        _mark_banned_from_exception(e)
        print(f"❌ [MARGIN] Error ejecutando: {e}", flush=True)
        return {"status": "ERROR", "error": str(e), "banned_until_ms": _BANNED_UNTIL_MS or None}
