# =============================================================
# utils/trade_executor_margin.py
# 🟣 Cross Margin Executor (BNB-only) — ATOMIC / SAFE IMPORT
# -------------------------------------------------------------
# - NO calls Binance en import
# - Usa get_client() SOLO dentro
# - Ban-guard para -1003
# - Para equity BTC→USDT usa btc_price de context (Sheets) si existe
# - BUY robusto:
#     borrow -> poll balance -> buy con colchón -> log -> return canónico
# - Si BUY falla tras borrow, intenta repay inmediato
# - BUY crea fila OPEN en Trades
# - SELL cierra la última fila OPEN en Trades (no agrega fila nueva normal)
# =============================================================

import os
import time
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, Optional

from utils.google_client import get_gsheet_client
from utils.binance_session import get_client, get_last_init_error

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

POST_BORROW_BUY_BUFFER = float(os.getenv("POST_BORROW_BUY_BUFFER", "0.9975"))
POST_BORROW_POLL_TRIES = int(os.getenv("POST_BORROW_POLL_TRIES", "8"))
POST_BORROW_POLL_SLEEP = float(os.getenv("POST_BORROW_POLL_SLEEP", "0.75"))

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

def _find_last_open_trade_row(symbol: str, trade_mode: str = "MARGIN") -> Optional[Dict[str, Any]]:
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
        print(f"⚠️ [MARGIN] get_all_records falló buscando OPEN trade: {e}", flush=True)
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
                "row_number": idx + 2,
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
        print(f"⚠️ [MARGIN] update close row falló en fila {row_number}: {e}", flush=True)

# =============================================================
# 3) Helpers numéricos / resultado canónico
# =============================================================

def _round_usdt_2(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("1.00"), rounding=ROUND_DOWN))

def _round_6(x: float) -> float:
    return float(Decimal(str(x)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN))

def _utcnow_iso() -> str:
    return datetime.utcnow().isoformat()

def _make_trade_id(symbol: str) -> str:
    return f"{symbol}_{datetime.utcnow().timestamp()}"

def _trade_row_base(
    trade_id: str,
    symbol: str,
    side: str,
    context: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "qty": "",
        "entry_price": context.get("bnb_price", ""),
        "entry_time": _utcnow_iso(),
        "exit_price": "",
        "exit_time": "",
        "profit_usdt": "",
        "status": "",
        "trade_mode": "MARGIN",
    }

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
# 4) BINANCE MARGIN HELPERS
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
    acc = _get_margin_account(client)
    btc_equity = float(acc.get("totalNetAssetOfBtc", 0) or 0)

    if btc_price_from_context and btc_price_from_context > 0:
        return btc_equity * float(btc_price_from_context)

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
    precision = int(round(-math.log(step, 10), 0)) if step < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)

def _borrow_usdt_if_needed(client, required_usdt: float) -> Dict[str, Any]:
    free = _get_margin_free_usdt(client)
    missing = max(0.0, required_usdt - free)

    print(f"💳 [MARGIN] free_usdt={free:.6f} required={required_usdt:.6f} missing={missing:.6f}", flush=True)

    if missing <= 0.0:
        return {"status": "NO_BORROW", "amount": 0.0, "free_before": free}

    missing_clean = _round_6(missing)

    if DRY_RUN:
        print(f"💤 [MARGIN] DRY_RUN borrow USDT {missing_clean}", flush=True)
        return {"status": "DRY_RUN_BORROW", "amount": missing_clean, "free_before": free}

    res = client.create_margin_loan(asset="USDT", amount=str(missing_clean))
    return {
        "status": "BORROWED",
        "amount": missing_clean,
        "free_before": free,
        "response": res,
    }

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

def _wait_margin_free_usdt(client, min_required: float, tries: int, sleep_s: float) -> float:
    best = 0.0
    for i in range(1, tries + 1):
        free = _get_margin_free_usdt(client)
        best = max(best, free)
        print(f"⏳ [MARGIN] balance check {i}/{tries} → free_usdt={free:.6f} required={min_required:.6f}", flush=True)
        if free >= min_required:
            return free
        time.sleep(sleep_s)
    return best

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

    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ [MARGIN] IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return _result("IGNORED_SYMBOL", executed=False, detail={"symbol": symbol, "trade_symbol": TRADE_SYMBOL})

    if _ban_active():
        print(f"⛔ [MARGIN] BANNED_ACTIVE until_ms={_BANNED_UNTIL_MS}", flush=True)
        return _result("BANNED", executed=False, detail={"until_ms": _BANNED_UNTIL_MS})

    try:
        client = get_client()
        if client is None:
            init_err = get_last_init_error()
            print(f"❌ [MARGIN] get_client() returned None | init_err={init_err}", flush=True)
            return _result("NO_CLIENT", executed=False, error=f"get_client() returned None | init_err={init_err}")
    except Exception as e:
        print(f"❌ [MARGIN] No pude obtener client: {e}", flush=True)
        return _result("NO_CLIENT", executed=False, error=str(e))

    trade_id = _make_trade_id(symbol)
    borrowed = False

    try:
        if side == "BUY":
            base_row = _trade_row_base(trade_id, symbol, side, context)

            mlevel = _get_margin_level(client)
            print(f"📊 [MARGIN] margin_level={mlevel:.2f} (min={MIN_MARGIN_LEVEL})", flush=True)
            if mlevel < MIN_MARGIN_LEVEL:
                row = {**base_row, "status": f"REJECTED:RISK_MARGIN_LEVEL:{mlevel:.4f}"}
                _append_trade_row(row)
                return _result("RISK_MARGIN_LEVEL", executed=False, trade_id=trade_id, detail={"margin_level": mlevel})

            btc_price = context.get("btc_price", None)
            equity_usdt = _get_margin_equity_usdt(client, btc_price)
            if equity_usdt <= 0:
                row = {**base_row, "status": "REJECTED:NO_MARGIN_COLLATERAL"}
                _append_trade_row(row)
                return _result("NO_MARGIN_COLLATERAL", executed=False, trade_id=trade_id, detail={"equity_usdt": equity_usdt})

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
                row = {**base_row, "status": f"REJECTED:TOO_SMALL:{safe:.2f}"}
                _append_trade_row(row)
                return _result("TOO_SMALL", executed=False, trade_id=trade_id, detail={"safe": safe, "min_required": min_required})

            borrow_res = _borrow_usdt_if_needed(client, safe)
            borrowed = borrow_res.get("status") in ("BORROWED", "DRY_RUN_BORROW")

            free_after = _wait_margin_free_usdt(
                client,
                min_required=safe,
                tries=POST_BORROW_POLL_TRIES,
                sleep_s=POST_BORROW_POLL_SLEEP
            )

            buy_quote = _round_usdt_2(min(safe, free_after * float(POST_BORROW_BUY_BUFFER)))

            print(
                f"🧱 [MARGIN] free_after={free_after:.6f} "
                f"buy_quote={buy_quote:.2f} buffer={POST_BORROW_BUY_BUFFER}",
                flush=True
            )

            if buy_quote < min_required:
                if borrowed:
                    try:
                        _repay_all_usdt(client)
                    except Exception as repay_err:
                        print(f"⚠️ [MARGIN] repay tras BUY fallido (pre-order) falló: {repay_err}", flush=True)

                row = {**base_row, "status": f"ERROR:INSUFFICIENT_POST_BORROW_BALANCE:{buy_quote:.2f}"}
                _append_trade_row(row)
                return _result(
                    "ERROR",
                    executed=False,
                    error="Insufficient post-borrow balance",
                    trade_id=trade_id,
                    detail={"buy_quote": buy_quote, "min_required": min_required}
                )

            order = _margin_buy_quote(client, symbol, buy_quote)

            row = {
                **base_row,
                "qty": float(order.get("executedQty", 0) or 0),
                "entry_time": _utcnow_iso(),
                "status": "OPEN",
                "trade_mode": "MARGIN",
            }
            _append_trade_row(row)

            return _result(
                "OK",
                executed=True,
                order=order,
                trade_id=trade_id,
                detail={"buy_quote": buy_quote}
            )

        elif side == "SELL":
            asset = symbol.replace("USDT", "").strip()
            qty_avail = _get_margin_free_asset(client, asset)
            print(f"ℹ️ [MARGIN] {asset} free≈{qty_avail:.8f}", flush=True)

            if qty_avail <= 0:
                try:
                    _repay_all_usdt(client)
                except Exception as repay_err:
                    print(f"⚠️ [MARGIN] repay on NO_POSITION failed: {repay_err}", flush=True)
                return _result("NO_POSITION_MARGIN", executed=False, trade_id=None)

            filters = _get_symbol_filters(client, symbol)
            qty_clean = _round_step(qty_avail, filters["step"])

            if qty_clean <= 0:
                return _result(
                    "INVALID_QTY",
                    executed=False,
                    trade_id=None,
                    detail={"qty_avail": qty_avail, "qty_clean": qty_clean}
                )

            open_trade = _find_last_open_trade_row(symbol=symbol, trade_mode="MARGIN")
            if open_trade is None:
                print(f"⚠️ [MARGIN] No encontré trade OPEN para cerrar en Sheets ({symbol})", flush=True)

            order = _margin_sell_qty(client, symbol, qty_clean)

            try:
                _repay_all_usdt(client)
            except Exception as repay_err:
                print(f"⚠️ [MARGIN] repay tras SELL falló: {repay_err}", flush=True)

            exit_price = _extract_fill_price(order, fallback_price=context.get("bnb_price"))
            exit_time = _utcnow_iso()

            profit_usdt = None
            trade_id_to_return = None

            if open_trade is not None:
                entry_price = float(open_trade.get("entry_price", 0) or 0)
                entry_qty   = float(open_trade.get("qty", 0) or 0)
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
                    "trade_mode": "MARGIN",
                }
                _append_trade_row(fallback_row)
                trade_id_to_return = fallback_trade_id

            return _result("OK", executed=True, order=order, trade_id=trade_id_to_return)

        return _result("IGNORED", executed=False, detail={"detail": "side inválido"})

    except Exception as e:
        _mark_banned_from_exception(e)

        if side == "BUY" and borrowed:
            try:
                _repay_all_usdt(client)
            except Exception as repay_err:
                print(f"⚠️ [MARGIN] repay tras excepción BUY falló: {repay_err}", flush=True)

        print(f"❌ [MARGIN] Error ejecutando: {e}", flush=True)

        err_row = _trade_row_base(trade_id, symbol, side, context)
        err_row["status"] = f"ERROR:{str(e)[:180]}"
        err_row["trade_mode"] = "MARGIN"
        _append_trade_row(err_row)

        return _result(
            "ERROR",
            executed=False,
            error=str(e),
            trade_id=trade_id,
            detail={"banned_until_ms": _BANNED_UNTIL_MS or None}
        )
