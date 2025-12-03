# =============================================================
# 🟣 Binance Cross Margin Autotrader — Opción B (todo en Margin)
# -------------------------------------------------------------
# - Todo el capital vive en Cross Margin.
# - Spot solo se usa como "bolsa secundaria" si existiera algo ahí,
#   pero el flujo normal es 100% Margin.
# - Lógica:
#       * Equity base = equity en Margin (totalAssetOfBtc * BTCUSDT)
#       * trade_notional = equity_base * weight * MARGIN_MULTIPLIER
#       * Si falta USDT libre en Margin → borrow USDT
#       * BUY en Cross Margin (MARKET)
#       * SELL:
#           - Vende qty del trade
#           - Calcula profit
#           - Repaga toda la deuda USDT
#           - NO transfiere nada a Spot
#       * Sheets:
#           - Columna 11 = trade_mode ("MARGIN")
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

import pandas as pd

from utils.google_client import get_gsheet_client

try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None

# =============================================================
# 0) CONFIG GENERAL
# =============================================================

API_KEY = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Multiplicador de tamaño vs equity base (ej. 3x)
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Mínimo por trade
BINANCE_NOTIONAL_FLOOR = 5.0

PORTFOLIO_WEIGHTS = {
    "BTCUSDT": 0.35,
    "ETHUSDT": 0.25,
    "ADAUSDT": 0.10,
    "XRPUSDT": 0.20,
    "BNBUSDT": 0.10,
}

client = None
BINANCE_ENABLED = False

if API_KEY and API_SECRET and Client:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print("✅ Margin Client OK (initialization successful)")
    except Exception as e:
        print(f"❌ Error Margin Client: {e}")
else:
    print("⚠️ Margin Client disabled (no API keys)")


# =============================================================
# 1) GOOGLE SHEETS
# =============================================================

GSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
gs_client = get_gsheet_client()
ws_trades = gs_client.open_by_key(GSHEET_ID).worksheet("Trades")


def append_trade_row_margin(ws, row_dict):
    """
    Inserta trade margin en la tabla Trades.
    Columnas esperadas:
    1) trade_id
    2) symbol
    3) side
    4) qty
    5) entry_price
    6) entry_time
    7) exit_price
    8) exit_time
    9) profit_usdt
    10) status
    11) trade_mode ("SPOT" / "MARGIN")
    """
    row = [
        row_dict["trade_id"],
        row_dict["symbol"],
        row_dict["side"],
        row_dict["qty"],
        row_dict["entry_price"],
        row_dict["entry_time"],
        row_dict["exit_price"],
        row_dict["exit_time"],
        row_dict["profit_usdt"],
        row_dict["status"],
        row_dict.get("trade_mode", "MARGIN"),
    ]
    ws.append_row(row, value_input_option="RAW")


# =============================================================
# 2) UTILS GENERALES
# =============================================================

def _round_step_size(value, step_size):
    if step_size == 0:
        return value
    dec_val = Decimal(str(value))
    dec_step = Decimal(str(step_size))
    rounded = (dec_val // dec_step) * dec_step
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)


def _get_symbol_filters(symbol):
    """Obtiene LOT_SIZE, TICK_SIZE y MIN_NOTIONAL aproximado."""
    if not BINANCE_ENABLED:
        return {"step": 0.000001, "tick": 0.01, "min_notional": 5.0}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}

    lot = filters.get("LOT_SIZE", {})
    min_not = filters.get("MIN_NOTIONAL", {})
    price = filters.get("PRICE_FILTER", {})

    return {
        "step": float(lot.get("stepSize", 0)),
        "tick": float(price.get("tickSize", 0)),
        "min_notional": float(min_not.get("minNotional", 0)) if min_not else 5.0,
    }


def _get_price(symbol):
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


# =============================================================
# 3) HELPERS MARGIN
# =============================================================

def _get_margin_account():
    if not BINANCE_ENABLED:
        return {}
    return client.get_margin_account()


def _get_margin_equity_usdt():
    """
    Equity total en Margin, en USDT:
    totalAssetOfBtc * precio_BTCUSDT
    """
    if not BINANCE_ENABLED:
        return 1000.0
    acc = client.get_margin_account()
    total_asset_btc = float(acc.get("totalAssetOfBtc", 0.0))
    btc_price = _get_price("BTCUSDT") or 0.0
    return total_asset_btc * btc_price


def get_margin_level():
    """
    Margin Level = totalAssetOfBtc / totalLiabilityOfBtc
    """
    if not BINANCE_ENABLED:
        return 99.0
    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0.0))
    liability = float(acc.get("totalLiabilityOfBtc", 0.0))
    if liability <= 0:
        return 99.0
    return assets / liability


def get_total_borrow_used_ratio():
    """
    borrow_used_ratio = totalLiabilityOfBtc / totalAssetOfBtc
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0.0))
    liability = float(acc.get("totalLiabilityOfBtc", 0.0))
    if assets <= 0:
        return 1.0
    return liability / assets


def _get_margin_free_usdt():
    """USDT libre en la cuenta Margin."""
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            return float(a.get("free", 0.0))
    return 0.0


def _get_margin_debt_usdt():
    """Deuda de USDT en Margin (borrowed + interest)."""
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            borrowed = float(a.get("borrowed", 0.0))
            interest = float(a.get("interest", 0.0))
            return borrowed + interest
    return 0.0


def borrow_if_needed(usdt_needed: float):
    """
    Pide prestado USDT si usdt_needed > 0.
    """
    if usdt_needed <= 0:
        return {"status": "NO_BORROW"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN borrow USDT por {usdt_needed:.4f}")
        return {"status": "DRY_RUN", "asset": "USDT", "amount": usdt_needed}

    try:
        res = client.create_margin_loan(asset="USDT", amount=str(usdt_needed))
        print(f"🟣 Borrow ejecutado USDT {usdt_needed:.4f}: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR borrow USDT: {e}")
        return {"error": str(e)}


def _repay_all_usdt_debt():
    """Repaga toda la deuda de USDT en Margin."""
    debt = _get_margin_debt_usdt()
    if debt <= 0:
        print("ℹ️ No hay deuda USDT que repagar.")
        return {"status": "NO_DEBT"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN repay USDT {debt:.4f}")
        return {"status": "DRY_RUN", "asset": "USDT", "amount": debt}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=str(debt))
        print(f"💰 Repay USDT debt ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repaying USDT: {e}")
        return {"error": str(e)}


# =============================================================
# 4) ORDERS MARGIN
# =============================================================

def place_margin_buy(symbol: str, usdt_amount: float):
    """Market BUY en Cross Margin usando quoteOrderQty."""
    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        qty = usdt_amount / price if price > 0 else 0.0
        print(f"💤 DRY_RUN margin BUY {symbol} notional={usdt_amount:.4f} qty≈{qty:.6f}")
        return {
            "symbol": symbol,
            "status": "FILLED",
            "executedQty": qty,
            "cummulativeQuoteQty": usdt_amount,
            "price": price,
        }

    try:
        res = client.create_margin_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quoteOrderQty=str(usdt_amount),
            isIsolated="FALSE"
        )
        print(f"🟣 Margin BUY ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR margin buy: {e}")
        return {"error": str(e)}


def place_margin_sell(symbol: str, qty: float):
    """Market SELL en Cross Margin."""
    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        print(f"💤 DRY_RUN margin SELL {symbol} qty={qty:.6f} price≈{price}")
        return {
            "symbol": symbol,
            "status": "FILLED",
            "executedQty": qty,
            "cummulativeQuoteQty": qty * price,
            "price": price,
        }

    try:
        res = client.create_margin_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            quantity=str(qty),
            isIsolated="FALSE"
        )
        print(f"🟣 Margin SELL ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR margin sell: {e}")
        return {"error": str(e)}


# =============================================================
# 5) HANDLE BUY SIGNAL (Opción B)
# =============================================================

def handle_margin_buy_signal(symbol: str):
    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    # 1. Equity en Margin como base
    margin_equity = _get_margin_equity_usdt()
    free_margin_usdt = _get_margin_free_usdt()
    weight = PORTFOLIO_WEIGHTS.get(symbol, 0.0)

    base_target = margin_equity * weight
    trade_notional_raw = base_target * MARGIN_MULTIPLIER

    print(f"ℹ️ Margin equity ≈ {margin_equity:.2f} USDT | free Margin USDT ≈ {free_margin_usdt:.2f}")
    print(f"🧮 {symbol}: base_target ≈ {base_target:.2f} → trade_notional_raw ≈ {trade_notional_raw:.2f}")

    if trade_notional_raw < BINANCE_NOTIONAL_FLOOR:
        print(f"❌ Trade demasiado pequeño: {trade_notional_raw:.2f} < {BINANCE_NOTIONAL_FLOOR}")
        return {"status": "too_small"}

    # 2. Controles de riesgo
    mlevel = get_margin_level()
    if mlevel < 2.0:
        print(f"❌ MarginLevel peligroso: {mlevel}")
        return {"status": "risk_margin_level"}

    borrow_ratio = get_total_borrow_used_ratio()
    if borrow_ratio > 0.40:
        print(f"❌ Borrow ratio > 40%: {borrow_ratio:.4f}")
        return {"status": "risk_borrow_limit"}

    # 3. Normalizar notional a tick
    f = _get_symbol_filters(symbol)
    tick = Decimal(str(f["tick"]))

    trade_notional_clean = float((Decimal(str(trade_notional_raw)) // tick) * tick)
    min_required = max(f["min_notional"], BINANCE_NOTIONAL_FLOOR)

    print(f"🔧 Notional limpio (tick) ≈ {trade_notional_clean:.4f} USDT (min_required={min_required:.2f})")

    if trade_notional_clean < min_required:
        print(f"❌ Notional limpio < mín requerido.")
        return {"status": "below_min_notional"}

    # 4. Borrow si hace falta
    borrow_needed = max(0.0, trade_notional_clean - free_margin_usdt)
    print(f"→ free_margin_usdt ≈ {free_margin_usdt:.4f} | borrow_needed ≈ {borrow_needed:.4f}")

    if borrow_needed > 0:
        borrow_res = borrow_if_needed(borrow_needed)
        if "error" in borrow_res:
            print(f"❌ ERROR en borrow USDT, abort BUY: {borrow_res['error']}")
            return {"status": "borrow_failed", "detail": borrow_res["error"]}

    # 5. Ejecutar BUY Margin
    res = place_margin_buy(symbol, trade_notional_clean)
    if "error" in res:
        print(f"❌ Margin BUY falló: {res['error']}")
        return res

    executed_qty = float(res.get("executedQty", 0.0))
    quote_used = float(res.get("cummulativeQuoteQty", trade_notional_clean))
    if executed_qty > 0 and quote_used > 0:
        entry_price = quote_used / executed_qty
    else:
        entry_price = _get_price(symbol)

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    append_trade_row_margin(ws_trades, {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": "BUY",
        "qty": executed_qty,
        "entry_price": entry_price,
        "entry_time": datetime.utcnow().isoformat(),
        "exit_price": "",
        "exit_time": "",
        "profit_usdt": "",
        "status": "OPEN",
        "trade_mode": "MARGIN"
    })

    print("🟣 Margin BUY completado.")
    return res


# =============================================================
# 6) HANDLE SELL SIGNAL (Opción B)
# =============================================================

def handle_margin_sell_signal(symbol: str):
    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    # 1. Buscar trade abierto para este símbolo
    trades = ws_trades.get_all_records()
    open_trades = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

    if not open_trades:
        print("⚠️ No hay trades OPEN para cerrar en Sheets.")
        return {"status": "NO_OPEN_TRADES"}

    margin_trades = [t for t in open_trades if str(t.get("trade_mode", "")).upper() == "MARGIN"]
    if margin_trades:
        last = margin_trades[-1]
    else:
        last = open_trades[-1]

    qty = float(last["qty"])
    entry_price = float(last["entry_price"])
    row_idx = trades.index(last) + 2  # header + índice base 1

    if qty <= 0:
        print("⚠️ Qty del trade <= 0, abort SELL.")
        return {"status": "INVALID_QTY"}

    # 2. Ajustar qty por LOT_SIZE
    f = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty, f["step"])

    if qty_clean <= 0:
        print("⚠️ Qty limpia <= 0 después de LOT_SIZE, abort SELL.")
        return {"status": "INVALID_QTY_CLEAN"}

    # 3. Ejecutar SELL Margin
    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ Margin SELL falló, no se actualiza Sheets.")
        return sell_res

    executed_qty = float(sell_res.get("executedQty", qty_clean))
    quote_got = float(sell_res.get("cummulativeQuoteQty", 0.0))

    if executed_qty > 0 and quote_got > 0:
        sell_price = quote_got / executed_qty
    else:
        sell_price = _get_price(symbol)

    profit = (sell_price - entry_price) * qty_clean

    # 4. Repagar deuda USDT
    _repay_all_usdt_debt()

    # 5. Actualizar Sheets
    ws_trades.update(
        f"G{row_idx}:J{row_idx}",
        [[
            sell_price,
            datetime.utcnow().isoformat(),
            profit,
            "CLOSED"
        ]]
    )

    print(f"🔴 Margin SELL completado. Profit ≈ {profit:.4f} USDT")
    return sell_res
