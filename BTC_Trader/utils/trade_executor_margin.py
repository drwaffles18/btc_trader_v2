# =============================================================
# 🟢 Binance Spot Autotrader — Victor + GPT (BNB-only)
# -------------------------------------------------------------
# - BUY → Market buy usando quoteOrderQty
# - SELL → Market sell full balance
# - Logs en CSV y Google Sheets
# - Columna trade_mode = "spot" en Google Sheets
#
# ✅ BNB-only guardrail:
#    - Solo ejecuta trades si symbol == TRADE_SYMBOL (default BNBUSDT)
#    - Sizing usa TRADE_WEIGHT (porcentaje del equity) en vez de PORTFOLIO multi-asset
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import pandas as pd

from utils.google_client import get_gsheet_client

# -----------------------------
# 0) CONFIGURACIÓN GENERAL
# -----------------------------

os.makedirs("/app/data", exist_ok=True)
LOG_FILE = "/app/data/trade_log.csv"

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ✅ Single-asset mode
TRADE_SYMBOL = os.getenv("TRADE_SYMBOL", "BNBUSDT").upper()
TRADE_WEIGHT = float(os.getenv("TRADE_WEIGHT", "1.0"))  # 1.0 = 100% equity (capado por free_usdt)
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

try:
    from binance.client import Client
except ImportError:
    Client = None

API_KEY    = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

BINANCE_NOTIONAL_FLOOR = 5.0  # mínimo real para market orders

# -----------------------------
# 1) INICIALIZACIÓN BINANCE
# -----------------------------

BINANCE_ENABLED = False
client = None

if not API_KEY or not API_SECRET or Client is None:
    print("⚠️ No hay claves Binance. Modo solo alertas.")
else:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print(f"✅ Cliente Binance inicializado (SPOT). TRADE_SYMBOL={TRADE_SYMBOL} TRADE_WEIGHT={TRADE_WEIGHT}")
    except Exception as e:
        print(f"❌ Error al iniciar Binance: {e}")
        pd.DataFrame([{
            "timestamp": datetime.utcnow().isoformat(),
            "action": "BINANCE_INIT_ERROR",
            "message": str(e),
            "dry_run": DRY_RUN
        }]).to_csv(LOG_FILE, mode="a", header=not os.path.exists(LOG_FILE), index=False)
        print("→ Continuando sin trading real.")

# -----------------------------
# 2) GOOGLE SHEETS (Trades)
# -----------------------------

GSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
gs_client = get_gsheet_client()
ws_trades = gs_client.open_by_key(GSHEET_ID).worksheet("Trades")


def append_trade_row(ws, row_dict):
    """
    Inserta fila nueva en la pestaña Trades.
    Formato esperado de columnas:
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
    11) trade_mode  ("spot" / "margin")
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
        row_dict.get("trade_mode", "spot"),
    ]
    ws.append_row(row, value_input_option="RAW")

# -----------------------------
# 3) UTILITARIOS
# -----------------------------

def _append_log(row):
    df = pd.DataFrame([row])
    df.to_csv(LOG_FILE, mode="a", header=not os.path.exists(LOG_FILE), index=False)
    print(f"🧾 LOG → {row.get('action')} {row.get('symbol')} (DRY_RUN={DRY_RUN})")


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
    if not BINANCE_ENABLED:
        return {"step_size": 0.000001, "min_notional": 0, "tick_size": 0.01}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}
    lot = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    price_filter = filters.get("PRICE_FILTER", {})

    return {
        "step_size": float(lot.get("stepSize", 0)),
        "tick_size": float(price_filter.get("tickSize", 0)),
        "min_notional": float(min_notional.get("minNotional", 0)) if min_notional else None,
    }


def _get_free_balance(asset):
    if not BINANCE_ENABLED:
        return 1000.0 if asset == "USDT" else 0.0
    for b in client.get_account()["balances"]:
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _get_price(symbol):
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


def _get_spot_equity_usdt():
    if not BINANCE_ENABLED:
        return 1000.0
    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acc["balances"]}
    total = balances.get("USDT", 0)
    for asset, qty in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD") or qty <= 0:
            continue
        symbol = f"{asset}USDT"
        try:
            price = _get_price(symbol)
            total += qty * price
        except Exception:
            pass
    return total

# -----------------------------
# 4) MARKET BUY
# -----------------------------

def place_market_buy_by_quote(symbol, usdt_amount):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick_size"]))
    amt_dec = Decimal(str(usdt_amount))
    usdt_clean = float((amt_dec // tick) * tick)

    if DRY_RUN:
        price = _get_price(symbol)
        qty = usdt_clean / price if price > 0 else 0.0
        return {"symbol": symbol, "status": "FILLED", "executedQty": qty, "cummulativeQuoteQty": usdt_clean, "price": price}

    return client.create_order(
        symbol=symbol,
        side="BUY",
        type="MARKET",
        quoteOrderQty=str(usdt_clean)
    )

# -----------------------------
# 5) MARKET SELL
# -----------------------------

def sell_all_market(symbol):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    asset = symbol.replace("USDT", "")
    qty = _get_free_balance(asset)
    if qty <= 0:
        return {"status": "NO_POSITION"}

    filters = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty, filters["step_size"])

    if DRY_RUN:
        return {"symbol": symbol, "status": "SIMULATED", "qty": qty_clean}

    return client.create_order(
        symbol=symbol,
        side="SELL",
        type="MARKET",
        quantity=str(qty_clean)
    )

# -----------------------------
# 6) BUY SIGNAL
# -----------------------------

def handle_buy_signal(symbol):
    """BUY Market (quoteOrderQty) + logging a Sheets (BNB-only)."""
    try:
        symbol = (symbol or "").upper()

        # ✅ Guardrail single-asset
        if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
            print(f"⛔ SPOT IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}")
            return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

        if not BINANCE_ENABLED:
            print(f"⚠️ BUY SKIPPED (no keys) {symbol}")
            return {"status": "DISABLED"}

        # === 1) cuánto invertir (porcentaje del equity) ===
        equity = _get_spot_equity_usdt()
        free_usdt = _get_free_balance("USDT")

        usdt_to_spend = min(equity * TRADE_WEIGHT, free_usdt)

        filters = _get_symbol_filters(symbol)
        min_notional = filters["min_notional"] or BINANCE_NOTIONAL_FLOOR
        min_required = max(min_notional, BINANCE_NOTIONAL_FLOOR)

        if usdt_to_spend < min_required:
            print(f"❌ USDT insuficiente para {symbol}: {usdt_to_spend:.2f} < {min_required:.2f}")
            return {"status": "INSUFFICIENT_USDT", "free_usdt": free_usdt, "equity": equity}

        print(f"🟢 SPOT BUY {symbol} por {usdt_to_spend:.2f} USDT (TRADE_WEIGHT={TRADE_WEIGHT})")

        # === 2) ejecutar BUY ===
        order = place_market_buy_by_quote(symbol, usdt_to_spend)

        executed_qty = float(order.get("executedQty", 0))
        quote_spent = float(order.get("cummulativeQuoteQty", usdt_to_spend))

        if executed_qty > 0:
            entry_price = quote_spent / executed_qty
        else:
            entry_price = _get_price(symbol)

        qty = executed_qty
        trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

        # === 3) log csv ===
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "BUY",
            "usdt_spent": usdt_to_spend,
            "entry_price": entry_price,
            "qty": qty,
            "dry_run": DRY_RUN
        })

        # === 4) insert Sheets ===
        append_trade_row(ws_trades, {
            "trade_id": trade_id,
            "symbol": symbol,
            "side": "BUY",
            "qty": qty,
            "entry_price": entry_price,
            "entry_time": datetime.utcnow().isoformat(),
            "exit_price": "",
            "exit_time": "",
            "profit_usdt": "",
            "status": "OPEN",
            "trade_mode": "spot",
        })

        return order

    except Exception as e:
        print(f"❌ Error BUY {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": (symbol or ""),
            "action": "ERROR_BUY",
            "message": str(e),
            "dry_run": DRY_RUN
        })
        return {"status": "ERROR", "detail": str(e)}

# -----------------------------
# 7) SELL SIGNAL
# -----------------------------

def handle_sell_signal(symbol):
    try:
        symbol = (symbol or "").upper()

        # ✅ Guardrail single-asset
        if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
            print(f"⛔ SPOT IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}")
            return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

        if not BINANCE_ENABLED:
            print(f"⚠️ SELL SKIPPED (no keys) {symbol}")
            return {"status": "DISABLED"}

        print(f"🔴 SPOT SELL {symbol}")
        sell_res = sell_all_market(symbol)

        sell_price = float(_get_price(symbol))

        trades = ws_trades.get_all_records()
        open_trades = [t for t in trades if t.get("symbol") == symbol and t.get("status") == "OPEN"]

        if not open_trades:
            print("⚠️ No hay trades abiertos para cerrar.")
            return sell_res

        last = open_trades[-1]
        row_idx = trades.index(last) + 2  # header + index 0

        entry_price = float(last["entry_price"])
        qty = float(last["qty"])
        profit = (sell_price - entry_price) * qty

        # G: exit_price, H: exit_time, I: profit_usdt, J: status, K: trade_mode
        ws_trades.update(
            f"G{row_idx}:K{row_idx}",
            [[
                sell_price,
                datetime.utcnow().isoformat(),
                profit,
                "CLOSED",
                "spot"
            ]]
        )

        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "SELL",
            "sell_price": sell_price,
            "profit_usdt": profit,
            "qty": qty,
            "dry_run": DRY_RUN
        })

        return sell_res

    except Exception as e:
        print(f"❌ Error SELL {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": (symbol or ""),
            "action": "ERROR_SELL",
            "message": str(e),
            "dry_run": DRY_RUN
        })
        return {"status": "ERROR", "detail": str(e)}
