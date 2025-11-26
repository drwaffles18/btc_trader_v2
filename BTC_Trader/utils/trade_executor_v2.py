# =============================================================
# 🟢 Binance Spot Autotrader — Victor + GPT (versión estable sin OCO)
# -------------------------------------------------------------
# Objetivo:
# - BUY → Market buy usando quoteOrderQty (USDT)
# - SELL → Market sell del total del asset disponible
# - Respeta los pesos por símbolo
# - Log en /app/data/trade_log.csv
# =============================================================

# =============================================================
# 🟢 Binance Spot Autotrader — Victor + GPT (versión estable sin OCO)
# -------------------------------------------------------------
# Log interno en CSV + Log de trades en Google Sheets
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import pandas as pd

from utils.google_client import get_gsheet_client

# Crear carpeta de logs
os.makedirs("/app/data", exist_ok=True)

LOG_FILE = "/app/data/trade_log.csv"

# DRY RUN
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# =============================
# 0) Binance client
# =============================
try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None

API_KEY    = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

BINANCE_ENABLED = False
client = None

if API_KEY and API_SECRET and Client:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print("✅ Cliente Binance inicializado.")
    except Exception as e:
        print(f"⚠️ Error Binance: {e}")
else:
    print("⚠️ Binance desactivado. Modo solo alertas.")


# =============================
# 1) GOOGLE SHEETS — pestaña Trades
# =============================
GSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
client_sheets = get_gsheet_client()
sheet_trades = client_sheets.open_by_key(GSHEET_ID).worksheet("Trades")


# =============================================================
# UTILITARIOS
# =============================================================

def _append_log(row: dict):
    """Escribe en /app/data/trade_log.csv."""
    df = pd.DataFrame([row])
    df.to_csv(LOG_FILE, mode="a", header=not os.path.exists(LOG_FILE), index=False)
    print(f"🧾 LOG → {row.get('action')} {row.get('symbol')} (DRY_RUN={DRY_RUN})")


def append_trade_row(ws, row_dict):
    """
    Inserta fila completa en pestaña Trades de Google Sheets.
    Columnas estándar:
    A: trade_id
    B: symbol
    C: side
    D: qty
    E: entry_price
    F: entry_time
    G: exit_price
    H: exit_time
    I: profit_usdt
    J: status
    """
    columns = [
        "trade_id",
        "symbol",
        "side",
        "qty",
        "entry_price",
        "entry_time",
        "exit_price",
        "exit_time",
        "profit_usdt",
        "status"
    ]

    values = [[row_dict.get(col, "") for col in columns]]

    next_row = len(ws.col_values(1)) + 1
    ws.update(f"A{next_row}:J{next_row}", values)


def _round_step_size(value: float, step_size: float) -> float:
    if step_size == 0:
        return value
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    dec_val  = Decimal(str(value))
    dec_step = Decimal(str(step_size))
    rounded = (dec_val // dec_step) * dec_step
    if precision > 0:
        rounded = rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)
    else:
        rounded = rounded.quantize(Decimal("1"), rounding=ROUND_DOWN)
    return float(rounded)


def _get_symbol_filters(symbol: str):
    if not BINANCE_ENABLED:
        return {"step_size": 0.000001, "min_qty": 0.0, "tick_size": 0.01, "min_notional": 10.0}
    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}
    return {
        "step_size": float(filters["LOT_SIZE"]["stepSize"]),
        "min_qty": float(filters["LOT_SIZE"]["minQty"]),
        "tick_size": float(filters["PRICE_FILTER"]["tickSize"]),
        "min_notional": float(filters["MIN_NOTIONAL"]["minNotional"]),
    }


def _get_free_balance(asset: str) -> float:
    if not BINANCE_ENABLED:
        return 1000.0 if asset == "USDT" else 0.0
    for b in client.get_account()["balances"]:
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


def _get_spot_equity_usdt() -> float:
    """Equity = USDT + valoración de todas las criptos."""
    if not BINANCE_ENABLED:
        return 1000.0

    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acc["balances"]}
    total = balances.get("USDT", 0.0)

    for asset, qty in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD"):
            continue
        if qty > 0:
            symbol = f"{asset}USDT"
            try:
                price = _get_price(symbol)
                total += qty * price
            except:
                pass

    return total


# =============================================================
# ÓRDENES
# =============================================================

PORTFOLIO_WEIGHTS = {
    "BTCUSDT": 0.35,
    "ETHUSDT": 0.25,
    "ADAUSDT": 0.10,
    "XRPUSDT": 0.20,
    "BNBUSDT": 0.10,
}

def place_market_buy_by_quote(symbol, usdt_amount):
    """Market BUY usando quoteOrderQty."""
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick_size"]))
    amt  = Decimal(str(usdt_amount))
    usdt_clean = float((amt // tick) * tick)

    if DRY_RUN:
        price = _get_price(symbol)
        qty = usdt_clean / price
        return {"symbol": symbol, "status": "FILLED", "executedQty": qty, "price": price}

    return client.create_order(
        symbol=symbol,
        side=SIDE_BUY,
        type=ORDER_TYPE_MARKET,
        quoteOrderQty=str(usdt_clean)
    )


def sell_all_market(symbol):
    """Market SELL del balance completo."""
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    asset = symbol.replace("USDT", "")
    qty = _get_free_balance(asset)
    if qty <= 0:
        return {"status": "NO_POSITION"}

    filters = _get_symbol_filters(symbol)
    q = _round_step_size(qty, filters["step_size"])

    if DRY_RUN:
        price = _get_price(symbol)
        return {"symbol": symbol, "status": "SIMULATED", "qty": q, "price": price}

    return client.create_order(
        symbol=symbol,
        side=SIDE_SELL,
        type=ORDER_TYPE_MARKET,
        quantity=str(q)
    )


# =============================================================
# BUY
# =============================================================

def handle_buy_signal(symbol):
    """BUY + registrar entrada en Google Sheets."""
    try:
        if not BINANCE_ENABLED:
            print(f"⚠️ BUY SKIPPED {symbol} (no keys)")
            return

        equity = _get_spot_equity_usdt()
        free_usdt = _get_free_balance("USDT")
        weight = PORTFOLIO_WEIGHTS.get(symbol, 0)

        usdt_to_spend = min(equity * weight, free_usdt)

        filters = _get_symbol_filters(symbol)
        if usdt_to_spend < filters["min_notional"]:
            return {"status": "INSUFFICIENT_USDT"}

        print(f"🟢 BUY {symbol} por {usdt_to_spend:.2f} USDT…")
        order = place_market_buy_by_quote(symbol, usdt_to_spend)

        entry_price = float(order.get("price"))
        qty = float(order.get("executedQty"))

        # log interno
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "BUY",
            "usdt_spent": usdt_to_spend,
            "entry_price": entry_price,
            "qty": qty,
            "dry_run": DRY_RUN
        })

        # log google sheets
        trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

        append_trade_row(sheet_trades, {
            "trade_id": trade_id,
            "symbol": symbol,
            "side": "BUY",
            "qty": qty,
            "entry_price": entry_price,
            "entry_time": datetime.utcnow().isoformat(),
            "exit_price": "",
            "exit_time": "",
            "profit_usdt": "",
            "status": "OPEN"
        })

        return order

    except Exception as e:
        print(f"❌ Error BUY {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "ERROR_BUY",
            "message": str(e),
            "dry_run": DRY_RUN
        })


# =============================================================
# SELL
# =============================================================

def handle_sell_signal(symbol):
    """SELL + cierre del trade en Google Sheets."""
    try:
        if not BINANCE_ENABLED:
            print(f"⚠️ SELL SKIPPED {symbol} (no keys)")
            return

        print(f"🔴 SELL {symbol}…")
        res = sell_all_market(symbol)

        sell_price = float(res.get("price", 0))

        # log interno
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "SELL",
            "sell_price": sell_price,
            "dry_run": DRY_RUN
        })

        # buscar trade OPEN
        trades = sheet_trades.get_all_records()
        open_trades = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

        if not open_trades:
            print("⚠️ No hay trades abiertos.")
            return res

        last_trade = open_trades[-1]
        row_index = trades.index(last_trade) + 2  # +2 por header + base 1

        qty = float(last_trade["qty"])
        entry_price = float(last_trade["entry_price"])
        profit = (sell_price - entry_price) * qty

        # actualizar google sheets
        sheet_trades.update(f"G{row_index}", str(sell_price))
        sheet_trades.update(f"H{row_index}", datetime.utcnow().isoformat())
        sheet_trades.update(f"I{row_index}", str(profit))
        sheet_trades.update(f"J{row_index}", "CLOSED")

        return res

    except Exception as e:
        print(f"❌ Error SELL {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "ERROR_SELL",
            "message": str(e),
            "dry_run": DRY_RUN
        })


# =============================================================
# ROUTER
# =============================================================

def route_signal(signal: dict):
    side = signal.get("side", "").upper()
    symbol = signal.get("symbol")

    if side == "BUY":
        return handle_buy_signal(symbol)
    elif side == "SELL":
        return handle_sell_signal(symbol)
    else:
        return {"status": "IGNORED"}

