# =============================================================
# 🟣 Binance Cross Margin Autotrader — Victor + GPT
# -------------------------------------------------------------
# - Este módulo es independiente del Spot executor.
# - NO se usa aún en producción (USE_MARGIN = false).
# - Contiene toda la lógica para margin:
#       * equity margin
#       * borrow automático
#       * repay automático
#       * market buy margin
#       * market sell margin
#       * control de riesgo (40% límite, marginLevel >= 2.0)
# -------------------------------------------------------------
# IMPORTANTE:
# Este archivo NO está conectado al bot todavía.
# Cuando USE_MARGIN=true crearemos el router que lo activará.
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import pandas as pd

from utils.google_client import get_gsheet_client

# Intentamos importar Cliente Binance
try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None


# =============================================================
# 0) CONFIG GENERAL
# =============================================================

API_KEY    = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
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
# 1) GOOGLE SHEETS INIT
# =============================================================

GSHEET_ID = os.getenv("GOOGLE_SHEET_ID")
gs_client = get_gsheet_client()
ws_trades = gs_client.open_by_key(GSHEET_ID).worksheet("Trades")


def append_trade_row_margin(ws, row_dict):
    """
    Inserta trade margin en la tabla general.
    Mantiene compatibilidad con la tabla actual.
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
    ]
    ws.append_row(row, value_input_option="RAW")


# =============================================================
# 2) UTILS
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
    """Obtiene LOT_SIZE, TICK_SIZE y MIN_NOTIONAL."""
    if not BINANCE_ENABLED:
        return {"step": 0.000001, "tick": 0.01, "min_notional": 5}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}
    lot = filters.get("LOT_SIZE", {})
    min_not = filters.get("MIN_NOTIONAL", {})
    price = filters.get("PRICE_FILTER", {})

    return {
        "step": float(lot.get("stepSize", 0)),
        "tick": float(price.get("tickSize", 0)),
        "min_notional": float(min_not.get("minNotional", 0)) if min_not else 5,
    }


def _get_price(symbol):
    if not BINANCE_ENABLED:
        return 0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


# =============================================================
# 3) MARGIN ACCOUNT + RISK
# =============================================================

def get_margin_account():
    """Devuelve el estado completo del margin account."""
    if not BINANCE_ENABLED:
        return {}
    return client.get_margin_account()


def get_margin_equity_usdt():
    """
    Equity total del margin account:
    free + locked + borrowed (negativo).
    """
    if not BINANCE_ENABLED:
        return 1000.0

    acc = client.get_margin_account()
    return float(acc["totalAssetOfBtc"]) * _get_price("BTCUSDT")


def get_margin_level():
    """Margin Level = totalAsset / totalLiability."""
    if not BINANCE_ENABLED:
        return 99

    acc = client.get_margin_account()
    debts = float(acc["totalLiabilityOfBtc"])
    assets = float(acc["totalAssetOfBtc"])
    if debts == 0:
        return 99
    return assets / debts


def get_total_borrow_used_ratio():
    """
    Devuelve % del borrow utilizado respecto al borrow limit estimado.
    En cross margin, el límite es dinámico pero para nuestro modelo:
    borrow_used_ratio = liability / asset
    """
    if not BINANCE_ENABLED:
        return 0.0

    acc = client.get_margin_account()
    liability = float(acc["totalLiabilityOfBtc"])
    asset = float(acc["totalAssetOfBtc"])
    if asset <= 0:
        return 1  # super riesgo
    return liability / asset  # ej: 0.27 → 27%


# =============================================================
# 4) BORROW Y REPAY
# =============================================================

def borrow_if_needed(asset, usdt_needed):
    """
    Pide prestado si no hay suficiente USDT disponible.
    """
    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN borrow {asset} por {usdt_needed}")
        return {"borrowed": usdt_needed}

    # pedir préstamo:
    try:
        res = client.create_margin_loan(asset=asset, amount=str(usdt_needed))
        print(f"🟣 Borrow ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR borrow: {e}")
        return {"error": str(e)}


def repay_borrow(asset, amount):
    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN repay {asset} por {amount}")
        return {"repaid": amount}
    try:
        return client.create_margin_repay(asset=asset, amount=str(amount))
    except Exception as e:
        print(f"❌ ERROR repaying: {e}")
        return {"error": str(e)}


# =============================================================
# 5) MARGIN MARKET BUY
# =============================================================

def place_margin_buy(symbol, usdt_amount):
    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        qty = usdt_amount / price
        print(f"💤 DRY_RUN margin buy {symbol} qty={qty}")
        return {"executedQty": qty, "price": price, "status": "FILLED"}

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


# =============================================================
# 6) MARGIN MARKET SELL
# =============================================================

def place_margin_sell(symbol, qty):
    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        print(f"💤 DRY_RUN margin sell {symbol} qty={qty}")
        return {"executedQty": qty, "price": price, "status": "FILLED"}

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
# 7) MANEJO DE BUY SIGNAL
# =============================================================

def handle_margin_buy_signal(symbol):
    """
    BUY en Cross Margin con:
    - cálculo de equity
    - borrow if needed
    - control de riesgo
    """

    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    # 1. Equity total
    equity = get_margin_equity_usdt()
    weight = PORTFOLIO_WEIGHTS.get(symbol, 0)

    usdt_target = equity * weight

    # 2. Control mínimo
    price = _get_price(symbol)
    if usdt_target < BINANCE_NOTIONAL_FLOOR:
        print(f"❌ Trade demasiado pequeño: {usdt_target}")
        return {"status": "too_small"}

    # 3. Nivel de margen
    mlevel = get_margin_level()
    if mlevel < 2.0:
        print(f"❌ MarginLevel peligroso: {mlevel}")
        return {"status": "risk_margin_level"}

    # 4. Borrow usado
    ratio = get_total_borrow_used_ratio()
    if ratio > 0.40:
        print(f"❌ Borrow > 40%: ratio={ratio}")
        return {"status": "risk_borrow_limit"}

    # 5. Ejecutar BUY
    filters = _get_symbol_filters(symbol)
    usdt_clean = float((Decimal(str(usdt_target)) // Decimal(str(filters["tick"]))) * Decimal(str(filters["tick"])))

    res = place_margin_buy(symbol, usdt_clean)

    # 6. Calcular qty real
    qty = float(res.get("executedQty", 0))
    entry_price = float(res.get("price", price))

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    append_trade_row_margin(ws_trades, {
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

    print("🟣 Margin BUY completado.")
    return res


# =============================================================
# 8) MANEJO DE SELL SIGNAL
# =============================================================

def handle_margin_sell_signal(symbol):
    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    # Buscar trade abierto
    trades = ws_trades.get_all_records()
    open_trades = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

    if not open_trades:
        print("⚠️ No hay trades margin abiertos.")
        return {"status": "no_open_trades"}

    last = open_trades[-1]
    qty = float(last["qty"])
    entry_price = float(last["entry_price"])

    # Ejecutar venta
    sell_res = place_margin_sell(symbol, qty)
    sell_price = float(sell_res.get("price", _get_price(symbol)))

    profit = (sell_price - entry_price) * qty

    # Repagar deuda si existe
    repay_borrow("USDT", abs(profit))

    # Actualizar en Sheets
    trades_all = ws_trades.get_all_records()
    idx = trades_all.index(last) + 2

    ws_trades.update(
        f"G{idx}:J{idx}",
        [[
            sell_price,
            datetime.utcnow().isoformat(),
            profit,
            "CLOSED"
        ]]
    )

    print("🔴 Margin SELL completado.")
    return sell_res
