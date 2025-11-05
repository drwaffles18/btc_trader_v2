# =============================================================
# 🟢 Binance Spot Autotrader con OCO — Victor + GPT
# -------------------------------------------------------------
# Objetivo:
# - Ejecutar compras Market por porcentaje del equity total (wallet Spot)
# - Colocar automáticamente una orden OCO (Take Profit + Stop Loss) tras cada compra
# - Cancelar OCO activa y vender Market al recibir señal SELL
# - Respeta pesos por símbolo (BTC, ETH, ADA, XRP, BNB)
#
# Notas clave:
# - TP: puede venir dado por la señal (tp_price). Si no viene, se calcula con RR
#   usando una pérdida por defecto (risk_pct) y RR provista (rr). Ej: RR=1.5
# - SL: stop-limit por debajo del precio de entrada; el TRIGGER se coloca 5% por
#   encima del stop-limit (requerimiento del usuario)
# - Manejo de filtros de Binance: LOT_SIZE, MIN_NOTIONAL, PRICE_FILTER
# - Registro en CSV de operaciones
# - DRY_RUN para pruebas (simula sin enviar órdenes)
#
# Requisitos:
#   pip install python-binance pandas python-dotenv
# =============================================================
# =============================================================
# 🟢 Binance Spot Autotrader con OCO — Victor + GPT (versión segura)
# -------------------------------------------------------------
# Objetivo:
# - Ejecutar compras Market por porcentaje del equity total (wallet Spot)
# - Colocar automáticamente una orden OCO (Take Profit + Stop Loss) tras cada compra
# - Cancelar OCO activa y vender Market al recibir señal SELL
# - Respeta pesos por símbolo (BTC, ETH, ADA, XRP, BNB)
#
# Seguridad:
# - Si no hay claves de Binance configuradas, entra en modo “solo alertas”
# - DRY_RUN permite simular trades sin enviar órdenes reales
# =============================================================

import os
import math
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

import pandas as pd
from dotenv import load_dotenv

# Intentamos importar el cliente solo si hay claves
try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None

# =============================
# 0) Configuración general
# =============================
load_dotenv()

API_KEY    = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

if not API_KEY or not API_SECRET or Client is None:
    print("⚠️ Claves Binance no configuradas o módulo no disponible. Modo solo alertas activo.")
    BINANCE_ENABLED = False
    client = None
else:
    BINANCE_ENABLED = True
    client = Client(API_KEY, API_SECRET)

# Si quieres probar en modo simulado sin enviar órdenes reales
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Pesos por símbolo (suman 100%)
PORTFOLIO_WEIGHTS = {
    "BTCUSDT": 0.35,
    "ETHUSDT": 0.25,
    "ADAUSDT": 0.10,
    "XRPUSDT": 0.20,
    "BNBUSDT": 0.10,
}

# Archivo de logs
LOG_FILE = "/data/trade_log.csv"

# Parámetros por defecto
DEFAULT_RISK_PCT = 0.01
DEFAULT_RR       = 1.5
SL_TRIGGER_GAP   = 0.05

# =============================
# 1) Utilitarios
# =============================

def _round_step_size(value: float, step_size: float) -> float:
    if step_size == 0:
        return value
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    return float((Decimal(str(value)) // Decimal(str(step_size))) * Decimal(str(step_size))).quantize(
        Decimal(f"1e-{precision}") if precision > 0 else Decimal("1"), rounding=ROUND_DOWN
    )


def _get_symbol_filters(symbol: str):
    if not BINANCE_ENABLED:
        return {"step_size": 0.000001, "min_qty": 0.000001, "tick_size": 0.01, "min_notional": 10.0}
    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}
    lot = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    price_filter = filters.get("PRICE_FILTER", {})
    return {
        "step_size": float(lot.get("stepSize", 0)),
        "min_qty": float(lot.get("minQty", 0)),
        "tick_size": float(price_filter.get("tickSize", 0)),
        "min_notional": float(min_notional.get("minNotional", 0)),
    }


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker["price"]) if ticker and "price" in ticker else None


def _get_spot_equity_usdt() -> float:
    if not BINANCE_ENABLED:
        return 1000.0  # simulación base
    account = client.get_account()
    balances = {b["asset"]: {"free": float(b["free"]), "locked": float(b["locked"])} for b in account.get("balances", [])}
    equity = balances.get("USDT", {"free": 0, "locked": 0})
    total = equity["free"] + equity["locked"]
    for asset, bal in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD"): continue
        qty = bal["free"] + bal["locked"]
        if qty > 0:
            symbol = f"{asset}USDT"
            try:
                p = _get_price(symbol)
                total += qty * p
            except Exception:
                pass
    return total


def _get_free_balance(asset: str) -> float:
    if not BINANCE_ENABLED:
        return 1000.0 if asset == "USDT" else 0.0
    for b in client.get_account()["balances"]:
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _append_log(row: dict):
    df = pd.DataFrame([row])
    header = not os.path.exists(LOG_FILE)
    df.to_csv(LOG_FILE, mode="a", header=header, index=False)

# =============================
# 2) Cálculo TP/SL
# =============================

def compute_tp_sl(entry_price, rr=None, risk_pct=None, side="BUY", sl_trigger_gap=SL_TRIGGER_GAP):
    rr = rr or DEFAULT_RR
    risk_pct = risk_pct or DEFAULT_RISK_PCT
    if side == "BUY":
        sl_limit = entry_price * (1 - risk_pct)
        tp_price = entry_price * (1 + rr * risk_pct)
        sl_trigger = sl_limit * (1 + sl_trigger_gap)
    else:
        sl_limit = entry_price * (1 + risk_pct)
        tp_price = entry_price * (1 - rr * risk_pct)
        sl_trigger = sl_limit * (1 - sl_trigger_gap)
    return tp_price, sl_limit, sl_trigger

# =============================
# 3) Órdenes
# =============================

def place_market_buy_by_quote(symbol, usdt_amount):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}
    if DRY_RUN:
        price = _get_price(symbol)
        qty = usdt_amount / price if price else 0
        return {"symbol": symbol, "status": "FILLED", "executedQty": str(qty), "price": str(price)}
    return client.create_order(symbol=symbol, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quoteOrderQty=str(usdt_amount))


def place_oco_sell(symbol, quantity, tp_price, sl_limit_price, sl_trigger_price):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}
    filters = _get_symbol_filters(symbol)
    q = _round_step_size(quantity, filters["step_size"])
    tp = _round_step_size(tp_price, filters["tick_size"])
    sl = _round_step_size(sl_limit_price, filters["tick_size"])
    tr = _round_step_size(sl_trigger_price, filters["tick_size"])
    if DRY_RUN:
        return {"symbol": symbol, "status": "SIMULATED_OCO", "qty": q, "tp": tp, "sl": sl, "tr": tr}
    return client.create_oco_order(symbol=symbol, side=SIDE_SELL, quantity=str(q),
                                   price=str(tp), stopPrice=str(tr), stopLimitPrice=str(sl),
                                   stopLimitTimeInForce=TIME_IN_FORCE_GTC)


def cancel_open_oco(symbol):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}
    open_ocos = client.get_open_oco_orders()
    cancelled = []
    for oco in open_ocos:
        if oco["orders"][0]["symbol"] == symbol:
            res = client.cancel_oco_order(symbol=symbol, orderListId=oco["orderListId"])
            cancelled.append(res)
    return cancelled


def sell_all_market(symbol):
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}
    asset = symbol.replace("USDT", "")
    qty = _get_free_balance(asset)
    if qty <= 0:
        return {"status": "NO_POSITION", "symbol": symbol}
    filters = _get_symbol_filters(symbol)
    q = _round_step_size(qty, filters["step_size"])
    if DRY_RUN:
        return {"symbol": symbol, "side": "SELL", "qty": q, "status": "SIMULATED"}
    return client.create_order(symbol=symbol, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=str(q))

# =============================
# 4) Señales
# =============================

def handle_buy_signal(symbol, rr=None, risk_pct=None, tp_price=None, sl_limit_pct=None):
    if not BINANCE_ENABLED:
        print(f"⚠️ {symbol}: Claves Binance ausentes. Modo solo alertas.")
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}

    equity = _get_spot_equity_usdt()
    weight = PORTFOLIO_WEIGHTS.get(symbol, 0)
    usdt_to_spend = min(equity * weight, _get_free_balance("USDT"))
    price = _get_price(symbol)
    if usdt_to_spend < 10:
        return {"status": "INSUFFICIENT_USDT", "symbol": symbol}

    buy_order = place_market_buy_by_quote(symbol, usdt_to_spend)
    entry_price = price
    qty = float(buy_order.get("executedQty", usdt_to_spend / price))

    if tp_price is None:
        tp_price, sl_limit, sl_trigger = compute_tp_sl(entry_price, rr, risk_pct)
    else:
        sl_limit = entry_price * (1 - (sl_limit_pct or DEFAULT_RISK_PCT))
        sl_trigger = sl_limit * (1 + SL_TRIGGER_GAP)

    oco = place_oco_sell(symbol, qty, tp_price, sl_limit, sl_trigger)
    _append_log({"ts": datetime.utcnow().isoformat(), "symbol": symbol, "action": "BUY+OCO",
                 "dry_run": DRY_RUN, "usdt_spent": usdt_to_spend, "tp": tp_price})
    return {"buy": buy_order, "oco": oco}


def handle_sell_signal(symbol):
    if not BINANCE_ENABLED:
        print(f"⚠️ {symbol}: Claves Binance ausentes. Modo solo alertas.")
        return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}
    cancel = cancel_open_oco(symbol)
    sell = sell_all_market(symbol)
    _append_log({"ts": datetime.utcnow().isoformat(), "symbol": symbol,
                 "action": "CANCEL_OCO+SELL", "dry_run": DRY_RUN})
    return {"cancel": cancel, "sell": sell}

# =============================
# 5) Enrutador
# =============================

def route_signal(signal: dict):
    symbol = signal.get("symbol")
    side = signal.get("side", "").upper()
    if side == "BUY":
        return handle_buy_signal(symbol, rr=signal.get("rr"), risk_pct=signal.get("risk_pct"),
                                 tp_price=signal.get("tp_price"), sl_limit_pct=signal.get("sl_limit_pct"))
    elif side == "SELL":
        return handle_sell_signal(symbol)
    else:
        return {"status": "IGNORED", "reason": "side no soportado", "side": side}
