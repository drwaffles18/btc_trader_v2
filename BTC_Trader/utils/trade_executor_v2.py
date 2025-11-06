# =============================================================
# 🟢 Binance Spot Autotrader con OCO — Victor + GPT (versión limpia)
# -------------------------------------------------------------
# Objetivo:
# - Ejecutar compras Market por porcentaje del equity total (wallet Spot)
# - Colocar automáticamente una orden OCO (Take Profit + Stop Loss)
# - Cancelar OCO activa y vender Market al recibir señal SELL
# - Respeta pesos por símbolo (BTC, ETH, ADA, XRP, BNB)
#
# Seguridad:
# - Si no hay claves configuradas, entra en modo “solo alertas”
# - DRY_RUN permite simular sin enviar órdenes reales
# =============================================================

import os
import math
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import pandas as pd

# Intentamos importar el cliente solo si hay claves
try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None

# =============================
# 0) Configuración general
# =============================

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
LOG_FILE = os.getenv("TRADE_LOG_PATH", "./trade_log.csv")

# Parámetros por defecto
DEFAULT_RISK_PCT = 0.01
DEFAULT_RR       = 1.5
SL_TRIGGER_GAP   = 0.05

# =============================
# 1) Utilitarios
# =============================

def _round_step_size(value: float, step_size: float) -> float:
    """Redondea el valor al múltiplo más cercano permitido por el filtro LOT_SIZE."""
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
    """Devuelve el equity total estimado en USDT."""
    if not BINANCE_ENABLED:
        return 1000.0  # simulación base
    account = client.get_account()
    balances = {b["asset"]: {"free": float(b["free"]), "locked": float(b["locked"])} for b in account.get("balances", [])}
    total = balances.get("USDT", {"free": 0, "locked": 0})
    total_usdt = total["free"] + total["locked"]
    for asset, bal in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD"):
            continue
        qty = bal["free"] + bal["locked"]
        if qty > 0:
            symbol = f"{asset}USDT"
            try:
                price = _get_price(symbol)
                total_usdt += qty * price
            except Exception:
                pass
    return total_usdt


def _get_free_balance(asset: str) -> float:
    if not BINANCE_ENABLED:
        return 1000.0 if asset == "USDT" else 0.0
    for b in client.get_account()["balances"]:
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _append_log(row: dict):
    """Agrega una línea al trade_log.csv."""
    df = pd.DataFrame([row])
    header = not os.path.exists(LOG_FILE)
    df.to_csv(LOG_FILE, mode="a", header=header, index=False)
    print(f"🧾 Log registrado: {row.get('action')} {row.get('symbol')} ({'DRY_RUN' if row.get('dry_run') else 'LIVE'})")

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
    try:
        if not BINANCE_ENABLED:
            msg = f"⚠️ {symbol}: Claves Binance ausentes. Modo solo alertas."
            print(msg)
            _append_log({
                "timestamp": datetime.utcnow(),
                "symbol": symbol,
                "action": "SKIPPED_NO_KEYS",
                "message": msg,
                "dry_run": DRY_RUN
            })
            return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}

        equity = _get_spot_equity_usdt()
        free_usdt = _get_free_balance("USDT")
        weight = PORTFOLIO_WEIGHTS.get(symbol, 0)
        usdt_to_spend = min(equity * weight, free_usdt)

        price = _get_price(symbol)
        if not price:
            raise ValueError("No se pudo obtener el precio del símbolo")

        filters = _get_symbol_filters(symbol)
        min_notional = filters["min_notional"]

        if usdt_to_spend < max(min_notional, 10.0):
            msg = f"❌ USDT insuficiente ({usdt_to_spend:.2f} < {min_notional:.2f})"
            print(msg)
            _append_log({
                "timestamp": datetime.utcnow(),
                "symbol": symbol,
                "action": "INSUFFICIENT_USDT",
                "equity_total": equity,
                "free_usdt": free_usdt,
                "usdt_spent": usdt_to_spend,
                "message": msg,
                "dry_run": DRY_RUN
            })
            return {"status": "INSUFFICIENT_USDT", "symbol": symbol}

        print(f"🟢 Ejecutando BUY {symbol} por {usdt_to_spend:.2f} USDT (equity={equity:.2f}, balance={free_usdt:.2f})")
        buy_order = place_market_buy_by_quote(symbol, usdt_to_spend)

        entry_price = float(buy_order.get("price", price))
        qty = float(buy_order.get("executedQty", usdt_to_spend / price))

        if tp_price is None:
            tp_price, sl_limit, sl_trigger = compute_tp_sl(entry_price, rr, risk_pct)
        else:
            sl_limit = entry_price * (1 - (sl_limit_pct or DEFAULT_RISK_PCT))
            sl_trigger = sl_limit * (1 + SL_TRIGGER_GAP)

        print(f"🎯 Colocando OCO {symbol} (TP={tp_price:.4f}, SL={sl_limit:.4f}, Trigger={sl_trigger:.4f})")
        oco = place_oco_sell(symbol, qty, tp_price, sl_limit, sl_trigger)

        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "BUY+OCO",
            "equity_total": equity,
            "free_usdt": free_usdt,
            "usdt_spent": usdt_to_spend,
            "entry_price": entry_price,
            "qty": qty,
            "tp_price": tp_price,
            "sl_price": sl_limit,
            "sl_trigger": sl_trigger,
            "dry_run": DRY_RUN,
            "message": "Buy ejecutado y OCO colocado correctamente"
        })

        return {"buy": buy_order, "oco": oco}

    except Exception as e:
        err = f"⚠️ Error en BUY {symbol}: {e}"
        print(err)
        _append_log({
            "timestamp": datetime.utcnow(),
            "symbol": symbol,
            "action": "ERROR_BUY",
            "message": str(e),
            "dry_run": DRY_RUN
        })
        return {"status": "ERROR", "error": str(e)}


def handle_sell_signal(symbol):
    try:
        if not BINANCE_ENABLED:
            msg = f"⚠️ {symbol}: Claves Binance ausentes. Modo solo alertas."
            print(msg)
            _append_log({
                "timestamp": datetime.utcnow(),
                "symbol": symbol,
                "action": "SKIPPED_NO_KEYS",
                "message": msg,
                "dry_run": DRY_RUN
            })
            return {"status": "SKIPPED_NO_KEYS", "symbol": symbol}

        print(f"🔍 Buscando OCO activo para {symbol}...")
        cancel_res = cancel_open_oco(symbol)
        if cancel_res:
            print(f"🟡 OCO encontrado y cancelado ({len(cancel_res)} órdenes).")
        else:
            print("⚠️ No se encontraron OCOs activos.")

        asset = symbol.replace("USDT", "")
        free_qty = _get_free_balance(asset)
        equity = _get_spot_equity_usdt()

        if free_qty <= 0:
            msg = f"❌ No hay balance disponible para vender {asset}."
            print(msg)
            _append_log({
                "timestamp": datetime.utcnow(),
                "symbol": symbol,
                "action": "NO_POSITION",
                "equity_total": equity,
                "free_qty": free_qty,
                "message": msg,
                "dry_run": DRY_RUN
            })
            return {"status": "NO_POSITION", "symbol": symbol}

        print(f"🔴 Ejecutando Market SELL {symbol} — cantidad={free_qty:.6f}")
        sell_res = sell_all_market(symbol)

        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "CANCEL_OCO+SELL",
            "equity_total": equity,
            "free_qty": free_qty,
            "dry_run": DRY_RUN,
            "message": "Cancelación y venta ejecutadas correctamente"
        })
        print(f"✅ SELL completado para {symbol}. (DRY_RUN={DRY_RUN})")

        return {"cancel": cancel_res, "sell": sell_res}

    except Exception as e:
        err = f"⚠️ Error en SELL {symbol}: {e}"
        print(err)
        _append_log({
            "timestamp": datetime.utcnow(),
            "symbol": symbol,
            "action": "ERROR_SELL",
            "message": str(e),
            "dry_run": DRY_RUN
        })
        return {"status": "ERROR", "error": str(e)}

# =============================
# 5) Enrutador
# =============================

def route_signal(signal: dict):
    """Enruta la señal BUY/SELL a la función correspondiente."""
    symbol = signal.get("symbol")
    side = signal.get("side", "").upper()
    if side == "BUY":
        return handle_buy_signal(symbol,
                                 rr=signal.get("rr"),
                                 risk_pct=signal.get("risk_pct"),
                                 tp_price=signal.get("tp_price"),
                                 sl_limit_pct=signal.get("sl_limit_pct"))
    elif side == "SELL":
        return handle_sell_signal(symbol)
    else:
        return {"status": "IGNORED", "reason": "side no soportado", "side": side}
