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

import os
import math
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

import pandas as pd
from binance.client import Client
from binance.enums import *
from dotenv import load_dotenv

# =============================
# 0) Configuración general
# =============================
load_dotenv()

API_KEY    = os.getenv("BINANCE_API_KEY_TRADING")  # 🔐 Clave con permisos de TRADE
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING")

# Si quieres probar en modo simulado sin enviar órdenes reales
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Pesos por símbolo (suman 100%)
PORTFOLIO_WEIGHTS = {
    "BTCUSDT": 0.35,  # 35%
    "ETHUSDT": 0.25,  # 25%
    "ADAUSDT": 0.10,  # 10% (corregido el typo ADAUSTD -> ADAUSDT)
    "XRPUSDT": 0.20,  # 20%
    "BNBUSDT": 0.10,  # 10%
}

# Archivo de logs
LOG_FILE = "/data/trade_log.csv"

# Parámetros por defecto para cálculos de TP/SL cuando no se proveen
DEFAULT_RISK_PCT = 0.01   # 1% de riesgo por defecto si no se especifica
DEFAULT_RR       = 1.5    # RR por defecto si no se especifica
SL_TRIGGER_GAP   = 0.05   # Trigger 5% por encima del Stop-Limit (requisito del usuario)

# =============================
# 1) Cliente y utilitarios
# =============================
client = Client(API_KEY, API_SECRET)


def _round_step_size(value: float, step_size: float) -> float:
    """Redondea 'value' al múltiplo inferior de 'step_size'."""
    if step_size == 0:
        return value
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    return float((Decimal(str(value)) // Decimal(str(step_size))) * Decimal(str(step_size))).quantize(
        Decimal(f"1e-{precision}") if precision > 0 else Decimal("1"), rounding=ROUND_DOWN
    )


def _get_symbol_filters(symbol: str):
    """Obtiene filtros de exchangeInfo para un símbolo (LOT_SIZE, MIN_NOTIONAL, PRICE_FILTER)."""
    info = client.get_symbol_info(symbol)
    if not info:
        raise ValueError(f"Símbolo no encontrado en exchangeInfo: {symbol}")

    filters = {f["filterType"]: f for f in info["filters"]}
    lot = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    price_filter = filters.get("PRICE_FILTER", {})

    step_size = float(lot.get("stepSize", 0)) if lot else 0.0
    min_qty   = float(lot.get("minQty", 0))   if lot else 0.0
    tick_size = float(price_filter.get("tickSize", 0)) if price_filter else 0.0
    min_not   = float(min_notional.get("minNotional", 0)) if min_notional else 0.0

    return {
        "step_size": step_size,
        "min_qty": min_qty,
        "tick_size": tick_size,
        "min_notional": min_not,
    }


def _get_price(symbol: str) -> float:
    """Precio last en USDT."""
    ticker = client.get_symbol_ticker(symbol=symbol)
    return float(ticker["price"]) if ticker and "price" in ticker else None


def _get_spot_equity_usdt() -> float:
    """Calcula el equity total del wallet Spot en USDT: USDT + sum(asset_qty * asset_price)"""
    account = client.get_account()
    balances = {b["asset"]: {
        "free": float(b["free"]),
        "locked": float(b["locked"])
    } for b in account.get("balances", [])}

    total_usdt = balances.get("USDT", {"free": 0.0, "locked": 0.0})
    equity_usdt = total_usdt["free"] + total_usdt["locked"]

    # Agregar valor de otros assets en USDT
    for asset, bal in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD"):
            continue
        qty = bal["free"] + bal["locked"]
        if qty <= 0:
            continue
        symbol = f"{asset}USDT"
        try:
            price = _get_price(symbol)
            if price:
                equity_usdt += qty * price
        except Exception:
            # Si el par no existe en USDT (poco probable con los 5 símbolos target), lo ignoramos
            pass

    return float(equity_usdt)


def _get_free_balance(asset: str) -> float:
    account = client.get_account()
    for b in account.get("balances", []):
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _append_log(row: dict):
    df = pd.DataFrame([row])
    header = not os.path.exists(LOG_FILE)
    df.to_csv(LOG_FILE, mode="a", header=header, index=False)

# =============================
# 2) Cálculo de TP/SL
# =============================

def compute_tp_sl(entry_price: float,
                  rr: float | None = None,
                  risk_pct: float | None = None,
                  side: str = "BUY",
                  sl_trigger_gap: float = SL_TRIGGER_GAP):
    """
    Calcula TP (limit) y SL (stopLimit + trigger) a partir de RR y risk_pct si no se provee tp/SL.
    - Para BUY: SL por debajo del entry, TP por encima.
    - Para SELL: (no usado aquí, pero dejamos la simetría).
    """
    rr = rr or DEFAULT_RR
    risk_pct = risk_pct or DEFAULT_RISK_PCT

    if side == "BUY":
        sl_limit = entry_price * (1 - risk_pct)
        tp_price = entry_price * (1 + rr * risk_pct)
        sl_trigger = sl_limit * (1 + sl_trigger_gap)  # trigger 5% por encima del stop-limit
    else:  # SELL short (no usamos en spot, pero queda la fórmula)
        sl_limit = entry_price * (1 + risk_pct)
        tp_price = entry_price * (1 - rr * risk_pct)
        sl_trigger = sl_limit * (1 - sl_trigger_gap)

    return float(tp_price), float(sl_limit), float(sl_trigger)

# =============================
# 3) Órdenes: Market BUY por monto USDT + OCO SELL
# =============================

def place_market_buy_by_quote(symbol: str, usdt_amount: float):
    """Compra Market gastando una cantidad en USDT usando quoteOrderQty."""
    if DRY_RUN:
        price = _get_price(symbol)
        qty = usdt_amount / price if price else 0.0
        return {
            "symbol": symbol,
            "status": "FILLED",
            "transactTime": int(time.time()*1000),
            "fills": [{"price": str(price), "qty": str(qty)}],
            "executedQty": str(qty),
            "cummulativeQuoteQty": str(usdt_amount)
        }

    order = client.create_order(
        symbol=symbol,
        side=SIDE_BUY,
        type=ORDER_TYPE_MARKET,
        quoteOrderQty=str(usdt_amount)
    )
    return order


def place_oco_sell(symbol: str, quantity: float, tp_price: float, sl_limit_price: float, sl_trigger_price: float):
    """Coloca un OCO de venta (TP + SL) para una posición long."""
    filters = _get_symbol_filters(symbol)
    step = filters["step_size"]
    tick = filters["tick_size"]

    q = _round_step_size(quantity, step)
    tp = _round_step_size(tp_price, tick)
    sl = _round_step_size(sl_limit_price, tick)
    tr = _round_step_size(sl_trigger_price, tick)

    if DRY_RUN:
        return {
            "symbol": symbol,
            "orderListId": -1,
            "contingencyType": "OCO",
            "status": "EXECUTING",
            "orders": [
                {"type": "TAKE_PROFIT_LIMIT", "price": str(tp)},
                {"type": "STOP_LOSS_LIMIT", "price": str(sl), "stopPrice": str(tr)}
            ],
            "origQty": str(q)
        }

    return client.create_oco_order(
        symbol=symbol,
        side=SIDE_SELL,
        quantity=str(q),
        price=str(tp),
        stopPrice=str(tr),
        stopLimitPrice=str(sl),
        stopLimitTimeInForce=TIME_IN_FORCE_GTC,
    )


def cancel_open_oco(symbol: str):
    """Cancela OCOs abiertos para un símbolo."""
    if DRY_RUN:
        return {"status": "CANCELLED", "symbol": symbol}

    open_ocos = client.get_open_oco_orders()
    cancelled = []
    for oco in open_ocos:
        # Cada OCO trae varios 'orders', tomamos el symbol del primero
        orders = oco.get("orders", [])
        if not orders:
            continue
        order_symbol = orders[0].get("symbol")
        if order_symbol == symbol:
            res = client.cancel_oco_order(symbol=symbol, orderListId=oco["orderListId"])
            cancelled.append(res)
    return cancelled


def sell_all_market(symbol: str):
    """Vende todo el asset del símbolo a mercado."""
    asset = symbol.replace("USDT", "")
    free_qty = _get_free_balance(asset)
    if free_qty <= 0:
        return {"status": "NO_POSITION", "symbol": symbol, "qty": 0}

    filters = _get_symbol_filters(symbol)
    qty = _round_step_size(free_qty, filters["step_size"])
    if qty <= 0:
        return {"status": "QTY_BELOW_STEP", "symbol": symbol, "qty": free_qty}

    if DRY_RUN:
        return {
            "symbol": symbol,
            "side": "SELL",
            "type": "MARKET",
            "executedQty": str(qty),
            "status": "FILLED"
        }

    return client.create_order(
        symbol=symbol,
        side=SIDE_SELL,
        type=ORDER_TYPE_MARKET,
        quantity=str(qty)
    )

# =============================
# 4) Señales: BUY / SELL
# =============================

def handle_buy_signal(symbol: str,
                      rr: float | None = None,
                      risk_pct: float | None = None,
                      tp_price: float | None = None,
                      sl_limit_pct: float | None = None):
    """
    - Calcula equity total en USDT
    - Determina monto USDT a invertir según peso
    - Si no alcanza MIN_NOTIONAL, intenta usar lo disponible en USDT; si sigue sin alcanzar, no tradea
    - Ejecuta Market BUY por monto USDT
    - Coloca OCO sell con TP y SL
    """
    assert symbol in PORTFOLIO_WEIGHTS, f"Símbolo {symbol} no tiene peso configurado"

    # 1) Equity total
    equity = _get_spot_equity_usdt()
    weight = PORTFOLIO_WEIGHTS[symbol]
    target_usdt = equity * weight

    # 2) Verificar balance USDT disponible
    free_usdt = _get_free_balance("USDT")
    usdt_to_spend = min(target_usdt, free_usdt)  # usar lo disponible si es menor (requisito del usuario)

    price = _get_price(symbol)
    if not price:
        return {"status": "NO_PRICE", "symbol": symbol}

    filters = _get_symbol_filters(symbol)
    min_notional = filters["min_notional"]

    if usdt_to_spend < max(min_notional, 10.0):  # proteger contra montos demasiado bajos
        return {"status": "INSUFFICIENT_USDT", "symbol": symbol, "needed": max(min_notional, 10.0), "available": free_usdt}

    # 3) Enviar compra market por USDT
    buy_order = place_market_buy_by_quote(symbol, usdt_to_spend)

    # Precio de entrada promedio
    if DRY_RUN:
        entry_price = float(buy_order["fills"][0]["price"]) if buy_order.get("fills") else price
        filled_qty  = float(buy_order.get("executedQty", 0))
    else:
        # Para órdenes market, a veces sólo hay cummulativeQuoteQty; usamos qty y precio medio
        fills = buy_order.get("fills", [])
        if fills:
            # promedio ponderado
            total_quote = sum(float(f["price"]) * float(f["qty"]) for f in fills)
            total_qty   = sum(float(f["qty"]) for f in fills)
            entry_price = total_quote / total_qty if total_qty > 0 else price
            filled_qty  = total_qty
        else:
            # fallback
            entry_price = price
            filled_qty  = float(buy_order.get("executedQty", 0)) if buy_order.get("executedQty") else usdt_to_spend / price

    # 4) Calcular TP/SL
    if tp_price is None:
        # Si no viene tp_price, calculamos con RR y risk_pct
        tp_price, sl_limit_price, sl_trigger_price = compute_tp_sl(entry_price, rr, risk_pct, side="BUY")
    else:
        # Si el TP viene dado por la señal, calculamos SL con sl_limit_pct o risk_pct
        if sl_limit_pct is not None:
            sl_limit_price = entry_price * (1 - float(sl_limit_pct))
        else:
            # usar risk_pct por defecto si no se provee sl_limit_pct
            rp = risk_pct or DEFAULT_RISK_PCT
            sl_limit_price = entry_price * (1 - rp)
        sl_trigger_price = sl_limit_price * (1 + SL_TRIGGER_GAP)

    # 5) Colocar OCO de venta
    oco_res = place_oco_sell(symbol, filled_qty, tp_price, sl_limit_price, sl_trigger_price)

    # 6) Log
    _append_log({
        "ts": datetime.utcnow().isoformat(),
        "action": "BUY+OCO",
        "symbol": symbol,
        "equity_usdt": round(equity, 2),
        "usdt_spent": round(usdt_to_spend, 2),
        "entry_price": round(entry_price, 8),
        "qty": round(filled_qty, 8),
        "tp_price": round(tp_price, 8),
        "sl_limit_price": round(sl_limit_price, 8),
        "sl_trigger_price": round(sl_trigger_price, 8),
        "dry_run": DRY_RUN
    })

    return {"buy_order": buy_order, "oco": oco_res}


def handle_sell_signal(symbol: str):
    """
    - Si hay OCO activo: cancelarlo
    - Vender a mercado toda la cantidad disponible del asset
    - Si no hay posición: no hacer nada
    """
    # 1) Cancelar OCO abierto
    cancel_res = cancel_open_oco(symbol)

    # 2) Vender todo a mercado
    sell_res = sell_all_market(symbol)

    # 3) Log
    _append_log({
        "ts": datetime.utcnow().isoformat(),
        "action": "CANCEL_OCO+SELL",
        "symbol": symbol,
        "cancel_result": str(cancel_res),
        "sell_result": str(sell_res),
        "dry_run": DRY_RUN
    })

    return {"cancel": cancel_res, "sell": sell_res}

# =============================
# 5) Enrutador de señales
# =============================

def route_signal(signal: dict):
    """
    Señal esperada (ejemplos):
      {"symbol": "BTCUSDT", "side": "BUY",  "rr": 1.5, "risk_pct": 0.01}
      {"symbol": "BNBUSDT", "side": "BUY",  "tp_price": 632.5, "sl_limit_pct": 0.02}
      {"symbol": "BTCUSDT", "side": "SELL"}
    """
    symbol = signal.get("symbol")
    side   = signal.get("side", "").upper()

    if side == "BUY":
        return handle_buy_signal(
            symbol=symbol,
            rr=signal.get("rr"),
            risk_pct=signal.get("risk_pct"),
            tp_price=signal.get("tp_price"),
            sl_limit_pct=signal.get("sl_limit_pct"),
        )
    elif side == "SELL":
        return handle_sell_signal(symbol)
    else:
        return {"status": "IGNORED", "reason": "side no soportado", "side": side}

# =============================
# 6) Ejemplos (comentar/ajustar en prod)
# =============================
if __name__ == "__main__":
    # ⚠️ Para pruebas rápidas: establecer DRY_RUN=true en .env

    # Ejemplo 1: BUY BTC con RR=1.5 y risk_pct=1%
    ejemplo_buy_btc = {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "rr": 1.5,
        "risk_pct": 0.01
    }
    print("BUY BTC:", route_signal(ejemplo_buy_btc))

    # Ejemplo 2: BUY BNB con TP dado por la señal y SL 2%
    ejemplo_buy_bnb = {
        "symbol": "BNBUSDT",
        "side": "BUY",
        "tp_price": 650.0,    # TP explícito de la señal
        "sl_limit_pct": 0.02  # SL 2% por debajo del entry
    }
    print("BUY BNB:", route_signal(ejemplo_buy_bnb))

    # Ejemplo 3: SELL BTC (cancela OCO y vende todo)
    ejemplo_sell_btc = {
        "symbol": "BTCUSDT",
        "side": "SELL"
    }
    print("SELL BTC:", route_signal(ejemplo_sell_btc))
