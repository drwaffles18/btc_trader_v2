# =============================================================
# 🟣 Binance Cross Margin Autotrader V3 — Victor + GPT
# -------------------------------------------------------------
# - Usa la misma API key que Spot (wallet compartida)
# - Lógica:
#       * Calcula cuánto hubiéramos invertido en Spot (spot_target)
#       * Trade en Margin = spot_target * MARGIN_MULTIPLIER (ej. 3x)
#       * Antes del BUY: transfiere spot_target USDT de Spot → Margin
#       * BUY en Cross Margin (puede usar borrow automático)
#       * SELL:
#           - Vende la posición en Margin
#           - Repaga deuda USDT
#           - Transfiere todo el USDT libre Margin → Spot
# - Logging:
#       * Google Sheets: columna extra trade_mode = "MARGIN"
# -------------------------------------------------------------
# IMPORTANTE:
# - Este módulo se usa solo si USE_MARGIN=true en el router.
# - El router llama a:
#       * handle_margin_buy_signal(symbol)
#       * handle_margin_sell_signal(symbol)
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

API_KEY = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Multiplicador de tamaño vs Spot (ej. 3x)
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Piso mínimo por trade en USDT
BINANCE_NOTIONAL_FLOOR = 5.0

# Pesos por símbolo (mismo criterio que Spot)
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
    Estructura esperada de columnas:
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
        return {"step": 0.000001, "tick": 0.01, "min_notional": 5}

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


# ---------------- Spot helpers ----------------

def _get_spot_free_usdt():
    """USDT libre en Spot."""
    if not BINANCE_ENABLED:
        return 1000.0
    acc = client.get_account()
    for b in acc["balances"]:
        if b["asset"] == "USDT":
            return float(b["free"])
    return 0.0


def _get_spot_equity_usdt():
    """
    Equity total en Spot en USDT (USDT + otros assets valorados en USDT).
    Similar al spot executor v2.
    """
    if not BINANCE_ENABLED:
        return 1000.0

    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acc["balances"]}

    total = balances.get("USDT", 0.0)
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


# ---------------- Margin helpers ----------------

def _get_margin_account():
    if not BINANCE_ENABLED:
        return {}
    return client.get_margin_account()


def _get_margin_free_usdt():
    """USDT libre en cuenta Margin (cross)."""
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            return float(a["free"])
    return 0.0


def _get_margin_debt_usdt():
    """Deuda total de USDT en Margin = borrowed + interest."""
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            borrowed = float(a.get("borrowed", 0.0))
            interest = float(a.get("interest", 0.0))
            return borrowed + interest
    return 0.0


def _transfer_spot_to_margin(asset: str, amount: float):
    if amount <= 0:
        return {"status": "SKIPPED", "reason": "amount <= 0"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN transfer Spot → Margin {asset} {amount}")
        return {"status": "DRY_RUN", "direction": "SPOT_TO_MARGIN", "asset": asset, "amount": amount}

    try:
        res = client.transfer_spot_to_margin(asset=asset, amount=str(amount))
        print(f"🔄 Spot → Margin transfer {asset} {amount}: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR transfer Spot→Margin: {e}")
        return {"error": str(e)}


def _transfer_margin_to_spot(asset: str, amount: float):
    if amount <= 0:
        return {"status": "SKIPPED", "reason": "amount <= 0"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN transfer Margin → Spot {asset} {amount}")
        return {"status": "DRY_RUN", "direction": "MARGIN_TO_SPOT", "asset": asset, "amount": amount}

    try:
        res = client.transfer_margin_to_spot(asset=asset, amount=str(amount))
        print(f"🔄 Margin → Spot transfer {asset} {amount}: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR transfer Margin→Spot: {e}")
        return {"error": str(e)}


def _repay_all_usdt_debt():
    """Repaga toda la deuda de USDT en Margin."""
    debt = _get_margin_debt_usdt()
    if debt <= 0:
        print("ℹ️ No hay deuda USDT que repagar.")
        return {"status": "NO_DEBT"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN repay USDT debt {debt}")
        return {"status": "DRY_RUN", "action": "REPAY", "asset": "USDT", "amount": debt}

    try:
        # En python-binance la función correcta es repay_margin_loan
        res = client.repay_margin_loan(asset="USDT", amount=str(debt))
        print(f"💰 Repay USDT debt ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repaying USDT debt: {e}")
        return {"error": str(e)}


# =============================================================
# 3) MARGIN MARKET BUY / SELL
# =============================================================

def place_margin_buy(symbol: str, usdt_amount: float):
    """Ejecuta un market BUY en Cross Margin con quoteOrderQty."""
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
    """Ejecuta un market SELL en Cross Margin."""
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
# 4) MANEJO DE BUY SIGNAL (API PÚBLICA)
# =============================================================

def handle_margin_buy_signal(symbol: str):
    """
    BUY en Cross Margin con:
    - cálculo de cuánto hubiéramos invertido en Spot
    - trade_notional = spot_target * MARGIN_MULTIPLIER
    - transferencia Spot → Margin del spot_target
    - BUY en margin
    - registro en Sheets con trade_mode="MARGIN"
    """

    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    weight = PORTFOLIO_WEIGHTS.get(symbol)
    if weight is None:
        print(f"⚠️ No hay weight configurado para {symbol}.")
        return {"status": "NO_WEIGHT"}

    # 1. Equity y free USDT en Spot
    spot_equity = _get_spot_equity_usdt()
    free_usdt_spot = _get_spot_free_usdt()
    print(f"ℹ️ Spot equity ≈ {spot_equity:.2f} USDT | free USDT ≈ {free_usdt_spot:.2f}")

    # 2. Cuánto habríamos invertido en Spot (como el spot executor v2)
    spot_target = min(spot_equity * weight, free_usdt_spot)
    if spot_target <= 0:
        print(f"❌ spot_target <= 0 para {symbol}.")
        return {"status": "NO_USDT_SPOT"}

    # 3. Tamaño del trade en Margin (ej. 3x)
    trade_notional = spot_target * MARGIN_MULTIPLIER

    # 4. Chequeo de mínimo
    min_required = max(BINANCE_NOTIONAL_FLOOR, 1.0)
    if trade_notional < min_required:
        print(f"❌ Trade demasiado pequeño: {trade_notional:.2f} < {min_required:.2f}")
        return {"status": "TOO_SMALL", "notional": trade_notional}

    print(f"🧮 {symbol}: spot_target ≈ {spot_target:.2f} → trade_notional (margin) ≈ {trade_notional:.2f}")

    # 5. Transferencia Spot → Margin del spot_target como colateral
    filters = _get_symbol_filters(symbol)
    tick_usdt = max(filters["tick"], 0.01)  # redondeo a 0.01 USDT

    col_dec = Decimal(str(spot_target))
    tick_dec = Decimal(str(tick_usdt))
    collateral_clean = float((col_dec // tick_dec) * tick_dec)

    if collateral_clean <= 0:
        print("⚠️ Colateral limpio <= 0, skip transfer.")
    else:
        _transfer_spot_to_margin("USDT", collateral_clean)

    # 6. Ejecutar BUY en Margin
    res = place_margin_buy(symbol, trade_notional)
    if "error" in res:
        print("❌ Margin BUY falló, no se registra trade en Sheets.")
        return res

    executed_qty = float(res.get("executedQty", 0.0))
    quote_spent = float(res.get("cummulativeQuoteQty", trade_notional))

    if executed_qty > 0:
        entry_price = quote_spent / executed_qty
    else:
        entry_price = _get_price(symbol)

    qty = executed_qty

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    # 7. Registrar en Google Sheets
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
        "status": "OPEN",
        "trade_mode": "MARGIN",
    })

    print("🟣 Margin BUY completado y registrado en Sheets.")
    return res


# =============================================================
# 5) MANEJO DE SELL SIGNAL (API PÚBLICA)
# =============================================================

def handle_margin_sell_signal(symbol: str):
    """
    SELL en Cross Margin:
    - Busca el último trade OPEN en Sheets para ese símbolo, preferiblemente MARGIN
    - Vende la cantidad registrada (qty) en Margin
    - Calcula profit
    - Repaga deuda USDT
    - Transfiere USDT libre Margin → Spot
    - Actualiza fila en Sheets
    """

    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    # 1. Buscar trade abierto en Sheets
    trades = ws_trades.get_all_records()
    open_trades = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

    if not open_trades:
        print("⚠️ No hay trades OPEN para cerrar en Sheets.")
        return {"status": "NO_OPEN_TRADES"}

    # Preferimos el último trade MARGIN, pero si no tiene trade_mode usamos el último
    margin_trades = [t for t in open_trades if t.get("trade_mode", "").upper() == "MARGIN"]
    if margin_trades:
        last = margin_trades[-1]
    else:
        last = open_trades[-1]

    qty = float(last["qty"])
    entry_price = float(last["entry_price"])
    row_idx = trades.index(last) + 2  # header + index base 1

    if qty <= 0:
        print("⚠️ Qty del trade <= 0, abort SELL.")
        return {"status": "INVALID_QTY"}

    # 2. Ajustar qty según LOT_SIZE en Margin
    filters = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty, filters["step"])

    if qty_clean <= 0:
        print("⚠️ Qty limpia <= 0 después de LOT_SIZE, abort SELL.")
        return {"status": "INVALID_QTY_CLEAN"}

    # 3. Ejecutar SELL en Margin
    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ Margin SELL falló, no se actualiza Sheets.")
        return sell_res

    # Precio efectivo de venta
    executed_qty = float(sell_res.get("executedQty", qty_clean))
    quote_got = float(sell_res.get("cummulativeQuoteQty", 0.0))
    if executed_qty > 0 and quote_got > 0:
        sell_price = quote_got / executed_qty
    else:
        sell_price = _get_price(symbol)

    profit = (sell_price - entry_price) * qty_clean

    # 4. Repagar deuda de USDT
    _repay_all_usdt_debt()

    # 5. Transferir todo USDT libre de Margin → Spot
    free_usdt_margin = _get_margin_free_usdt()
    if free_usdt_margin > 0:
        _transfer_margin_to_spot("USDT", free_usdt_margin)
    else:
        print("ℹ️ No hay USDT libre en Margin para devolver a Spot.")

    # 6. Actualizar en Sheets
    ws_trades.update(
        f"G{row_idx}:J{row_idx}",
        [[
            sell_price,
            datetime.utcnow().isoformat(),
            profit,
            "CLOSED"
        ]]
    )

    # trade_mode ya estaba en la columna 11 como "MARGIN", no se toca aquí

    print(f"🔴 Margin SELL completado. Profit ≈ {profit:.4f} USDT")
    return sell_res
