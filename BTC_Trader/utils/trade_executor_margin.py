# =============================================================
# 🟣 Binance Cross Margin Autotrader — Victor + GPT
# -------------------------------------------------------------
# - Usa la misma API key que Spot (misma cuenta).
# - Pensado para operar principalmente en Cross Margin:
#       * Calcula equity base (margin si hay, si no spot).
#       * Tamaño objetivo del trade = equity_base * weight * MARGIN_MULTIPLIER
#       * Usa margin loan (borrow) automático si hace falta USDT.
#       * Ejecuta BUY / SELL en Cross Margin (isIsolated = FALSE).
# - Al cerrar:
#       * Vende qty registrada.
#       * Calcula profit.
#       * Repaga TODA la deuda USDT en Margin.
#       * NO transfiere de vuelta a Spot (tu API no tiene permiso).
#
# - Logging en Google Sheets:
#       * Columna 11: trade_mode = "MARGIN"
#
# - Este módulo se usa solo si USE_MARGIN = true en el router.
#   El router llama a:
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

# Multiplicador de tamaño vs equity base (ej. 3x)
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

def _round_step_size(value: float, step_size: float) -> float:
    """Redondea qty al múltiplo permitido por LOT_SIZE."""
    if step_size == 0:
        return value
    dec_val = Decimal(str(value))
    dec_step = Decimal(str(step_size))
    rounded = (dec_val // dec_step) * dec_step
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)


def _get_symbol_filters(symbol: str):
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


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


# ---------------- Spot helpers ----------------

def _get_spot_free_usdt() -> float:
    """USDT libre en Spot."""
    if not BINANCE_ENABLED:
        return 1000.0
    acc = client.get_account()
    for b in acc["balances"]:
        if b["asset"] == "USDT":
            return float(b["free"])
    return 0.0


def _get_spot_equity_usdt() -> float:
    """
    Equity total en Spot en USDT (USDT + otros assets valorados en USDT).
    Similar al spot executor.
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


def _get_margin_equity_usdt() -> float:
    """
    Equity total del margin account en USDT:
    totalAssetOfBtc * precio BTCUSDT.
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    total_btc = float(acc.get("totalAssetOfBtc", 0.0))
    if total_btc <= 0:
        return 0.0
    btc_price = _get_price("BTCUSDT") or 0.0
    return total_btc * btc_price


def _get_margin_free_usdt() -> float:
    """USDT libre en cuenta Margin (cross)."""
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            return float(a["free"])
    return 0.0


def _get_margin_debt_usdt() -> float:
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


def get_margin_level() -> float:
    """Margin Level = totalAssetOfBtc / totalLiabilityOfBtc."""
    if not BINANCE_ENABLED:
        return 99.0

    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0.0))
    debts = float(acc.get("totalLiabilityOfBtc", 0.0))
    if debts == 0:
        return 99.0
    return assets / debts


def get_total_borrow_used_ratio() -> float:
    """
    Ratio de endeudamiento:
    liability / asset (en BTC).
    """
    if not BINANCE_ENABLED:
        return 0.0

    acc = client.get_margin_account()
    liability = float(acc.get("totalLiabilityOfBtc", 0.0))
    asset = float(acc.get("totalAssetOfBtc", 0.0))
    if asset <= 0:
        return 1.0  # riesgo máximo
    return liability / asset


def borrow_if_needed(asset: str, amount_needed: float):
    """
    Pide prestado 'asset' si el free en Margin es menor a amount_needed.
    Solo pedimos lo que falta.
    """
    if amount_needed <= 0:
        print("ℹ️ borrow_if_needed: amount_needed <= 0, no se pide préstamo.")
        return {"status": "NO_BORROW"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN borrow {asset} por {amount_needed:.6f}")
        return {"status": "DRY_RUN", "asset": asset, "amount": amount_needed}

    # Free actual en Margin
    free_margin = 0.0
    if asset == "USDT":
        free_margin = _get_margin_free_usdt()

    missing = amount_needed - free_margin
    if missing <= 0:
        print(f"ℹ️ No se requiere borrow. free_margin={free_margin:.6f} ≥ needed={amount_needed:.6f}")
        return {"status": "NO_BORROW", "free_margin": free_margin}

    try:
        res = client.create_margin_loan(asset=asset, amount=str(missing))
        print(f"🟣 Borrow ejecutado: {asset} {missing:.6f} → {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR borrow {asset}: {e}")
        return {"error": str(e)}


def _repay_all_usdt_debt():
    """Repaga toda la deuda de USDT en Margin."""
    debt = _get_margin_debt_usdt()
    if debt <= 0:
        print("ℹ️ No hay deuda USDT que repagar.")
        return {"status": "NO_DEBT"}

    if DRY_RUN or not BINANCE_ENABLED:
        print(f"💤 DRY_RUN repay USDT debt {debt:.6f}")
        return {"status": "DRY_RUN", "action": "REPAY", "asset": "USDT", "amount": debt}

    try:
        # En python-binance la función es repay_margin_loan
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
# 4) MANEJO DE BUY SIGNAL (MARGIN)
# =============================================================

def handle_margin_buy_signal(symbol: str):
    """
    BUY en Cross Margin:
    - Usa equity de Margin si existe, si no equity Spot.
    - Tamaño objetivo = equity_base * weight * MARGIN_MULTIPLIER.
    - Aplica controles de riesgo (margin level, borrow_ratio).
    - Usa borrow_if_needed("USDT", notional) antes del BUY.
    - Registra el trade en Sheets con trade_mode = "MARGIN".
    """
    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    # 1. Equity base
    margin_equity = _get_margin_equity_usdt()
    spot_equity = _get_spot_equity_usdt()
    free_spot_usdt = _get_spot_free_usdt()

    equity_base = margin_equity if margin_equity > 0 else spot_equity

    print(f"ℹ️ Margin equity ≈ {margin_equity:.2f} USDT | Spot equity ≈ {spot_equity:.2f} USDT | free Spot USDT ≈ {free_spot_usdt:.2f}")
    print(f"ℹ️ Usando equity_base ≈ {equity_base:.2f} USDT")

    weight = PORTFOLIO_WEIGHTS.get(symbol, 0.0)
    if weight <= 0:
        print(f"⚠️ weight=0 para {symbol}, se ignora BUY.")
        return {"status": "NO_WEIGHT"}

    base_target = equity_base * weight
    trade_notional_raw = base_target * MARGIN_MULTIPLIER

    print(f"🧮 {symbol}: base_target ≈ {base_target:.2f} → trade_notional_raw ≈ {trade_notional_raw:.2f}")

    # 2. Filtros del símbolo y pisos mínimos
    f = _get_symbol_filters(symbol)
    tick = Decimal(str(f["tick"]))
    min_notional_filter = f["min_notional"]
    min_required = max(BINANCE_NOTIONAL_FLOOR, min_notional_filter)

    if trade_notional_raw < min_required:
        print(f"❌ Trade demasiado pequeño: {trade_notional_raw:.2f} < min_required={min_required:.2f}")
        return {"status": "too_small"}

    # 🔧 Ajuste de precisión de notional
    trade_notional = float((Decimal(str(trade_notional_raw)) // tick) * tick)
    print(f"🔧 Notional limpio (tick) ≈ {trade_notional:.4f} USDT (min_required={min_required:.2f})")

    if trade_notional < min_required:
        print(f"❌ Notional limpio < min_required después de ajustar tick.")
        return {"status": "too_small_clean"}

    # 3. Controles de riesgo en Margin
    mlevel = get_margin_level()
    if mlevel < 2.0:
        print(f"❌ MarginLevel peligroso: {mlevel}")
        return {"status": "risk_margin_level", "margin_level": mlevel}

    borrow_ratio = get_total_borrow_used_ratio()
    if borrow_ratio > 0.40:
        print(f"❌ Borrow ratio > 40%: {borrow_ratio}")
        return {"status": "risk_borrow_limit", "borrow_ratio": borrow_ratio}

    # 4. Pedir préstamo si hace falta
    borrow_res = borrow_if_needed("USDT", trade_notional)
    if isinstance(borrow_res, dict) and "error" in borrow_res:
        print(f"❌ ERROR en borrow USDT, abort BUY: {borrow_res['error']}")
        return {"status": "borrow_failed", "detail": borrow_res["error"]}

    # 5. Ejecutar BUY en Margin
    res = place_margin_buy(symbol, trade_notional)
    if "error" in res:
        print(f"❌ Margin BUY falló: {res['error']}")
        return res

    executed_qty = float(res.get("executedQty", 0.0))
    quote_spent = float(res.get("cummulativeQuoteQty", trade_notional))

    if executed_qty > 0 and quote_spent > 0:
        entry_price = quote_spent / executed_qty
    else:
        entry_price = _get_price(symbol)

    qty = executed_qty

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    # 6. Registrar trade en Sheets
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

    print(f"🟣 Margin BUY completado: qty≈{qty:.6f} entry≈{entry_price:.4f}")
    return res


# =============================================================
# 5) MANEJO DE SELL SIGNAL (MARGIN)
# =============================================================

def handle_margin_sell_signal(symbol: str):
    """
    SELL en Cross Margin:
    - Busca el último trade OPEN en Sheets para ese símbolo, preferiblemente MARGIN.
    - Vende la cantidad registrada (qty) en Margin.
    - Calcula profit.
    - Repaga deuda USDT (toda).
    - No transfiere de vuelta a Spot.
    - Actualiza fila en Sheets.
    """
    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    # 1. Buscar trade abierto en Sheets
    trades = ws_trades.get_all_records()
    open_trades = [t for t in trades if t.get("symbol") == symbol and t.get("status") == "OPEN"]

    if not open_trades:
        print("⚠️ No hay trades OPEN para cerrar en Sheets.")
        return {"status": "NO_OPEN_TRADES"}

    # Preferimos el último con trade_mode = "MARGIN", si existe
    margin_trades = [t for t in open_trades if str(t.get("trade_mode", "")).upper() == "MARGIN"]
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

    # 5. Actualizar en Sheets
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
