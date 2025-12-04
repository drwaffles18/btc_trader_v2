# =============================================================
# 🟣 Binance Cross Margin Autotrader V5 — Victor + GPT
# -------------------------------------------------------------
#  ✔ Usa cuenta Cross Margin como principal (Opción B)
#  ✔ No transfiere nada Spot ↔ Margin
#  ✔ Usa borrow cuando falta USDT (con decimales limpios)
#  ✔ BUY calcula notional según portafolio × 3x
#  ✔ Ajusta el notional al máximo posible después del borrow
#  ✔ Si el BUY falla, repaga solo lo prestado en ese intento
#  ✔ SELL liquida el 100% de lo que haya realmente en Margin
#  ✔ Repaga deuda automáticamente al vender
#  ✔ Registro de Trades en Google Sheets con trade_mode = "MARGIN"
#  ✔ Debug extendido pero liviano
#
#  Se usa solo cuando USE_MARGIN = true en el router.
#  Funciones llamadas:
#      handle_margin_buy_signal(symbol)
#      handle_margin_sell_signal(symbol)
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from utils.google_client import get_gsheet_client

# -------------------------------------------------------------
#  Importar cliente Binance
# -------------------------------------------------------------
try:
    from binance.client import Client
    from binance.enums import *
except ImportError:
    Client = None


# =============================================================
# 0) CONFIGURACIÓN GENERAL
# =============================================================

API_KEY = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Multiplicador del tamaño base (spot_target)
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Piso mínimo de notional por trade
BINANCE_NOTIONAL_FLOOR = 5.0

# Pesos de portafolio usados tanto en Spot como en Margin
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
    Inserta trade en la hoja 'Trades'.
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
    """
    Redondea hacia abajo al múltiplo permitido por LOT_SIZE.
    """
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
    """
    Lee LOT_SIZE, PRICE_FILTER(tick) y MIN_NOTIONAL.
    """
    if not BINANCE_ENABLED:
        return {"step": 0.000001, "tick": 0.01, "min_notional": 5.0}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}

    return {
        "step": float(filters.get("LOT_SIZE", {}).get("stepSize", 0)),
        "tick": float(filters.get("PRICE_FILTER", {}).get("tickSize", 0.01)),
        "min_notional": float(filters.get("MIN_NOTIONAL", {}).get("minNotional", 5.0)),
    }


def _get_price(symbol):
    if not BINANCE_ENABLED:
        return 0.0
    try:
        return float(client.get_symbol_ticker(symbol=symbol)["price"])
    except Exception:
        return 0.0


# -------------------------------------------------------------
# Spot helpers (fallback únicamente)
# -------------------------------------------------------------

def _get_spot_equity_usdt():
    if not BINANCE_ENABLED:
        return 0.0

    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acc["balances"]}

    total = balances.get("USDT", 0.0)
    for asset, qty in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD") or qty <= 0:
            continue
        symbol = f"{asset}USDT"
        price = _get_price(symbol)
        total += qty * price
    return total


# -------------------------------------------------------------
# Margin helpers
# -------------------------------------------------------------

def get_margin_equity_usdt():
    """
    Convierte totalAssetOfBtc → USDT.
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    btc_equity = float(acc.get("totalAssetOfBtc", 0))
    return btc_equity * _get_price("BTCUSDT")


def _get_margin_free_usdt():
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc["userAssets"]:
        if a["asset"] == "USDT":
            return float(a["free"])
    return 0.0


def _get_margin_free_asset(asset):
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc["userAssets"]:
        if a["asset"] == asset:
            return float(a["free"])
    return 0.0


def get_margin_level():
    if not BINANCE_ENABLED:
        return 99.0
    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0))
    liab = float(acc.get("totalLiabilityOfBtc", 0))
    return 99.0 if liab == 0 else assets / liab


def get_total_borrow_used_ratio():
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0))
    liab = float(acc.get("totalLiabilityOfBtc", 0))
    return 1.0 if assets == 0 else liab / assets


# =============================================================
# 3) BORROW / REPAY
# =============================================================

def borrow_if_needed(asset, required_usdt):
    """
    Realiza borrow si free_margin_usdt < required_usdt.
    Usa Decimal y trunca a 2 decimales para evitar errores -1100.
    Devuelve cuánto se pidió prestado.
    """
    if not BINANCE_ENABLED:
        return {"status": "DISABLED", "borrowed": 0.0}

    free = _get_margin_free_usdt()

    required_dec = Decimal(str(required_usdt))
    free_dec = Decimal(str(free))
    missing_dec = required_dec - free_dec

    if missing_dec <= Decimal("0"):
        print(
            f"💳 borrow_if_needed → free={free:.6f}, "
            f"required={required_usdt:.6f}, missing_raw={float(missing_dec):.6f}, borrow_amount=0.00"
        )
        return {"status": "NO_BORROW_NEEDED", "borrowed": 0.0, "free_before": free}

    # Truncar a 2 decimales (ej: 33.662875 → 33.66)
    amount_dec = missing_dec.quantize(Decimal("1.00"), rounding=ROUND_DOWN)

    if amount_dec <= Decimal("0"):
        print(
            f"💳 borrow_if_needed → free={free:.6f}, "
            f"required={required_usdt:.6f}, missing_raw={float(missing_dec):.6f}, amount_dec<=0 tras truncar"
        )
        return {"status": "NO_BORROW_NEEDED_POST_TRUNC", "borrowed": 0.0, "free_before": free}

    amount_str = str(amount_dec)

    print(
        f"💳 borrow_if_needed → free={free:.6f}, required={required_usdt:.6f}, "
        f"missing_raw={float(missing_dec):.6f}, borrow_amount={amount_str}"
    )

    if DRY_RUN:
        print(f"💤 DRY_RUN borrow {asset} {amount_str}")
        return {"status": "DRY_RUN", "borrowed": float(amount_dec), "free_before": free}

    try:
        res = client.create_margin_loan(asset=asset, amount=amount_str)
        print(f"🟣 Borrow ejecutado correctamente: {res}")
        return {"status": "BORROW_OK", "borrowed": float(amount_dec), "free_before": free, "raw": res}
    except Exception as e:
        print(f"❌ ERROR borrow {asset}: {e}")
        return {"status": "BORROW_FAILED", "error": str(e), "borrowed": 0.0, "free_before": free}


def _repay_all_usdt_debt():
    """
    Repaga toda la deuda de USDT.
    Monto truncado a 2 decimales para evitar errores de formato.
    """
    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    acc = client.get_margin_account()
    borrowed = interest = 0.0
    for a in acc["userAssets"]:
        if a["asset"] == "USDT":
            borrowed = float(a.get("borrowed", 0))
            interest = float(a.get("interest", 0))
            break

    debt = borrowed + interest

    if debt <= 0:
        print("ℹ️ No hay deuda que repagar.")
        return {"status": "NO_DEBT"}

    debt_dec = Decimal(str(debt)).quantize(Decimal("1.00"), rounding=ROUND_DOWN)
    if debt_dec <= Decimal("0"):
        print(f"ℹ️ Deuda muy pequeña tras truncar: {debt_dec}")
        return {"status": "NO_DEBT_TRUNC"}

    debt_str = str(debt_dec)
    print(f"💰 Repagando deuda total USDT: {debt_str}")

    if DRY_RUN:
        return {"status": "DRY_RUN", "debt": float(debt_dec)}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=debt_str)
        print(f"💰 Repay ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repay: {e}")
        return {"status": "REPAY_FAILED", "error": str(e)}


def _repay_usdt_amount(amount):
    """
    Repaga una cantidad específica de deuda USDT (parcial).
    Trunca a 2 decimales y se asegura de no exceder la deuda real.
    """
    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    if amount <= 0:
        return {"status": "NO_AMOUNT"}

    acc = client.get_margin_account()
    borrowed = interest = 0.0
    for a in acc["userAssets"]:
        if a["asset"] == "USDT":
            borrowed = float(a.get("borrowed", 0))
            interest = float(a.get("interest", 0))
            break

    debt = borrowed + interest
    if debt <= 0:
        print("ℹ️ No hay deuda que repagar (parcial).")
        return {"status": "NO_DEBT"}

    repay_raw = min(debt, amount)
    repay_dec = Decimal(str(repay_raw)).quantize(Decimal("1.00"), rounding=ROUND_DOWN)
    if repay_dec <= Decimal("0"):
        print(f"ℹ️ Deuda parcial muy pequeña tras truncar: {repay_dec}")
        return {"status": "NO_DEBT_TRUNC"}

    repay_str = str(repay_dec)
    print(f"💰 Repagando deuda parcial USDT: {repay_str}")

    if DRY_RUN:
        return {"status": "DRY_RUN", "debt": float(repay_dec)}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=repay_str)
        print(f"💰 Repay parcial ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repay parcial: {e}")
        return {"status": "REPAY_FAILED", "error": str(e)}


# =============================================================
# 4) EXECUTE MARKET BUY / SELL
# =============================================================

def place_margin_buy(symbol, notional):
    """
    BUY en Margin usando quoteOrderQty.
    """
    print(f"➡️ Ejecutando MARKET BUY Margin {symbol} notional={notional:.6f}")

    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        qty = notional / price if price > 0 else 0.0
        print(f"💤 DRY_RUN BUY qty≈{qty:.6f}")
        return {"executedQty": qty, "cummulativeQuoteQty": notional, "price": price}

    try:
        res = client.create_margin_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quoteOrderQty=str(notional),
            isIsolated="FALSE",
        )
        print(f"🟣 BUY ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR BUY: {e}")
        return {"error": str(e)}


def place_margin_sell(symbol, qty):
    """
    SELL margin usando cantidad.
    """
    print(f"➡️ Ejecutando MARKET SELL Margin {symbol} qty={qty:.6f}")

    if DRY_RUN or not BINANCE_ENABLED:
        price = _get_price(symbol)
        return {"executedQty": qty, "cummulativeQuoteQty": qty * price, "price": price}

    try:
        res = client.create_margin_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            quantity=str(qty),
            isIsolated="FALSE",
        )
        print(f"🟣 SELL ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR SELL: {e}")
        return {"error": str(e)}


# =============================================================
# 5) HANDLE BUY SIGNAL — *IRONCLAD V5*
# =============================================================

def handle_margin_buy_signal(symbol):
    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    weight = PORTFOLIO_WEIGHTS.get(symbol, 0)
    if weight <= 0:
        print("⚠️ Sin weight definido")
        return {"status": "NO_WEIGHT"}

    margin_equity = get_margin_equity_usdt()
    spot_equity = _get_spot_equity_usdt()

    # Equity base → Margin si existe, de lo contrario Spot
    equity_base = margin_equity if margin_equity > 0 else spot_equity
    print(f"ℹ️ Margin equity={margin_equity:.2f} | Spot equity={spot_equity:.2f}")
    print(f"ℹ️ Usando equity_base={equity_base:.2f}")

    base_target = equity_base * weight
    trade_raw = base_target * MARGIN_MULTIPLIER
    print(f"🧮 base_target={base_target:.2f} → trade_raw≈{trade_raw:.2f}")

    # Filtros del símbolo
    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick"]))
    min_notional = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

    # Redondeo a tick → requerido por Binance
    clean_notional = float((Decimal(str(trade_raw)) // tick) * tick)
    print(f"🔧 clean_notional={clean_notional:.6f} (min_required={min_notional})")

    if clean_notional < min_notional:
        print("❌ Trade demasiado pequeño")
        return {"status": "too_small"}

    # Safe Notional IRONCLAD
    safe_notional = clean_notional * 0.9995
    safe_notional = float((Decimal(str(safe_notional)) // tick) * tick)
    print(f"🧱 SAFE notional={safe_notional:.6f}")

    if safe_notional < min_notional:
        print("❌ SAFE notional < min_notional")
        return {"status": "too_small_safe"}

    # Controles de riesgo
    mlevel = get_margin_level()
    if mlevel < 2.0:
        print(f"❌ Margin level bajo: {mlevel}")
        return {"status": "risk_margin_level"}

    borrow_ratio = get_total_borrow_used_ratio()
    if borrow_ratio > 0.40:
        print(f"❌ Borrow usage alto: {borrow_ratio}")
        return {"status": "risk_borrow_limit"}

    # ---------------------------------------------------------
    # Borrow si hace falta
    # ---------------------------------------------------------
    borrow_res = borrow_if_needed("USDT", safe_notional)
    if borrow_res.get("status") == "BORROW_FAILED":
        print("❌ Abort BUY por error en borrow")
        return {"status": "borrow_failed", "detail": borrow_res}

    borrowed_amount = float(borrow_res.get("borrowed", 0.0))

    # Releer free USDT tras el borrow, como si el usuario mirara su balance
    free_after = _get_margin_free_usdt()
    print(f"💵 USDT libre tras borrow (antes de BUY): {free_after:.6f}")

    # Ajustar notional al máximo posible con los fondos realmente disponibles
    effective_notional = min(safe_notional, free_after)
    effective_notional = float((Decimal(str(effective_notional)) // tick) * tick)
    print(f"🧮 effective_notional={effective_notional:.6f} (ajustado a fondos reales)")

    if effective_notional < min_notional:
        print("❌ effective_notional < min_notional después de borrow.")
        if borrowed_amount > 0:
            print("🔁 Repagando borrow porque no se puede ejecutar un trade válido.")
            _repay_usdt_amount(borrowed_amount)
        return {"status": "too_small_after_borrow", "borrowed": borrowed_amount}

    # Ejecutar BUY con el notional ajustado
    res = place_margin_buy(symbol, effective_notional)
    if "error" in res:
        print("❌ BUY falló")
        # Si falló el BUY y se había hecho borrow, repagamos ese monto
        if borrowed_amount > 0:
            print("🔁 Repagando borrow porque el BUY falló.")
            _repay_usdt_amount(borrowed_amount)
        return {"status": "buy_failed", "detail": res, "borrowed": borrowed_amount}

    qty = float(res.get("executedQty", 0))
    quote = float(res.get("cummulativeQuoteQty", effective_notional))
    entry_price = quote / qty if qty > 0 else _get_price(symbol)

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
        "status": "OPEN",
        "trade_mode": "MARGIN",
    })

    print(f"🟣 BUY completado qty={qty:.6f} entry={entry_price:.6f}")
    return res


# =============================================================
# 6) HANDLE SELL SIGNAL — SELL 100% DEL MARGIN
# =============================================================

def handle_margin_sell_signal(symbol):
    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    asset = symbol.replace("USDT", "")

    qty_avail = _get_margin_free_asset(asset)
    print(f"ℹ️ {asset} disponible en Margin ≈ {qty_avail:.8f}")

    if qty_avail <= 0:
        print("⚠️ No hay posición en Margin")
        return {"status": "NO_POSITION_MARGIN"}

    filters = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty_avail, filters["step"])

    print(f"🔧 qty_clean={qty_clean:.8f}")

    if qty_clean <= 0:
        print("❌ qty_clean inválida")
        return {"status": "INVALID_QTY"}

    # Ejecutar SELL
    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ SELL falló")
        return sell_res

    executed = float(sell_res.get("executedQty", qty_clean))
    quote = float(sell_res.get("cummulativeQuoteQty", 0))
    sell_price = quote / executed if executed > 0 else _get_price(symbol)

    # Buscar último trade abierto en Sheets
    trades = ws_trades.get_all_records()
    opens = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

    entry_price = sell_price
    row_idx = None

    if opens:
        margin_trades = [t for t in opens if str(t.get("trade_mode", "")).upper() == "MARGIN"]
        last = margin_trades[-1] if margin_trades else opens[-1]
        entry_price = float(last["entry_price"])
        row_idx = trades.index(last) + 2

    profit = (sell_price - entry_price) * executed

    # Repagar toda la deuda restante (modo Opción B)
    _repay_all_usdt_debt()

    free_usdt = _get_margin_free_usdt()
    print(f"💵 USDT libre tras SELL: {free_usdt:.6f}")
    print("🟣 Capital permanece en Margin (Opción B).")

    # Actualizar Sheets
    if row_idx:
        ws_trades.update(
            f"G{row_idx}:J{row_idx}",
            [[sell_price, datetime.utcnow().isoformat(), profit, "CLOSED"]]
        )
        print(f"📑 Sheets actualizado fila {row_idx}. Profit={profit:.6f}")

    print("🔴 SELL completado.")
    return sell_res
