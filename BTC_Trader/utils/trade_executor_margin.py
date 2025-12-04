# =============================================================
# 🟣 Binance Cross Margin Autotrader V5 — Victor + GPT
# -------------------------------------------------------------
#  ✔ Usa cuenta Cross Margin como principal (Opción B)
#  ✔ No transfiere nada Spot ↔ Margin
#  ✔ Usa borrow cuando falta USDT
#  ✔ BUY calcula notional según portafolio × 2x  ⬅️⬅️ (ANTES 3x)
#  ✔ Safe Notional IRONCLAD (evita errores 1100/2010)
#  ✔ SELL liquida el 100% de lo que haya realmente en Margin
#  ✔ Repaga deuda automáticamente
#  ✔ Registro de Trades en Google Sheets con trade_mode = "MARGIN"
#  ✔ Debug extendido pero liviano (incluye snapshot de riesgo)
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
# 🔁 ANTES: default = "3.0"
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "2.0"))

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
        print("✅ Margin Client OK (initialization successful) — IRONCLAD V5 (2x)")
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
    """
    Devuelve liab / assets (en BTC). Sirve para ver qué fracción
    de los activos está financiada con deuda.
    """
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
    Incluye logs de debug.
    """
    free = _get_margin_free_usdt()
    missing_raw = required_usdt - free
    missing_clean = max(0.0, missing_raw)

    print(f"💳 borrow_if_needed → free={free:.6f}, required={required_usdt:.6f}, "
          f"missing_raw={missing_raw:.6f}, missing_clean={missing_clean:.6f}")

    if missing_clean <= 0:
        return {"status": "NO_BORROW_NEEDED", "free": free}

    if DRY_RUN:
        print(f"💤 DRY_RUN borrow {asset} {missing_clean}")
        return {"status": "DRY_RUN", "amount": missing_clean}

    try:
        res = client.create_margin_loan(asset=asset, amount=str(missing_clean))
        print(f"🟣 Borrow ejecutado correctamente: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR borrow {asset}: {e}")
        return {"status": "BORROW_FAILED", "error": str(e)}


def _repay_all_usdt_debt():
    """
    Repaga toda la deuda de USDT.
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

    print(f"💰 Repagando deuda total USDT: {debt:.6f}")

    if DRY_RUN:
        return {"status": "DRY_RUN", "debt": debt}

    try:
        res = client.repay_margin_loan(asset="USTO", amount=str(debt))
        print(f"💰 Repay ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repay: {e}")
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
# 5) HANDLE BUY SIGNAL — *IRONCLAD V5 (2x)*
# =============================================================

def handle_margin_buy_signal(symbol):
    print(f"\n========== 🟣 MARGIN BUY {symbol} — IRONCLAD V5 (2x) ==========")

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
    print(f"ℹ️ Usando equity_base={equity_base:.2f} | MARGIN_MULTIPLIER={MARGIN_MULTIPLIER:.2f}")

    base_target = equity_base * weight
    trade_raw = base_target * MARGIN_MULTIPLIER
    print(f"🧮 base_target={base_target:.2f} → trade_raw≈{trade_raw:.2f}")

    # Snapshot de riesgo antes de construir el notional final
    mlevel_before = get_margin_level()
    borrow_ratio = get_total_borrow_used_ratio()
    print(f"📊 Risk snapshot pre-trade → margin_level={mlevel_before:.2f}, borrow_ratio={borrow_ratio:.3f}")

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

    # Controles de riesgo (usamos los valores ya calculados)
    if mlevel_before < 2.0:
        print(f"❌ Margin level bajo: {mlevel_before:.2f}")
        return {"status": "risk_margin_level"}

    if borrow_ratio > 0.40:
        print(f"❌ Borrow usage alto: {borrow_ratio:.3f}")
        return {"status": "risk_borrow_limit"}

    # Borrow si hace falta
    borrow_res = borrow_if_needed("USDT", safe_notional)
    if borrow_res.get("status") == "BORROW_FAILED":
        print("❌ Abort BUY por error en borrow")
        return {"status": "borrow_failed", "detail": borrow_res}

    # Ejecutar BUY
    res = place_margin_buy(symbol, safe_notional)
    if "error" in res:
        print("❌ BUY falló")
        return res

    qty = float(res.get("executedQty", 0))
    quote = float(res.get("cummulativeQuoteQty", safe_notional))
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

    # Snapshot de riesgo post-trade (orientativo; puede variar ligeramente en Binance)
    mlevel_after = get_margin_level()
    borrow_ratio_after = get_total_borrow_used_ratio()
    print(f"📊 Risk snapshot post-trade → margin_level={mlevel_after:.2f}, borrow_ratio={borrow_ratio_after:.3f}")

    print(f"🟣 BUY completado qty={qty:.6f} entry={entry_price:.6f}")
    return res


# =============================================================
# 6) HANDLE SELL SIGNAL — SELL 100% DEL MARGIN
# =============================================================

def handle_margin_sell_signal(symbol):
    print(f"\n========== 🔴 MARGIN SELL {symbol} — IRONCLAD V5 ==========")

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

    # Repagar deuda
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
