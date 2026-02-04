# =============================================================
# 🟣 Binance Cross Margin Autotrader — IRONCLAD (BNB-only)
# -------------------------------------------------------------
#  ✔ Usa cuenta Cross Margin como principal
#  ✔ No transfiere nada Spot ↔ Margin
#  ✔ Usa borrow cuando falta USDT
#  ✔ BUY calcula notional según equity_base * TRADE_WEIGHT * MARGIN_MULTIPLIER
#  ✔ Safe Notional IRONCLAD
#  ✔ SELL liquida el 100% disponible en Margin
#  ✔ Repaga deuda automáticamente
#  ✔ Registro de Trades en Google Sheets con trade_mode = "MARGIN"
#
# ✅ BNB-only guardrail:
#    - Solo ejecuta trades si symbol == TRADE_SYMBOL (default BNBUSDT)
#    - Sizing usa TRADE_WEIGHT (porcentaje del equity) en vez de PORTFOLIO multi-asset
#
# ✅ “3x respeta”:
#    - MARGIN_MULTIPLIER default = 3.0
# =============================================================

import os
import math
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from utils.google_client import get_gsheet_client

# -------------------------------------------------------------
#  Importar cliente Binance
# -------------------------------------------------------------
try:
    from binance.client import Client
except ImportError:
    Client = None


# =============================================================
# 0) CONFIGURACIÓN GENERAL
# =============================================================

API_KEY = os.getenv("BINANCE_API_KEY_TRADING") or os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET_TRADING") or os.getenv("BINANCE_API_SECRET")

DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# ✅ Single-asset mode
TRADE_SYMBOL = os.getenv("TRADE_SYMBOL", "BNBUSDT").upper()
TRADE_WEIGHT = float(os.getenv("TRADE_WEIGHT", "1.0"))
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

# Multiplicador del tamaño base (para respetar 3x)
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Piso mínimo real de notional por trade
BINANCE_NOTIONAL_FLOOR = 5.0

client = None
BINANCE_ENABLED = False

if API_KEY and API_SECRET and Client:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print(
            f"✅ Margin Client OK — IRONCLAD (BNB-only) | "
            f"TRADE_SYMBOL={TRADE_SYMBOL} TRADE_WEIGHT={TRADE_WEIGHT} MARGIN_MULTIPLIER={MARGIN_MULTIPLIER}"
        )
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


def _get_spot_equity_usdt():
    if not BINANCE_ENABLED:
        return 0.0

    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"]) for b in acc["balances"]}

    total = balances.get("USDT", 0.0)
    for asset, qty in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD") or qty <= 0:
            continue
        sym = f"{asset}USDT"
        price = _get_price(sym)
        total += qty * price
    return total


# -------------------------------------------------------------
# Margin helpers
# -------------------------------------------------------------

def get_margin_equity_usdt():
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


# =============================================================
# 3) BORROW / REPAY
# =============================================================

def borrow_if_needed(asset, required_usdt):
    free = _get_margin_free_usdt()
    missing_raw = required_usdt - free
    missing_clean = max(0.0, missing_raw)

    print(
        f"💳 borrow_if_needed → free={free:.6f}, required={required_usdt:.6f}, "
        f"missing_raw={missing_raw:.6f}, missing_clean={missing_clean:.6f}"
    )

    if missing_clean <= 0:
        return {"status": "NO_BORROW_NEEDED", "free": free}

    missing_clean = float(
        Decimal(str(missing_clean)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN)
    )
    print(f"🔧 borrow amount ajustado={missing_clean}")

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

    debt_clean = float(
        Decimal(str(debt)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN)
    )

    print(f"💰 Repagando deuda total USDT (clean) = {debt_clean:.6f}")

    if DRY_RUN:
        print(f"💤 DRY_RUN repay {debt_clean}")
        return {"status": "DRY_RUN", "debt": debt_clean}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=str(debt_clean))
        print(f"💰 Repay ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repay: {e} (amount={debt_clean})")
        return {"status": "REPAY_FAILED", "error": str(e), "amount": debt_clean}


def _wait_for_real_balance(required, retries=6, delay=0.5):
    if DRY_RUN or not BINANCE_ENABLED:
        return True

    epsilon = 0.01
    for i in range(retries):
        free_now = _get_margin_free_usdt()
        print(f"⏳ Real balance check {i+1}/{retries} → free={free_now:.6f}, required={required:.6f}")
        if free_now + epsilon >= required:
            return True
        time.sleep(delay)

    print("❌ Balance real nunca alcanzó el requerido.")
    return False


# =============================================================
# 4) EXECUTE MARKET BUY / SELL
# =============================================================

def place_margin_buy(symbol, notional):
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
# 5) HANDLE BUY SIGNAL — IRONCLAD (BNB-only, 3x default)
# =============================================================

def handle_margin_buy_signal(symbol):
    symbol = (symbol or "").upper()
    print(f"\n========== 🟣 MARGIN BUY {symbol} — IRONCLAD (BNB-only) ==========")

    # ✅ Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ MARGIN IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}")
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    # ✅ Sizing por TRADE_WEIGHT (en vez de portafolio)
    if TRADE_WEIGHT <= 0:
        print("⚠️ TRADE_WEIGHT <= 0")
        return {"status": "NO_WEIGHT"}

    margin_equity = get_margin_equity_usdt()
    spot_equity = _get_spot_equity_usdt()

    equity_base = margin_equity if margin_equity > 0 else spot_equity
    print(f"ℹ️ Margin equity={margin_equity:.2f} | Spot equity={spot_equity:.2f}")
    print(f"ℹ️ Usando equity_base={equity_base:.2f} | TRADE_WEIGHT={TRADE_WEIGHT:.2f} | MARGIN_MULTIPLIER={MARGIN_MULTIPLIER:.2f}")

    base_target = equity_base * TRADE_WEIGHT
    trade_raw = base_target * MARGIN_MULTIPLIER
    print(f"🧮 base_target={base_target:.2f} → trade_raw≈{trade_raw:.2f}")

    # Guardrail por margin level (tu guardrail real)
    mlevel_before = get_margin_level()
    print(f"📊 Risk snapshot pre-trade → margin_level={mlevel_before:.2f}")

    if mlevel_before < 1.50:
        print(f"❌ Margin level bajo REAL: {mlevel_before:.2f}")
        return {"status": "risk_margin_level", "margin_level": mlevel_before}

    # Filtros del símbolo
    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick"]))
    min_notional = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

    # Redondeo a tick
    clean_notional = float((Decimal(str(trade_raw)) // tick) * tick)
    print(f"🔧 clean_notional={clean_notional:.6f} (min_required={min_notional})")

    if clean_notional < min_notional:
        print("❌ Trade demasiado pequeño")
        return {"status": "too_small"}

    # SAFE notional
    safe_notional = clean_notional * 0.9995
    safe_notional = float((Decimal(str(safe_notional)) // tick) * tick)
    print(f"🧱 SAFE notional={safe_notional:.6f}")

    if safe_notional < min_notional:
        print("❌ SAFE notional < min_notional")
        return {"status": "too_small_safe"}

    # ¿Necesita borrow?
    free_before = _get_margin_free_usdt()
    missing_raw = safe_notional - free_before
    needs_borrow = missing_raw > 0.0001

    print(
        f"🔍 Pre-borrow check → free_before={free_before:.6f}, "
        f"safe_notional={safe_notional:.6f}, missing_raw={missing_raw:.6f}, "
        f"needs_borrow={needs_borrow}"
    )

    # Borrow si hace falta
    borrow_res = borrow_if_needed("USDT", safe_notional)
    if borrow_res.get("status") == "BORROW_FAILED":
        print("❌ Abort BUY por error en borrow")
        return {"status": "borrow_failed", "detail": borrow_res}

    print("⏳ Borrow ejecutado — iniciando sincronización de balance real...")
    ok_balance = _wait_for_real_balance(safe_notional, retries=6, delay=0.5)
    if not ok_balance:
        print("❌ Abort BUY → El balance prestado no está disponible todavía.")
        return {"status": "borrow_balance_not_ready"}

    # Modo entero si dependía de borrow
    use_notional = safe_notional
    if needs_borrow:
        use_notional = float(math.floor(safe_notional))
        print(f"🧮 Modo borrow-safe → Ejecutando BUY con entero={use_notional:.2f}")

        if use_notional < min_notional:
            print("❌ use_notional entero < min_notional, abortando.")
            return {"status": "too_small_after_floor"}

    # BUY con retries
    last_res = None
    for attempt in range(1, 4):
        print(f"➡️ Intento BUY {attempt}/3 con notional={use_notional:.6f}...")
        res = place_margin_buy(symbol, use_notional)
        last_res = res

        if "error" not in res:
            break

        print(f"⚠️ BUY intento #{attempt} falló: {res['error']}")
        if DRY_RUN:
            break
        time.sleep(0.5)

    if last_res is None or "error" in last_res:
        print("❌ BUY falló incluso tras retries.")

        if needs_borrow:
            print("⚠️ BUY falló tras borrow — intentando repagar deuda...")
            _repay_all_usdt_debt()

        return last_res if last_res is not None else {"error": "unknown_buy_error"}

    res = last_res

    qty = float(res.get("executedQty", 0))
    quote = float(res.get("cummulativeQuoteQty", use_notional))
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

    mlevel_after = get_margin_level()
    print(f"📊 Risk snapshot post-trade → margin_level={mlevel_after:.2f}")
    print(f"🟣 BUY completado qty={qty:.6f} entry={entry_price:.6f}")

    return res


# =============================================================
# 6) HANDLE SELL SIGNAL — SELL 100% DEL MARGIN (BNB-only)
# =============================================================

def handle_margin_sell_signal(symbol):
    symbol = (symbol or "").upper()
    print(f"\n========== 🔴 MARGIN SELL {symbol} — IRONCLAD (BNB-only) ==========")

    # ✅ Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ MARGIN IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}")
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

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

    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ SELL falló")
        return sell_res

    executed = float(sell_res.get("executedQty", qty_clean))
    quote = float(sell_res.get("cummulativeQuoteQty", 0))
    sell_price = quote / executed if executed > 0 else _get_price(symbol)

    # Buscar último trade abierto en Sheets
    trades = ws_trades.get_all_records()
    opens = [t for t in trades if t.get("symbol") == symbol and t.get("status") == "OPEN"]

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
    print("🟣 Capital permanece en Margin.")

    # Actualizar Sheets
    if row_idx:
        ws_trades.update(
            f"G{row_idx}:J{row_idx}",
            [[sell_price, datetime.utcnow().isoformat(), profit, "CLOSED"]]
        )
        print(f"📑 Sheets actualizado fila {row_idx}. Profit={profit:.6f}")

    print("🔴 SELL completado.")
    return sell_res
