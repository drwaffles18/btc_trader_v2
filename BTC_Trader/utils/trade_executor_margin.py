# =============================================================
# 🟣 Binance Cross Margin Autotrader — IRONCLAD (BNB-only)
# Victor + GPT — Margin Executor (FULL)
# -------------------------------------------------------------
# Objetivo (tu lógica actual):
#  ✅ Si USE_MARGIN=true (router) → operar SOLO con capital en Margin
#     - Sizing basado en equity en Margin (NO fallback a Spot)
#     - Target notional = margin_equity_usdt * TRADE_WEIGHT * MARGIN_MULTIPLIER (default 3x)
#     - Borrow = max(0, target_notional - free_usdt_margin)
#     - BUY con quoteOrderQty (USDT)
#     - SELL liquida 100% del asset en Margin
#     - Repaga deuda USDT automáticamente
#     - Capital queda en Margin
#
# Guardrails:
#  ✅ BNB-only (TRADE_SYMBOL + STRICT_TRADE_SYMBOL)
#  ✅ Margin level pre-trade mínimo (default 1.50)
#  ✅ Safe-notional (reduce un poco para evitar errores)
#
# Logging:
#  ✅ Google Sheets (Trades) con trade_mode="MARGIN"
#
# Notas técnicas:
#  - NO transfiere Spot↔Margin (tu colateral debe estar en Margin si quieres 3x "puro")
#  - No usa "tickSize" para redondear quoteOrderQty; redondea USDT a 2 decimales
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
TRADE_SYMBOL = os.getenv("TRADE_SYMBOL", "BNBUSDT").strip().upper()
TRADE_WEIGHT = float(os.getenv("TRADE_WEIGHT", "1.0"))
STRICT_TRADE_SYMBOL = os.getenv("STRICT_TRADE_SYMBOL", "true").lower() == "true"

# Multiplicador del tamaño base (para respetar 3x)
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Guardrail margen (si cae por debajo, abort)
MIN_MARGIN_LEVEL = float(os.getenv("MIN_MARGIN_LEVEL", "1.50"))

# Piso mínimo real de notional por trade (Market)
BINANCE_NOTIONAL_FLOOR = float(os.getenv("BINANCE_NOTIONAL_FLOOR", "5.0"))

# Ajuste safety para notional (reduce para evitar error por fees/redondeos)
SAFE_NOTIONAL_FACTOR = float(os.getenv("SAFE_NOTIONAL_FACTOR", "0.9995"))

# Usar net asset si existe (más cercano a "equity")
USE_NET_ASSET_FOR_EQUITY = os.getenv("USE_NET_ASSET_FOR_EQUITY", "true").lower() == "true"

client = None
BINANCE_ENABLED = False

if API_KEY and API_SECRET and Client:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print(
            f"✅ Margin Client OK — IRONCLAD (BNB-only) | "
            f"TRADE_SYMBOL={TRADE_SYMBOL} TRADE_WEIGHT={TRADE_WEIGHT} "
            f"MARGIN_MULTIPLIER={MARGIN_MULTIPLIER} MIN_MARGIN_LEVEL={MIN_MARGIN_LEVEL} "
            f"DRY_RUN={DRY_RUN}",
            flush=True
        )
    except Exception as e:
        print(f"❌ Error Margin Client: {e}", flush=True)
else:
    print("⚠️ Margin Client disabled (no API keys / binance lib missing)", flush=True)


# =============================================================
# 1) GOOGLE SHEETS INIT (lazy-safe)
# =============================================================

GSHEET_ID = (os.getenv("GOOGLE_SHEET_ID") or "").strip()
ws_trades = None

def _get_ws_trades():
    global ws_trades
    if ws_trades is not None:
        return ws_trades

    if DRY_RUN:
        # En DRY_RUN no queremos tocar sheets.
        return None

    if not GSHEET_ID:
        print("⚠️ GOOGLE_SHEET_ID no definido → Sheets logging deshabilitado.", flush=True)
        return None

    try:
        gs_client = get_gsheet_client()
        ws_trades = gs_client.open_by_key(GSHEET_ID).worksheet("Trades")
        return ws_trades
    except Exception as e:
        print(f"⚠️ No se pudo inicializar Google Sheets Trades: {e}", flush=True)
        return None


def append_trade_row_margin(row_dict: dict):
    ws = _get_ws_trades()
    if ws is None:
        return

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
    try:
        ws.append_row(row, value_input_option="RAW")
    except Exception as e:
        print(f"⚠️ Sheets append_row falló: {e}", flush=True)


# =============================================================
# 2) UTILS GENERALES
# =============================================================

def _round_step_size(value: float, step_size: float) -> float:
    """Redondea hacia abajo a step_size (para quantity)."""
    if step_size == 0:
        return float(value)
    dec_val = Decimal(str(value))
    dec_step = Decimal(str(step_size))
    rounded = (dec_val // dec_step) * dec_step
    # precision ≈ número de decimales del step
    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0
    if precision > 0:
        return float(rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN))
    return float(rounded)


def _round_usdt_2(value: float) -> float:
    """QuoteOrderQty en USDT: redondear hacia abajo a 2 decimales."""
    dec = Decimal(str(value)).quantize(Decimal("1.00"), rounding=ROUND_DOWN)
    return float(dec)


def _get_symbol_filters(symbol: str) -> dict:
    """
    Retorna filtros básicos:
      - step (LOT_SIZE stepSize) para qty
      - min_notional (MIN_NOTIONAL)
    """
    if not BINANCE_ENABLED:
        return {"step": 0.000001, "min_notional": BINANCE_NOTIONAL_FLOOR}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info.get("filters", [])}

    lot = filters.get("LOT_SIZE", {}) or {}
    min_notional = filters.get("MIN_NOTIONAL", {}) or {}

    return {
        "step": float(lot.get("stepSize", 0) or 0),
        "min_notional": float(min_notional.get("minNotional", BINANCE_NOTIONAL_FLOOR) or BINANCE_NOTIONAL_FLOOR),
    }


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    try:
        return float(client.get_symbol_ticker(symbol=symbol)["price"])
    except Exception:
        return 0.0


# =============================================================
# 3) MARGIN HELPERS
# =============================================================

def _get_margin_account():
    if not BINANCE_ENABLED:
        return None
    return client.get_margin_account()


def get_margin_equity_usdt() -> float:
    """
    Equity aproximada en USDT usando BTC como referencia.
    Preferimos totalNetAssetOfBtc si está disponible (más cercano a equity neta).
    """
    if not BINANCE_ENABLED:
        return 0.0

    acc = _get_margin_account()
    if not acc:
        return 0.0

    key = "totalNetAssetOfBtc" if USE_NET_ASSET_FOR_EQUITY else "totalAssetOfBtc"
    btc_equity = float(acc.get(key, 0) or 0)

    btc_price = _get_price("BTCUSDT")
    return btc_equity * btc_price


def _get_margin_free_usdt() -> float:
    if not BINANCE_ENABLED:
        return 0.0
    acc = _get_margin_account()
    if not acc:
        return 0.0
    for a in acc.get("userAssets", []):
        if a.get("asset") == "USDT":
            return float(a.get("free", 0) or 0)
    return 0.0


def _get_margin_free_asset(asset: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    acc = _get_margin_account()
    if not acc:
        return 0.0
    for a in acc.get("userAssets", []):
        if a.get("asset") == asset:
            return float(a.get("free", 0) or 0)
    return 0.0


def get_margin_level() -> float:
    """
    Margin level (assets / liabilities) en BTC terms.
    Si no hay liabilities, retornamos valor alto.
    """
    if not BINANCE_ENABLED:
        return 99.0
    acc = _get_margin_account()
    if not acc:
        return 99.0
    assets = float(acc.get("totalAssetOfBtc", 0) or 0)
    liab = float(acc.get("totalLiabilityOfBtc", 0) or 0)
    return 99.0 if liab == 0 else assets / liab


# =============================================================
# 4) BORROW / REPAY
# =============================================================

def borrow_if_needed(required_usdt: float) -> dict:
    """
    Borrow USDT si free_usdt_margin < required_usdt.
    """
    free = _get_margin_free_usdt()
    missing = max(0.0, required_usdt - free)

    print(
        f"💳 borrow_if_needed → free_usdt_margin={free:.6f}, required={required_usdt:.6f}, missing={missing:.6f}",
        flush=True
    )

    if missing <= 0:
        return {"status": "NO_BORROW_NEEDED", "free": free}

    # Binance espera string; mantenemos 6 decimales y floor
    missing_clean = float(Decimal(str(missing)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN))

    if DRY_RUN:
        print(f"💤 DRY_RUN borrow USDT {missing_clean}", flush=True)
        return {"status": "DRY_RUN_BORROW", "amount": missing_clean}

    try:
        res = client.create_margin_loan(asset="USDT", amount=str(missing_clean))
        print(f"🟣 Borrow ejecutado: {res}", flush=True)
        return res
    except Exception as e:
        print(f"❌ ERROR borrow USDT: {e}", flush=True)
        return {"status": "BORROW_FAILED", "error": str(e), "amount": missing_clean}


def _repay_all_usdt_debt() -> dict:
    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    acc = _get_margin_account()
    if not acc:
        return {"status": "DISABLED"}

    borrowed = 0.0
    interest = 0.0
    for a in acc.get("userAssets", []):
        if a.get("asset") == "USDT":
            borrowed = float(a.get("borrowed", 0) or 0)
            interest = float(a.get("interest", 0) or 0)
            break

    debt = borrowed + interest
    if debt <= 0:
        print("ℹ️ No hay deuda USDT que repagar.", flush=True)
        return {"status": "NO_DEBT"}

    debt_clean = float(Decimal(str(debt)).quantize(Decimal("1.000000"), rounding=ROUND_DOWN))
    print(f"💰 Repagando deuda USDT total={debt_clean:.6f}", flush=True)

    if DRY_RUN:
        print(f"💤 DRY_RUN repay USDT {debt_clean}", flush=True)
        return {"status": "DRY_RUN_REPAY", "debt": debt_clean}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=str(debt_clean))
        print(f"💰 Repay ejecutado: {res}", flush=True)
        return res
    except Exception as e:
        print(f"❌ ERROR repay USDT: {e} (amount={debt_clean})", flush=True)
        return {"status": "REPAY_FAILED", "error": str(e), "amount": debt_clean}


def _wait_for_real_balance(required: float, retries: int = 8, delay: float = 0.5) -> bool:
    """
    Espera a que free_usdt_margin refleje el borrow (o depósito) antes del BUY.
    """
    if DRY_RUN or not BINANCE_ENABLED:
        return True

    epsilon = 0.02  # pequeño margen
    for i in range(retries):
        free_now = _get_margin_free_usdt()
        print(f"⏳ Balance check {i+1}/{retries} → free={free_now:.6f} required={required:.6f}", flush=True)
        if free_now + epsilon >= required:
            return True
        time.sleep(delay)

    print("❌ Balance en Margin nunca alcanzó el requerido a tiempo.", flush=True)
    return False


# =============================================================
# 5) EXECUTE MARKET BUY / SELL (MARGIN)
# =============================================================

def place_margin_buy(symbol: str, notional_usdt: float) -> dict:
    """
    Cross Margin market BUY con quoteOrderQty.
    """
    print(f"➡️ MARKET BUY (Margin) {symbol} quoteOrderQty={notional_usdt:.2f}", flush=True)

    price = _get_price(symbol)

    if DRY_RUN or not BINANCE_ENABLED:
        qty = (notional_usdt / price) if price > 0 else 0.0
        print(f"💤 DRY_RUN BUY qty≈{qty:.6f} @price≈{price:.6f}", flush=True)
        return {"executedQty": qty, "cummulativeQuoteQty": notional_usdt, "price": price}

    try:
        return client.create_margin_order(
            symbol=symbol,
            side="BUY",
            type="MARKET",
            quoteOrderQty=str(_round_usdt_2(notional_usdt)),
            isIsolated="FALSE",
        )
    except Exception as e:
        print(f"❌ ERROR BUY: {e}", flush=True)
        return {"error": str(e)}


def place_margin_sell(symbol: str, qty: float) -> dict:
    """
    Cross Margin market SELL con quantity.
    """
    print(f"➡️ MARKET SELL (Margin) {symbol} qty={qty:.8f}", flush=True)

    price = _get_price(symbol)

    if DRY_RUN or not BINANCE_ENABLED:
        return {"executedQty": qty, "cummulativeQuoteQty": qty * price, "price": price}

    try:
        return client.create_margin_order(
            symbol=symbol,
            side="SELL",
            type="MARKET",
            quantity=str(qty),
            isIsolated="FALSE",
        )
    except Exception as e:
        print(f"❌ ERROR SELL: {e}", flush=True)
        return {"error": str(e)}


# =============================================================
# 6) HANDLE BUY SIGNAL — BNB-only, 3x default (MARGIN)
# =============================================================

def handle_margin_buy_signal(symbol: str) -> dict:
    symbol = (symbol or "").strip().upper()
    print(f"\n========== 🟣 MARGIN BUY {symbol} — IRONCLAD (BNB-only) ==========", flush=True)

    # ✅ Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ MARGIN IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    if TRADE_WEIGHT <= 0:
        print("⚠️ TRADE_WEIGHT <= 0 → no se opera.", flush=True)
        return {"status": "NO_WEIGHT"}

    # ✅ Equity SOLO en Margin (sin fallback a Spot)
    margin_equity = get_margin_equity_usdt()
    print(
        f"ℹ️ Margin equity={margin_equity:.2f} USDT | TRADE_WEIGHT={TRADE_WEIGHT:.2f} | MARGIN_MULTIPLIER={MARGIN_MULTIPLIER:.2f}",
        flush=True
    )

    if margin_equity <= 0:
        print("⛔ NO_MARGIN_COLLATERAL → No hay equity en Margin. No se opera en Margin.", flush=True)
        return {"status": "NO_MARGIN_COLLATERAL", "margin_equity": margin_equity}

    # Guardrail por margin level (pre-trade)
    mlevel_before = get_margin_level()
    print(f"📊 Risk snapshot pre-trade → margin_level={mlevel_before:.2f}", flush=True)

    if mlevel_before < MIN_MARGIN_LEVEL:
        print(f"❌ Margin level bajo: {mlevel_before:.2f} (< {MIN_MARGIN_LEVEL})", flush=True)
        return {"status": "RISK_MARGIN_LEVEL", "margin_level": mlevel_before}

    # Target notional (3x)
    base_target = margin_equity * TRADE_WEIGHT
    target_notional = base_target * MARGIN_MULTIPLIER
    print(f"🧮 base_target={base_target:.2f} → target_notional≈{target_notional:.2f}", flush=True)

    # Filtros
    filters = _get_symbol_filters(symbol)
    min_notional = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

    # Clean notional USDT (2 decimals) + safe
    clean_notional = _round_usdt_2(target_notional)
    if clean_notional < min_notional:
        print(f"❌ Trade demasiado pequeño: {clean_notional:.2f} < {min_notional:.2f}", flush=True)
        return {"status": "TOO_SMALL", "clean_notional": clean_notional, "min_required": min_notional}

    safe_notional = _round_usdt_2(clean_notional * SAFE_NOTIONAL_FACTOR)
    print(f"🧱 clean_notional={clean_notional:.2f} → SAFE notional={safe_notional:.2f} (min_required={min_notional:.2f})", flush=True)

    if safe_notional < min_notional:
        print("❌ SAFE notional < min_notional", flush=True)
        return {"status": "TOO_SMALL_SAFE", "safe_notional": safe_notional, "min_required": min_notional}

    # Borrow si hace falta (borrow = max(0, safe_notional - free_usdt_margin))
    free_before = _get_margin_free_usdt()
    missing = max(0.0, safe_notional - free_before)
    needs_borrow = missing > 0.02  # umbral pequeño

    print(
        f"🔍 Pre-borrow → free_usdt_margin={free_before:.6f}, safe_notional={safe_notional:.2f}, "
        f"missing={missing:.6f}, needs_borrow={needs_borrow}",
        flush=True
    )

    if needs_borrow:
        borrow_res = borrow_if_needed(safe_notional)
        if borrow_res.get("status") == "BORROW_FAILED":
            print("❌ Abort BUY por error en borrow.", flush=True)
            return {"status": "BORROW_FAILED", "detail": borrow_res}

        print("⏳ Borrow ejecutado — sincronizando balance...", flush=True)
        if not _wait_for_real_balance(safe_notional, retries=8, delay=0.5):
            print("❌ Abort BUY → balance no listo tras borrow.", flush=True)
            return {"status": "BORROW_BALANCE_NOT_READY"}

    # Si dependía de borrow, usar entero (opcional) para evitar errores de quoteOrderQty
    use_notional = safe_notional
    if needs_borrow:
        use_notional = float(math.floor(safe_notional))
        use_notional = _round_usdt_2(use_notional)
        print(f"🧮 borrow-safe → usando notional entero={use_notional:.2f}", flush=True)

        if use_notional < min_notional:
            print("❌ use_notional entero < min_notional, abortando.", flush=True)
            return {"status": "TOO_SMALL_AFTER_FLOOR", "use_notional": use_notional, "min_required": min_notional}

    # BUY con retries
    last_res = None
    for attempt in range(1, 4):
        print(f"➡️ Intento BUY {attempt}/3 con notional={use_notional:.2f}", flush=True)
        res = place_margin_buy(symbol, use_notional)
        last_res = res

        if "error" not in res:
            break

        print(f"⚠️ BUY intento #{attempt} falló: {res['error']}", flush=True)
        if DRY_RUN:
            break
        time.sleep(0.6)

    if last_res is None or "error" in last_res:
        print("❌ BUY falló incluso tras retries.", flush=True)

        # Si hubo borrow, intenta repagar deuda para no dejar préstamo abierto
        if needs_borrow:
            print("⚠️ BUY falló tras borrow — intentando repagar deuda...", flush=True)
            _repay_all_usdt_debt()

        return last_res if last_res is not None else {"error": "UNKNOWN_BUY_ERROR"}

    res = last_res

    qty = float(res.get("executedQty", 0) or 0)
    quote = float(res.get("cummulativeQuoteQty", use_notional) or use_notional)

    # Precio entrada
    entry_price = (quote / qty) if qty > 0 else _get_price(symbol)

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    append_trade_row_margin({
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
    print(f"📊 Risk snapshot post-trade → margin_level={mlevel_after:.2f}", flush=True)
    print(f"🟣 BUY completado qty={qty:.6f} entry={entry_price:.6f}", flush=True)

    return res


# =============================================================
# 7) HANDLE SELL SIGNAL — SELL 100% DEL MARGIN (BNB-only)
# =============================================================

def handle_margin_sell_signal(symbol: str) -> dict:
    symbol = (symbol or "").strip().upper()
    print(f"\n========== 🔴 MARGIN SELL {symbol} — IRONCLAD (BNB-only) ==========", flush=True)

    # ✅ Guardrail single-asset
    if STRICT_TRADE_SYMBOL and symbol != TRADE_SYMBOL:
        print(f"⛔ MARGIN IGNORE → {symbol} != TRADE_SYMBOL {TRADE_SYMBOL}", flush=True)
        return {"status": "IGNORED_SYMBOL", "symbol": symbol, "trade_symbol": TRADE_SYMBOL}

    if not BINANCE_ENABLED:
        return {"status": "DISABLED"}

    asset = symbol.replace("USDT", "").strip()
    qty_avail = _get_margin_free_asset(asset)
    print(f"ℹ️ {asset} disponible en Margin ≈ {qty_avail:.8f}", flush=True)

    if qty_avail <= 0:
        print("⚠️ No hay posición en Margin.", flush=True)
        # Aun así, si quedó deuda por cualquier razón, intenta repagar (opcional)
        _repay_all_usdt_debt()
        return {"status": "NO_POSITION_MARGIN"}

    filters = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty_avail, filters["step"])
    print(f"🔧 qty_clean={qty_clean:.8f} (step={filters['step']})", flush=True)

    if qty_clean <= 0:
        print("❌ qty_clean inválida.", flush=True)
        return {"status": "INVALID_QTY"}

    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ SELL falló.", flush=True)
        return sell_res

    executed = float(sell_res.get("executedQty", qty_clean) or qty_clean)
    quote = float(sell_res.get("cummulativeQuoteQty", 0) or 0)
    sell_price = (quote / executed) if executed > 0 else _get_price(symbol)

    # Buscar último trade abierto (Sheets)
    entry_price = sell_price
    row_idx = None

    ws = _get_ws_trades()
    if ws is not None:
        try:
            trades = ws.get_all_records()
            opens = [t for t in trades if str(t.get("symbol", "")).upper() == symbol and str(t.get("status", "")).upper() == "OPEN"]
            if opens:
                margin_opens = [t for t in opens if str(t.get("trade_mode", "")).upper() == "MARGIN"]
                last = margin_opens[-1] if margin_opens else opens[-1]
                entry_price = float(last.get("entry_price", sell_price) or sell_price)
                row_idx = trades.index(last) + 2  # + header
        except Exception as e:
            print(f"⚠️ No se pudo leer/filtrar Trades en Sheets: {e}", flush=True)

    profit = (sell_price - entry_price) * executed

    # Repagar deuda USDT
    _repay_all_usdt_debt()

    free_usdt = _get_margin_free_usdt()
    print(f"💵 USDT libre tras SELL: {free_usdt:.6f}", flush=True)
    print("🟣 Capital permanece en Margin.", flush=True)

    # Actualizar Sheets si hay fila
    if ws is not None and row_idx is not None:
        try:
            # G exit_price, H exit_time, I profit_usdt, J status
            ws.update(
                f"G{row_idx}:J{row_idx}",
                [[sell_price, datetime.utcnow().isoformat(), profit, "CLOSED"]]
            )
            print(f"📑 Sheets actualizado fila {row_idx}. Profit={profit:.6f}", flush=True)
        except Exception as e:
            print(f"⚠️ Sheets update falló: {e}", flush=True)

    print("🔴 SELL completado.", flush=True)
    return sell_res
