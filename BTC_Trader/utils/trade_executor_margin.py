# =============================================================
# 🟣 Binance Cross Margin Autotrader — Victor + GPT (Opción B)
# -------------------------------------------------------------
# - Cuenta principal: Cross Margin (3x)
# - No se transfiere nada Spot <-> Margin.
# - Flujo:
#     * BUY:
#         - Usa equity de Margin (o Spot si Margin está vacío, solo como fallback)
#         - Calcula tamaño base por peso de portafolio
#         - Aplica MARGIN_MULTIPLIER (ej. 3x)
#         - Redondea notional y ejecuta MARKET BUY en Margin
#         - Si no hay suficiente USDT en Margin, usa borrow USDT
#         - Registra en Google Sheets con trade_mode = "MARGIN"
#     * SELL:
#         - Vende 100% de la posición disponible en Margin para ese símbolo
#         - Calcula profit usando el último trade OPEN en Sheets
#         - Repaga toda la deuda de USDT en Margin
#         - NO transfiere nada a Spot (capital permanece en Margin)
#         - Actualiza fila en Sheets (exit_price, exit_time, profit, status)
#
# - Este módulo se usa solo cuando USE_MARGIN = true en el router.
#   El router debe llamar:
#       handle_margin_buy_signal(symbol)
#       handle_margin_sell_signal(symbol)
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN

from utils.google_client import get_gsheet_client

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

# Multiplicador de tamaño vs "peso spot"
MARGIN_MULTIPLIER = float(os.getenv("MARGIN_MULTIPLIER", "3.0"))

# Piso mínimo de notional por trade
BINANCE_NOTIONAL_FLOOR = 5.0

# Pesos de portafolio (mismo criterio que Spot)
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
    Columnas esperadas en 'Trades':
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
    """
    Redondea 'value' al múltiplo inferior de 'step_size' (LOT_SIZE).
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


def _get_symbol_filters(symbol: str):
    """
    Obtiene LOT_SIZE, TICK_SIZE y MIN_NOTIONAL para el símbolo.
    """
    if not BINANCE_ENABLED:
        return {"step": 0.000001, "tick": 0.01, "min_notional": BINANCE_NOTIONAL_FLOOR}

    info = client.get_symbol_info(symbol)
    filters = {f["filterType"]: f for f in info["filters"]}

    lot = filters.get("LOT_SIZE", {})
    min_not = filters.get("MIN_NOTIONAL", {})
    price = filters.get("PRICE_FILTER", {})

    return {
        "step": float(lot.get("stepSize", 0)),
        "tick": float(price.get("tickSize", 0)) or 0.01,
        "min_notional": float(min_not.get("minNotional", 0)) if min_not else BINANCE_NOTIONAL_FLOOR,
    }


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


# ================== Spot helpers (fallback mínimo) ==================

def _get_spot_equity_usdt() -> float:
    """
    Equity total en Spot en USDT (USDT + otros assets valorados en USDT).
    En Opción B casi no se usa, pero sirve de fallback si Margin está vacío.
    """
    if not BINANCE_ENABLED:
        return 0.0

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


# ================== Margin helpers ==================

def _get_margin_account():
    if not BINANCE_ENABLED:
        return {}
    return client.get_margin_account()


def get_margin_equity_usdt() -> float:
    """
    Equity total del margin account en USDT.
    Binance lo da en BTC → lo convertimos a USDT.
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    total_asset_btc = float(acc.get("totalAssetOfBtc", 0.0))
    btc_price = _get_price("BTCUSDT") or 0.0
    return total_asset_btc * btc_price


def get_margin_level() -> float:
    """
    Margin level = totalAssetOfBtc / totalLiabilityOfBtc.
    Si no hay deuda, devolvemos un valor grande (ej. 99).
    """
    if not BINANCE_ENABLED:
        return 99.0

    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0.0))
    liability = float(acc.get("totalLiabilityOfBtc", 0.0))
    if liability <= 0:
        return 99.0
    return assets / liability


def get_total_borrow_used_ratio() -> float:
    """
    borrow_used_ratio = liability / asset.
    Ejemplo: 0.27 → 27% de uso de borrow.
    """
    if not BINANCE_ENABLED:
        return 0.0

    acc = client.get_margin_account()
    assets = float(acc.get("totalAssetOfBtc", 0.0))
    liability = float(acc.get("totalLiabilityOfBtc", 0.0))
    if assets <= 0:
        return 1.0  # máximo riesgo conceptual
    return liability / assets


def _get_margin_free_usdt() -> float:
    """
    USDT libre en cuenta Margin.
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            return float(a.get("free", 0.0))
    return 0.0


def _get_margin_free_asset(asset: str) -> float:
    """
    Cantidad libre en Margin para un asset específico (ej. 'ADA', 'XRP').
    """
    if not BINANCE_ENABLED:
        return 0.0
    acc = client.get_margin_account()
    for a in acc.get("userAssets", []):
        if a["asset"] == asset:
            return float(a.get("free", 0.0))
    return 0.0


# =============================================================
# 3) BORROW / REPAY
# =============================================================

def _clean_amount_for_asset(asset: str, amount: float) -> float:
    """
    Limpia el número de decimales según el asset.
    - USDT: máx 3 decimales (por seguridad en margin loan/repay).
    - Otros: se deja igual.
    """
    if amount <= 0:
        return 0.0

    if asset.upper() == "USDT":
        dec = Decimal(str(amount))
        return float(dec.quantize(Decimal("0.001"), rounding=ROUND_DOWN))

    return amount


def borrow_if_needed(asset: str, notional_required_usdt: float):
    """
    Si no hay suficiente USDT libre en Margin, pide prestado la diferencia.
    Aplica limpieza de decimales para evitar:
      APIError(code=-1100): Illegal characters found in a parameter.
    """
    if not BINANCE_ENABLED:
        print("⚠️ borrow_if_needed: Margin no habilitado.")
        return {"status": "DISABLED"}

    free_usdt = _get_margin_free_usdt()
    missing = max(0.0, notional_required_usdt - free_usdt)

    # 🔧 Fix: limpiar decimales de la cantidad a pedir prestada
    missing_clean = _clean_amount_for_asset(asset, missing)

    print(
        f"💳 borrow_if_needed → free USDT margin={free_usdt:.4f}, "
        f"required={notional_required_usdt:.4f}, missing_raw={missing:.6f}, "
        f"missing_clean={missing_clean:.6f}"
    )

    if missing_clean <= 0:
        print("ℹ️ No hace falta borrow, hay USDT suficiente en Margin.")
        return {"status": "NO_BORROW_NEEDED", "free_usdt": free_usdt}

    if DRY_RUN:
        print(f"💤 DRY_RUN borrow {asset} amount={missing_clean:.4f}")
        return {"status": "DRY_RUN", "asset": asset, "amount": missing_clean}

    try:
        res = client.create_margin_loan(asset=asset, amount=str(missing_clean))
        print(f"🟣 Borrow ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR borrow USDT: {e}")
        return {"status": "BORROW_FAILED", "error": str(e)}


def _repay_all_usdt_debt():
    """
    Repaga toda la deuda USDT en Margin (borrow + intereses).
    Aplica limpieza de decimales para evitar errores de parámetro.
    """
    if not BINANCE_ENABLED:
        print("⚠️ _repay_all_usdt_debt: Margin no habilitado.")
        return {"status": "DISABLED"}

    acc = client.get_margin_account()
    debt = 0.0
    for a in acc.get("userAssets", []):
        if a["asset"] == "USDT":
            borrowed = float(a.get("borrowed", 0.0))
            interest = float(a.get("interest", 0.0))
            debt = borrowed + interest
            break

    if debt <= 0:
        print("ℹ️ No hay deuda USDT que repagar.")
        return {"status": "NO_DEBT"}

    debt_clean = _clean_amount_for_asset("USDT", debt)

    if debt_clean <= 0:
        print("ℹ️ Deuda limpiada <= 0, nada que repagar.")
        return {"status": "NO_DEBT_CLEAN"}

    if DRY_RUN:
        print(f"💤 DRY_RUN repay USDT debt={debt_clean:.4f}")
        return {"status": "DRY_RUN", "action": "REPAY", "asset": "USDT", "amount": debt_clean}

    try:
        res = client.repay_margin_loan(asset="USDT", amount=str(debt_clean))
        print(f"💰 Repay USDT ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR repaying USDT debt: {e}")
        return {"status": "REPAY_FAILED", "error": str(e)}


# =============================================================
# 4) MARKET BUY / SELL EN MARGIN
# =============================================================

def place_margin_buy(symbol: str, usdt_amount: float):
    """
    Market BUY en Cross Margin usando quoteOrderQty.
    """
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
            isIsolated="FALSE",
        )
        print(f"🟣 Margin BUY ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR margin buy: {e}")
        return {"error": str(e)}


def place_margin_sell(symbol: str, qty: float):
    """
    Market SELL en Cross Margin usando quantity.
    """
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
            isIsolated="FALSE",
        )
        print(f"🟣 Margin SELL ejecutado: {res}")
        return res
    except Exception as e:
        print(f"❌ ERROR margin sell: {e}")
        return {"error": str(e)}


# =============================================================
# 5) HANDLE BUY SIGNAL (MARGIN COMO CUENTA PRINCIPAL)
# =============================================================

def handle_margin_buy_signal(symbol: str):
    """
    BUY en Cross Margin:
      - Usa equity de Margin como base (si es > 0)
      - Fallback a equity Spot si Margin está vacío
      - Aplica peso de portafolio + MARGIN_MULTIPLIER
      - Controla min_notional, margin_level y borrow_ratio
      - Si hace falta, hace borrow USDT
      - Ejecuta market BUY y lo registra en Sheets (trade_mode = "MARGIN")
    """
    print(f"\n========== 🟣 MARGIN BUY {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    weight = PORTFOLIO_WEIGHTS.get(symbol, 0.0)
    if weight <= 0:
        print(f"⚠️ Sin weight definido para {symbol}, abort BUY.")
        return {"status": "NO_WEIGHT"}

    margin_equity = get_margin_equity_usdt()
    spot_equity = _get_spot_equity_usdt()

    # Usamos Margin como cuenta primaria; si estuviera vacío, usamos Spot como backup
    equity_base = margin_equity if margin_equity > 0 else spot_equity

    print(f"ℹ️ Margin equity ≈ {margin_equity:.2f} USDT | Spot equity ≈ {spot_equity:.2f} USDT")
    print(f"ℹ️ Usando equity_base ≈ {equity_base:.2f} USDT")

    base_target = equity_base * weight
    trade_notional_raw = base_target * MARGIN_MULTIPLIER

    print(f"🧮 {symbol}: base_target ≈ {base_target:.2f} → trade_notional_raw ≈ {trade_notional_raw:.2f}")

    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick"]))
    min_notional = max(filters["min_notional"], BINANCE_NOTIONAL_FLOOR)

    # Redondeo del notional al múltiplo del tick
    usdt_notional_clean = float((Decimal(str(trade_notional_raw)) // tick) * tick)
    print(f"🔧 Notional limpio (tick) ≈ {usdt_notional_clean:.4f} USDT (min_required={min_notional:.2f})")

    if usdt_notional_clean < min_notional:
        print("❌ Trade demasiado pequeño para margin.")
        return {"status": "too_small"}

    # Controles de riesgo
    mlevel = get_margin_level()
    if mlevel < 2.0:
        print(f"❌ MarginLevel peligroso: {mlevel:.4f}")
        return {"status": "risk_margin_level", "margin_level": mlevel}

    borrow_ratio = get_total_borrow_used_ratio()
    if borrow_ratio > 0.40:
        print(f"❌ Borrow ratio > 40%: {borrow_ratio:.4f}")
        return {"status": "risk_borrow_limit", "borrow_ratio": borrow_ratio}

    # Borrow si hace falta (con cantidad limpia)
    borrow_res = borrow_if_needed("USDT", usdt_notional_clean)
    if isinstance(borrow_res, dict) and borrow_res.get("status") in ("BORROW_FAILED", "DISABLED"):
        print(f"❌ ERROR en borrow USDT, abort BUY: {borrow_res}")
        return {"status": "borrow_failed", "detail": str(borrow_res)}

    # Ejecutar BUY
    res = place_margin_buy(symbol, usdt_notional_clean)
    if "error" in res:
        print(f"❌ Margin BUY falló, no se registra trade en Sheets.")
        return res

    executed_qty = float(res.get("executedQty", 0.0))
    quote_spent = float(res.get("cummulativeQuoteQty", usdt_notional_clean))

    if executed_qty > 0 and quote_spent > 0:
        entry_price = quote_spent / executed_qty
    else:
        entry_price = _get_price(symbol)

    trade_id = f"{symbol}_{datetime.utcnow().timestamp()}"

    append_trade_row_margin(ws_trades, {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": "BUY",
        "qty": executed_qty,
        "entry_price": entry_price,
        "entry_time": datetime.utcnow().isoformat(),
        "exit_price": "",
        "exit_time": "",
        "profit_usdt": "",
        "status": "OPEN",
        "trade_mode": "MARGIN",
    })

    print(f"🟣 Margin BUY completado. qty≈{executed_qty:.6f} entry≈{entry_price:.4f}")
    return res


# =============================================================
# 6) HANDLE SELL SIGNAL (VENDE TODO EN MARGIN)
# =============================================================

def handle_margin_sell_signal(symbol: str):
    """
    SELL en Cross Margin:
      - Ignora 'qty' que está guardado en Sheets.
      - Busca cuánto hay realmente en Margin para ese asset (free).
      - Ajusta cantidad por LOT_SIZE y vende TODO via MARKET SELL.
      - Calcula profit usando el último trade OPEN en Sheets (preferentemente MARGIN).
      - Repaga toda la deuda de USDT.
      - NO transfiere nada a Spot (capital se queda en Margin).
      - Actualiza la fila en Sheets (G:J).
    """
    print(f"\n========== 🔴 MARGIN SELL {symbol} ==========")

    if not BINANCE_ENABLED:
        print("⚠️ Margin no habilitado (no API keys).")
        return {"status": "DISABLED"}

    asset = symbol.replace("USDT", "")

    # 1. Cantidad real disponible en Margin
    qty_margin = _get_margin_free_asset(asset)
    print(f"ℹ️ {asset} free en Margin ≈ {qty_margin:.8f}")

    if qty_margin <= 0:
        print("⚠️ No hay posición disponible en Margin para vender.")
        return {"status": "NO_POSITION_MARGIN"}

    filters = _get_symbol_filters(symbol)
    qty_clean = _round_step_size(qty_margin, filters["step"])

    if qty_clean <= 0:
        print("⚠️ Qty limpia <= 0 tras aplicar LOT_SIZE, SELL abortado.")
        return {"status": "INVALID_QTY_CLEAN"}

    # 2. Ejecutar SELL en Margin
    sell_res = place_margin_sell(symbol, qty_clean)
    if "error" in sell_res:
        print("❌ Margin SELL falló, no se actualiza Sheets ni se calcula profit.")
        return sell_res

    executed_qty = float(sell_res.get("executedQty", qty_clean))
    quote_got = float(sell_res.get("cummulativeQuoteQty", 0.0))

    if executed_qty > 0 and quote_got > 0:
        sell_price = quote_got / executed_qty
    else:
        sell_price = _get_price(symbol)

    # 3. Buscar último trade OPEN en Sheets para ese símbolo (preferencia MARGIN)
    trades = ws_trades.get_all_records()
    open_trades = [t for t in trades if t["symbol"] == symbol and t["status"] == "OPEN"]

    entry_price = sell_price
    row_idx = None

    if open_trades:
        margin_trades = [t for t in open_trades if str(t.get("trade_mode", "")).upper() == "MARGIN"]
        if margin_trades:
            last = margin_trades[-1]
        else:
            last = open_trades[-1]

        row_idx = trades.index(last) + 2  # header + index base 1
        try:
            entry_price = float(last["entry_price"])
        except Exception:
            entry_price = sell_price  # fallback seguro

    profit = (sell_price - entry_price) * executed_qty

    # 4. Repagar toda la deuda USDT
    _repay_all_usdt_debt()

    # 5. No transferimos USDT a Spot (Opción B)
    free_usdt_margin = _get_margin_free_usdt()
    print(f"💵 USDT libre en Margin después del SELL: {free_usdt_margin:.4f}")
    print("🟣 Capital permanece en Margin (no se transfiere a Spot).")

    # 6. Actualizar Sheets si encontramos trade abierto
    if row_idx is not None:
        ws_trades.update(
            f"G{row_idx}:J{row_idx}",
            [[
                sell_price,
                datetime.utcnow().isoformat(),
                profit,
                "CLOSED"
            ]]
        )
        print(f"📑 Sheets actualizado fila {row_idx}. Profit ≈ {profit:.4f} USDT")
    else:
        print("⚠️ No se encontró trade OPEN en Sheets para cerrar; no se actualiza tabla.")

    print("🔴 Margin SELL completado.")
    return sell_res
