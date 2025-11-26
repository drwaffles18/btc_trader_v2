# =============================================================
# 🟢 Binance Spot Autotrader — Victor + GPT (versión estable sin OCO)
# -------------------------------------------------------------
# Objetivo:
# - BUY → Market buy usando quoteOrderQty (USDT)
# - SELL → Market sell del total del asset disponible
# - Respeta los pesos por símbolo
# - Log en /app/data/trade_log.csv
# =============================================================

import os
import math
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import pandas as pd

# Crear carpeta de logs
os.makedirs("/app/data", exist_ok=True)

# Archivo de logs fijo
LOG_FILE = "/app/data/trade_log.csv"

# DRY RUN (tiene que estar definido ANTES de usarlo en inicialización)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

# Intentamos importar Binance
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

# ===== Pesos por moneda (suman 1.0) =====
PORTFOLIO_WEIGHTS = {
    "BTCUSDT": 0.35,
    "ETHUSDT": 0.25,
    "ADAUSDT": 0.10,
    "XRPUSDT": 0.20,
    "BNBUSDT": 0.10,
}

DEFAULT_RISK_PCT = 0.01
DEFAULT_RR = 1.5
SL_TRIGGER_GAP = 0.05

# =============================
# 🔒 1) Inicialización segura del cliente Binance
# =============================

BINANCE_ENABLED = False
client = None

if not API_KEY or not API_SECRET or Client is None:
    print("⚠️ No hay claves Binance. Modo solo alertas.")
else:
    try:
        client = Client(API_KEY, API_SECRET)
        client.ping()
        BINANCE_ENABLED = True
        print("✅ Cliente Binance inicializado correctamente.")

    except Exception as e:
        print(f"⚠️ Error al iniciar Binance: {e}")

        # Log robusto
        try:
            pd.DataFrame([{
                "timestamp": datetime.utcnow().isoformat(),
                "symbol": "SYSTEM",
                "action": "BINANCE_INIT_ERROR",
                "message": str(e),
                "dry_run": DRY_RUN
            }]).to_csv(LOG_FILE, mode="a",
                       header=not os.path.exists(LOG_FILE),
                       index=False)
        except Exception as log_err:
            print(f"⚠️ Error al guardar log inicial: {log_err}")

        print("→ Continuando en modo solo alertas.")

# =============================
# 2) Utilitarios
# =============================

def _append_log(row: dict):
    df = pd.DataFrame([row])
    df.to_csv(LOG_FILE, mode="a",
              header=not os.path.exists(LOG_FILE),
              index=False)
    print(f"🧾 LOG → {row.get('action')} {row.get('symbol')} (DRY_RUN={DRY_RUN})")


def _round_step_size(value: float, step_size: float) -> float:
    if step_size == 0:
        return value

    precision = int(round(-math.log(step_size, 10), 0)) if step_size < 1 else 0

    dec_val  = Decimal(str(value))
    dec_step = Decimal(str(step_size))

    rounded = (dec_val // dec_step) * dec_step

    if precision > 0:
        rounded = rounded.quantize(Decimal(f"1e-{precision}"), rounding=ROUND_DOWN)
    else:
        rounded = rounded.quantize(Decimal("1"), rounding=ROUND_DOWN)

    return float(rounded)


def _get_symbol_filters(symbol: str):
    """Obtiene filters: LOT_SIZE, MIN_NOTIONAL, PRICE_FILTER."""
    if not BINANCE_ENABLED:
        return {"step_size": 0.000001, "min_qty": 0.0, "tick_size": 0.01, "min_notional": 10.0}

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


def _get_free_balance(asset: str) -> float:
    if not BINANCE_ENABLED:
        return 1000.0 if asset == "USDT" else 0.0

    for b in client.get_account()["balances"]:
        if b["asset"] == asset:
            return float(b["free"])
    return 0.0


def _get_price(symbol: str) -> float:
    if not BINANCE_ENABLED:
        return 0.0
    t = client.get_symbol_ticker(symbol=symbol)
    return float(t["price"])


def _get_spot_equity_usdt() -> float:
    """USDT total = balance USDT + valor de todas las criptos en USDT."""
    if not BINANCE_ENABLED:
        return 1000.0

    acc = client.get_account()
    balances = {b["asset"]: float(b["free"]) + float(b["locked"])
                for b in acc["balances"]}

    total = balances.get("USDT", 0.0)

    for asset, qty in balances.items():
        if asset in ("USDT", "BUSD", "FDUSD"):
            continue
        if qty <= 0:
            continue
        symbol = f"{asset}USDT"
        try:
            price = _get_price(symbol)
            total += qty * price
        except:
            pass

    return total

# =============================
# 3) Órdenes
# =============================

def place_market_buy_by_quote(symbol, usdt_amount):
    """Market BUY usando quoteOrderQty, redondeado a tick_size."""
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    filters = _get_symbol_filters(symbol)
    tick = Decimal(str(filters["tick_size"]))
    amt  = Decimal(str(usdt_amount))

    # Redondeo seguro → evita error -1111
    usdt_clean = float((amt // tick) * tick)

    if DRY_RUN:
        price = _get_price(symbol)
        qty = usdt_clean / price
        return {"symbol": symbol, "status": "FILLED", "executedQty": qty, "price": price}

    return client.create_order(
        symbol=symbol,
        side=SIDE_BUY,
        type=ORDER_TYPE_MARKET,
        quoteOrderQty=str(usdt_clean)
    )


def sell_all_market(symbol):
    """Vender TODO el balance de un asset."""
    if not BINANCE_ENABLED:
        return {"status": "SKIPPED", "dry_run": DRY_RUN}

    asset = symbol.replace("USDT", "")
    qty = _get_free_balance(asset)
    if qty <= 0:
        return {"status": "NO_POSITION"}

    filters = _get_symbol_filters(symbol)
    q = _round_step_size(qty, filters["step_size"])

    if DRY_RUN:
        return {"symbol": symbol, "status": "SIMULATED", "qty": q}

    return client.create_order(
        symbol=symbol,
        side=SIDE_SELL,
        type=ORDER_TYPE_MARKET,
        quantity=str(q)
    )

# =============================
# 4) Señales BUY / SELL
# =============================

def handle_buy_signal(symbol):
    """BUY sin OCO."""
    try:
        if not BINANCE_ENABLED:
            print(f"⚠️ BUY SKIPPED {symbol} (no keys)")
            return

        equity = _get_spot_equity_usdt()
        free_usdt = _get_free_balance("USDT")
        weight = PORTFOLIO_WEIGHTS.get(symbol, 0)

        usdt_to_spend = min(equity * weight, free_usdt)

        filters = _get_symbol_filters(symbol)
        if usdt_to_spend < filters["min_notional"]:
            return {"status": "INSUFFICIENT_USDT"}

        print(f"🟢 BUY {symbol} por {usdt_to_spend:.2f} USDT…")
        order = place_market_buy_by_quote(symbol, usdt_to_spend)

        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "BUY",
            "usdt_spent": usdt_to_spend,
            "entry_price": order.get("price"),
            "qty": order.get("executedQty"),
            "dry_run": DRY_RUN
        })

        return order

    except Exception as e:
        print(f"❌ Error BUY {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "ERROR_BUY",
            "message": str(e),
            "dry_run": DRY_RUN
        })


def handle_sell_signal(symbol):
    """SELL (market)."""
    try:
        if not BINANCE_ENABLED:
            print(f"⚠️ SELL SKIPPED {symbol} (no keys)")
            return

        print(f"🔴 SELL {symbol}…")
        res = sell_all_market(symbol)

        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "SELL",
            "qty": res.get("origQty") if isinstance(res, dict) else None,
            "dry_run": DRY_RUN
        })

        return res

    except Exception as e:
        print(f"❌ Error SELL {symbol}: {e}")
        _append_log({
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "action": "ERROR_SELL",
            "message": str(e),
            "dry_run": DRY_RUN
        })

# =============================
# 5) Enrutador
# =============================

def route_signal(signal: dict):
    side = signal.get("side", "").upper()
    symbol = signal.get("symbol")

    if side == "BUY":
        return handle_buy_signal(symbol)
    elif side == "SELL":
        return handle_sell_signal(symbol)
    else:
        return {"status": "IGNORED", "detail": "side no soportado"}
