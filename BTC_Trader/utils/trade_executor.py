import os
import time
import logging
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Configura logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Inicializa el cliente de Binance
api_key = os.getenv("BINANCE_API_KEY")
api_secret = os.getenv("BINANCE_API_SECRET")
client = Client(api_key, api_secret)

# Pesos predefinidos por símbolo
weights = {
    "BTCUSDT": 0.4,
    "ETHUSDT": 0.3,
    "ADAUSDT": 0.1,
    "XRPUSDT": 0.2
}

def obtener_balance_usdt():
    try:
        balance = client.get_asset_balance(asset="USDT")
        return float(balance['free'])
    except BinanceAPIException as e:
        logging.error(f"Error al obtener balance USDT: {e.message}")
        return 0

def obtener_balance_moneda(moneda):
    try:
        balance = client.get_asset_balance(asset=moneda)
        return float(balance['free'])
    except BinanceAPIException as e:
        logging.error(f"Error al obtener balance de {moneda}: {e.message}")
        return 0

def ejecutar_compra(symbol, peso):
    try:
        balance_usdt = obtener_balance_usdt()
        if balance_usdt == 0:
            logging.warning("Balance USDT es 0. No se puede comprar.")
            return

        monto_usdt = round(balance_usdt * peso, 2)
        orden = client.order_market_buy(symbol=symbol, quoteOrderQty=monto_usdt)
        logging.info(f"✅ Compra ejecutada: {symbol} por ${monto_usdt}")
        return orden
    except BinanceAPIException as e:
        logging.error(f"❌ Error al comprar {symbol}: {e.message}")


def ejecutar_venta(symbol):
    try:
        coin = symbol.replace("USDT", "")
        balance_moneda = obtener_balance_moneda(coin)
        if balance_moneda == 0:
            logging.warning(f"Balance en {coin} es 0. No se puede vender.")
            return

        orden = client.order_market_sell(symbol=symbol, quantity=round(balance_moneda, 4))
        logging.info(f"✅ Venta ejecutada: {symbol} - cantidad {balance_moneda}")
        return orden
    except BinanceAPIException as e:
        logging.error(f"❌ Error al vender {symbol}: {e.message}")


def ejecutar_operacion(symbol, nueva_senal, senal_anterior):
    if nueva_senal == 'BUY' and senal_anterior != 'BUY':
        ejecutar_compra(symbol, weights[symbol])
    elif nueva_senal == 'SELL' and senal_anterior != 'SELL':
        ejecutar_venta(symbol)
    else:
        logging.info(f"ℹ️ No se ejecutó operación para {symbol}. Señal sin cambios: {nueva_senal}")

