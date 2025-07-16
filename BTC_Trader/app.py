# app.py

import streamlit as st
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import plotly.graph_objects as go
from utils.indicators import calculate_indicators
from utils.model_bayes import BayesSignalPredictor
from utils.binance_fetch import get_binance_4h_data
import streamlit.components.v1 as components

# --- CONFIGURACION INICIAL ---
st.set_page_config(page_title="BTC Streamlit V2.0", layout="wide")
st.title("📈 BTC/USDT Análisis Automático - MVP")

# --- PARÁMETROS ---
SYMBOL = "BTCUSDT"
INTERVAL = "4h"

# --- CARGA DE DATOS ---
st.markdown("### 1. Datos Binance y Cálculo de Indicadores")
@st.cache_data(ttl=60*5)  # cache por 5 minutos
def load_data():
    df = get_binance_4h_data(SYMBOL)
    df = calculate_indicators(df)
    return df

df = load_data()

# --- APLICAR MODELO BAYESIANO ---
predictor = BayesSignalPredictor()
df = predictor.predict_signals(df)

# --- GRÁFICO DE SEÑALES ---
st.markdown("### 2. Señales de Compra/Venta")
fig = go.Figure()
fig.add_trace(go.Candlestick(
    x=df['Open time'],
    open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
    name='Candles'))

# Marcar Buy/Sell
buys = df[df['B-H-S Signal'] == 'B']
sells = df[df['B-H-S Signal'] == 'S']
fig.add_trace(go.Scatter(x=buys['Open time'], y=buys['High'], mode='markers',
                         marker=dict(color='green', symbol='triangle-up', size=10), name='Buy'))
fig.add_trace(go.Scatter(x=sells['Open time'], y=sells['Low'], mode='markers',
                         marker=dict(color='red', symbol='triangle-down', size=10), name='Sell'))

fig.update_layout(height=600, width=1100, title="BTC 4H + Señales Bayesianas")
st.plotly_chart(fig, use_container_width=True)

# --- EMBED DE TRADINGVIEW ---
st.markdown("### 3. Visualización en TradingView (embed)")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)

