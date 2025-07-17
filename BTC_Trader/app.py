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

@st.cache_data(ttl=60*5)
def load_data():
    df_raw = get_binance_4h_data(SYMBOL)
    st.subheader("🟡 Datos crudos de Binance")
    st.dataframe(df_raw.head())
    st.write("Dimensiones crudas:", df_raw.shape)

    df = calculate_indicators(df_raw)
    st.subheader("🟢 Datos con indicadores calculados")
    st.dataframe(df.head())
    st.write("Dimensiones después de indicadores:", df.shape)

    return df

df = load_data()

# Verificar columnas y estado general
st.write("Columnas disponibles después de indicadores:", df.columns.tolist())

# --- APLICAR MODELO BAYESIANO ---
st.markdown("### 2. Aplicar Modelo Bayesiano")

predictor = BayesSignalPredictor()

# Verificar si columna de señales ya existe
st.write("Antes del modelo - columnas presentes:", df.columns)
st.write("¿Contiene 'B-H-S Signal' antes?", 'B-H-S Signal' in df.columns)

# --- VERIFICACIÓN DE COLUMNAS Y NULOS PREVIO A MODELO ---
st.markdown("### 🧪 Verificación previa al modelo")

required_columns = [
    'EMA20', 'EMA50', 'EMA200', 'EMA_12', 'EMA_26',
    'MACD', 'Signal_Line', 'RSI', '%K', '%D',
    'MACD Comp', 'Cross Check', 'EMA20 Check', 'EMA 200 Check', 'RSI Check'
]

# Verificar columnas faltantes
missing = [col for col in required_columns if col not in df.columns]
if missing:
    st.error(f"❌ Faltan columnas necesarias para el modelo: {missing}")
else:
    st.success("✅ Todas las columnas necesarias están presentes.")

# Verificar NaNs por columna
st.write("🔍 Conteo de NaNs por columna del modelo:")
st.dataframe(df[required_columns].isna().sum().to_frame("NaNs"))

# Verificar cuántas filas quedarían después del dropna
st.write("📏 Filas antes del dropna:", df.shape[0])
clean_df = df[required_columns].dropna()
st.write("✅ Filas después del dropna (solo en columnas del modelo):", clean_df.shape[0])

# Opcional: ver cuántas filas candidatas se enviarán al modelo
candidatas = df[df['B-H-S Signal'].isna()].dropna(subset=required_columns)
st.write("🔍 Filas candidatas a predecir por el modelo:", candidatas.shape)

# --- APLICAR EL MODELO ---
df = predictor.predict_signals(df)

# Verificar después de aplicar modelo
st.write("Después del modelo - columnas presentes:", df.columns)

if 'B-H-S Signal' in df.columns and not df['B-H-S Signal'].dropna().empty:
    st.write("Conteo de señales:", df['B-H-S Signal'].value_counts(dropna=False))
else:
    st.warning("⚠️ No se generaron señales bayesianas. Revisa si el dataframe está vacío o contiene valores nulos.")

# --- GRÁFICO DE SEÑALES ---
st.markdown("### 3. Señales de Compra/Venta")
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
st.markdown("### 4. Visualización en TradingView (embed)")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)
