import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
from utils.indicators import calculate_indicators
from utils.model_bayes import BayesSignalPredictor
from utils.binance_fetch import get_binance_4h_data
import streamlit.components.v1 as components
from utils.signal_postprocessing import eliminar_señales_consecutivas
from utils.evaluation import calcular_estadisticas_modelo

# --- CONFIGURACION INICIAL ---
st.set_page_config(page_title="BTC Streamlit V2.0", layout="wide")
st.title("📈 BTC/USDT Señales Automatizadas")

# --- PARÁMETROS ---
SYMBOL = "BTCUSDT"
INTERVAL = "4h"

# --- CARGA DE DATOS E INDICADORES ---
@st.cache_data(ttl=60*5)
def load_data():
    df_raw = get_binance_4h_data(SYMBOL)
    df = calculate_indicators(df_raw)
    return df

df = load_data()

# --- APLICAR MODELO ---
predictor = BayesSignalPredictor()
if 'B-H-S Signal' not in df.columns:
    df['B-H-S Signal'] = np.nan
df = predictor.predict_signals(df)

# Eliminar señales Buy consecutivas
df = eliminar_señales_consecutivas(df, columna='B-H-S Signal', señal='B')

# Calcular Hit Rate
hit_rate, total_pares, ganancia_media, perdida_media, profit_factor = calcular_estadisticas_modelo(df)
color_box = "#90EE90" if hit_rate >= 50 else "#FF7F7F"

# Mostrar caja de resultados
with st.container():
    st.markdown(f"""
        <div style="position: absolute; top: 30px; right: 40px; background-color: {color_box}; 
                    padding: 12px 20px; border-radius: 10px; font-size: 16px;">
            ✅ <strong>Hit Rate:</strong> {hit_rate:.1f}%<br>
            🔁 <strong>Total pares:</strong> {total_pares}<br>
            💰 <strong>Ganancia media:</strong> {ganancia_media:.2f}<br>
            📉 <strong>Pérdida media:</strong> {perdida_media:.2f}<br>
            📈 <strong>Profit Factor:</strong> {profit_factor:.2f}
        </div>
    """, unsafe_allow_html=True)

# --- GRÁFICO DE SEÑALES ---
st.markdown("### 🟢 Señales de Compra/Venta")
fig = go.Figure()
fig.add_trace(go.Candlestick(
    x=df['Open time'],
    open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
    name='Candlestick'))

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
st.markdown("### 📊 Visualización en TradingView")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)
