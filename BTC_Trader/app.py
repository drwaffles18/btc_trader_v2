import streamlit as st
import pandas as pd
import numpy as np
from datetime import timedelta
import plotly.graph_objects as go
from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.binance_fetch import get_binance_4h_data
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.evaluation import calcular_estadisticas_long_only
import streamlit.components.v1 as components

# --- CONFIGURACION INICIAL ---
st.set_page_config(page_title="BTC Streamlit V2.0", layout="wide")
st.title("📈 BTC/USDT Señales Automatizadas")

# --- PARÁMETROS ---
SYMBOL = "BTCUSDT"

# --- CARGA DE DATOS E INDICADORES ---
@st.cache_data(ttl=60*5)
def load_data():
    df_raw = get_binance_4h_data(SYMBOL)
    df = calculate_indicators(df_raw)
    return df

df = load_data()

# --- CALCULAR MOMENTUM INTEGRAL ---
df_momentum = calcular_momentum_integral(df, window=6)
df_momentum = limpiar_señales_consecutivas(df_momentum, columna='Momentum Signal')

# --- EVALUACIÓN ---
df_eval = df_momentum.copy()
df_eval['Eval Signal'] = df_eval['Signal Final'].replace({'BUY': 'B', 'SELL': 'S'})
hit_m, total_m, ganancia_m, perdida_m, pf_m = calcular_estadisticas_long_only(
    df_eval, señal_col='Eval Signal', precio_col='Close'
)

# --- MOSTRAR ESTADÍSTICAS ---
st.markdown("### 📊 Estadísticas del Indicador Momentum Integral")

color_m = "#90EE90" if hit_m >= 50 else "#FF7F7F"

col1, col2, col3 = st.columns([1, 1, 2])
with col3:
    st.markdown(f"""
    <div style="background-color: {color_m}; 
                padding: 12px 20px; border-radius: 10px; font-size: 16px;">
        ✅ <strong>Hit Rate:</strong> {hit_m:.1f}%<br>
        🔁 <strong>Total pares:</strong> {total_m}<br>
        💰 <strong>Ganancia media:</strong> {ganancia_m:.2f}<br>
        📉 <strong>Pérdida media:</strong> {perdida_m:.2f}<br>
        📈 <strong>Profit Factor:</strong> {pf_m:.2f}
    </div>
    """, unsafe_allow_html=True)

# --- MOSTRAR ÚLTIMA SEÑAL ---
ultima = df_momentum['Signal Final'].iloc[-1]
if ultima == 'BUY':
    color = '#90EE90'
    emoji = '🟢'
elif ultima == 'SELL':
    color = '#FF7F7F'
    emoji = '🔴'
else:
    color = '#D3D3D3'
    emoji = '⏸️'

st.markdown(f"""
<div style="background-color: {color}; 
            padding: 12px 20px; 
            border-radius: 10px; 
            font-size: 16px;
            text-align: center;">
    📌 <strong>Última Señal del Indicador:</strong> {emoji} {ultima}
</div>
""", unsafe_allow_html=True)

# --- GRÁFICO MOMENTUM INTEGRAL ---
st.markdown("### 📉 Indicador de Momentum Integral")
fig_m = go.Figure()

fig_m.add_trace(go.Candlestick(
    x=df_momentum['Open time'],
    open=df_momentum['Open'], high=df_momentum['High'],
    low=df_momentum['Low'], close=df_momentum['Close'],
    name='Candlestick'))

for i, row in df_momentum.iterrows():
    if i > 0:
        actual = row['Signal Final']
        anterior = df_momentum.at[i-1, 'Signal Final']
        if actual != anterior:
            if actual == 'BUY':
                fig_m.add_trace(go.Scatter(
                    x=[row['Open time']], y=[row['Low']],
                    mode='text', text=["🟢BUY"],
                    textposition="bottom center", showlegend=False
                ))
                fig_m.add_vrect(
                    x0=row['Open time'],
                    x1=row['Open time'] + timedelta(hours=4),
                    fillcolor="green", opacity=0.15, line_width=0
                )
            elif actual == 'SELL':
                fig_m.add_trace(go.Scatter(
                    x=[row['Open time']], y=[row['High']],
                    mode='text', text=["🔴SELL"],
                    textposition="top center", showlegend=False
                ))
                fig_m.add_vrect(
                    x0=row['Open time'],
                    x1=row['Open time'] + timedelta(hours=4),
                    fillcolor="red", opacity=0.15, line_width=0
                )

fig_m.update_layout(
    height=500,
    width=1100,
    title="Indicador de Momentum Integral (4h)",
    showlegend=False
)
st.plotly_chart(fig_m, use_container_width=True)

# --- EMBED TRADINGVIEW ---
st.markdown("### 📊 Visualización en TradingView")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)
