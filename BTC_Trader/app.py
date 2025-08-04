import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta
import streamlit.components.v1 as components
from streamlit_autorefresh import st_autorefresh


# --- IMPORTACIONES PERSONALIZADAS ---
from utils.indicators import calculate_indicators, calcular_momentum_integral
from utils.binance_fetch import get_binance_4h_data
from utils.signal_postprocessing import limpiar_señales_consecutivas

# --- CONFIGURACIÓN INICIAL ---
st.set_page_config(page_title="Cripto Señales Multi-Token", layout="wide")

st.title("📊 Señales Automatizadas por Token")

# 🔄 Refrescar cada 10 minutos (600,000 ms)
st_autorefresh(interval=600000, key="auto_refresh")

# --- PARÁMETROS ---
symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT"]
interval = "4h"

# --- FUNCIÓN PARA PROCESAR CADA TOKEN ---
def procesar_symbol(symbol):
    df = get_binance_4h_data(symbol)
    df = calculate_indicators(df)
    df = calcular_momentum_integral(df, window=6)
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')
    df['Signal Final'] = df['Momentum Signal']
    return df

# --- MOSTRAR ÚLTIMA SEÑAL DE CADA TOKEN ---
st.markdown("### 🔹 Últimas Señales por Token")

for symbol in symbols:
    df = procesar_symbol(symbol)
    ultima_fila = df.dropna(subset=['Signal Final']).iloc[-1]
    ultima_senal = ultima_fila['Signal Final']
    fecha_ultima = ultima_fila['Open time']

    if ultima_senal == 'BUY':
        color = '#90EE90'  # verde claro
        emoji = '🟢'
        texto_color = '#000000'  # negro
    elif ultima_senal == 'SELL':
        color = '#FF7F7F'  # rojo claro
        emoji = '🔴'
        texto_color = '#FFFFFF'  # blanco
    else:
        color = '#D3D3D3'  # gris claro
        emoji = '⏸️'
        texto_color = '#000000'  # negro

    st.markdown(f"""
    <div style="background-color: {color}; 
                color: {texto_color};
                padding: 10px 18px; 
                border-radius: 10px; 
                font-size: 16px; 
                margin-bottom: 10px">
        🔹 <strong>{symbol}:</strong> {emoji} {ultima_senal} <br>
        🗓️ <strong>Fecha:</strong> {fecha_ultima}
    </div>
    """, unsafe_allow_html=True)


# --- MOSTRAR LOS 4 GRÁFICOS ---
st.markdown("### 📊 Gráficos de Señales por Token")

for symbol in symbols:
    df = procesar_symbol(symbol)

    # Filtrar solo datos de los últimos 30 días
    fecha_limite = pd.Timestamp.now().tz_localize(None) - pd.Timedelta(days=30)
    df['Open time naive'] = df['Open time'].dt.tz_localize(None)
    df_filtrado = df[df['Open time naive'] >= fecha_limite]

    fig = go.Figure()

    # Velas SOLO de los últimos 30 días
    fig.add_trace(go.Candlestick(
        x=df_filtrado['Open time'],
        open=df_filtrado['Open'], high=df_filtrado['High'],
        low=df_filtrado['Low'], close=df_filtrado['Close'],
        name='Candlestick'))

    # Añadir banderas de señales de los últimos 30 días
    for i, row in df_filtrado.iterrows():
        if i > 0:
            actual = row['Signal Final']
            anterior = df.at[i-1, 'Signal Final']
            if actual != anterior:
                if actual == 'BUY':
                    fig.add_trace(go.Scatter(
                        x=[row['Open time']], y=[row['Low']],
                        mode='text', text=["🟢BUY"],
                        textposition="bottom center", showlegend=False))
                    
                elif actual == 'SELL':
                    fig.add_trace(go.Scatter(
                        x=[row['Open time']], y=[row['High']],
                        mode='text', text=["🔴SELL"],
                        textposition="top center", showlegend=False))
                    

    fig.update_layout(
        height=500,
        width=1100,
        title=f"Señales Últimos 30 Días - {symbol}",
        showlegend=False,
        xaxis_rangeslider_visible=False,
        template="plotly_dark"
    )

    st.plotly_chart(fig, use_container_width=True)


# --- EMBED DE TRADINGVIEW ---
st.markdown("### 📊 Visualización en TradingView (BTCUSDT)")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)

