import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import streamlit.components.v1 as components

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
# Agregamos BNBUSDT
symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]
interval = "4h"

# --- FUNCIÓN PARA PROCESAR CADA TOKEN ---
def procesar_symbol(symbol):
    df = get_binance_4h_data(symbol)
    df = calculate_indicators(df)
    df = calcular_momentum_integral(df, window=6)

    # IMPORTANTE: limpiar antes de usar y NO volver a pisar 'Signal Final'
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')  # crea/actualiza 'Signal Final'
    # NO hacer: df['Signal Final'] = df['Momentum Signal']

    return df

# --- MOSTRAR ÚLTIMA SEÑAL DE CADA TOKEN ---
st.markdown("### 🔹 Últimas Señales por Token")

for symbol in symbols:
    df = procesar_symbol(symbol)

    # Evitar errores si aún no hay señales válidas
    df_valid = df.dropna(subset=['Signal Final'])
    if df_valid.empty:
        st.info(f"Sin señales aún para {symbol}.")
        continue

    ultima_fila = df_valid.iloc[-1]
    ultima_senal = ultima_fila['Signal Final']
    fecha_ultima = ultima_fila['Open time']

    if ultima_senal == 'BUY':
        color = '#90EE90'  # verde claro
        emoji = '🟢'
        texto_color = '#000000'
    elif ultima_senal == 'SELL':
        color = '#FF7F7F'  # rojo claro
        emoji = '🔴'
        texto_color = '#FFFFFF'
    else:
        color = '#D3D3D3'  # gris claro
        emoji = '⏸️'
        texto_color = '#000000'

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

# --- MOSTRAR LOS GRÁFICOS ---
st.markdown("### 📊 Gráficos de Señales por Token (últimos 30 días)")

for symbol in symbols:
    df = procesar_symbol(symbol)

    # Filtrar solo datos de los últimos 30 días
    fecha_limite = pd.Timestamp.now(tz=df['Open time'].dt.tz) - pd.Timedelta(days=30)
    df_filtrado = df[df['Open time'] >= fecha_limite].copy()

    if df_filtrado.empty:
        st.warning(f"No hay datos en ventana de 30 días para {symbol}.")
        continue

    fig = go.Figure()

    # Velas
    fig.add_trace(go.Candlestick(
        x=df_filtrado['Open time'],
        open=df_filtrado['Open'], high=df_filtrado['High'],
        low=df_filtrado['Low'], close=df_filtrado['Close'],
        name='Candlestick'
    ))

    # Señales: comparar contra la fila anterior DENTRO del filtrado
    df_filtrado['prev_signal'] = df_filtrado['Signal Final'].shift(1)

    # BUYs
    mask_buy = (df_filtrado['Signal Final'] == 'BUY') & (df_filtrado['prev_signal'] != 'BUY')
    buys = df_filtrado[mask_buy]
    for _, row in buys.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Open time']], y=[row['Low']],
            mode='text', text=["🟢BUY"],
            textposition="bottom center", showlegend=False
        ))

    # SELLs
    mask_sell = (df_filtrado['Signal Final'] == 'SELL') & (df_filtrado['prev_signal'] != 'SELL')
    sells = df_filtrado[mask_sell]
    for _, row in sells.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Open time']], y=[row['High']],
            mode='text', text=["🔴SELL"],
            textposition="top center", showlegend=False
        ))

    fig.update_layout(
        height=500,
        title=f"Señales — {symbol}",
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
