import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import streamlit.components.v1 as components

# --- IMPORTACIONES PERSONALIZADAS ---
from utils.indicators import calcular_momentum_fisico_speed
from utils.binance_fetch import get_binance_5m_data
from utils.signal_postprocessing import limpiar_señales_consecutivas

# --- CONFIGURACIÓN INICIAL ---
st.set_page_config(page_title="Cripto Señales Multi-Token (5m)", layout="wide")
st.title("📊 Señales Automatizadas por Token — 5m Momentum Físico")

# 🔄 Refrescar cada 5 minutos (300,000 ms)
st_autorefresh(interval=300000, key="auto_refresh_5m")

symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

# ==========================================================
# 🎯 PARÁMETROS ÓPTIMOS POR SÍMBOLO (según tu grid search)
# ==========================================================
SYMBOL_PARAMS = {
    "BTCUSDT": {
        "mom_win": 4,
        "speed_win": 9,
        "accel_win": 7,
        "zspeed_min": 0.3,
        "zaccel_min": 0.1,
    },
    "ETHUSDT": {
        "mom_win": 7,
        "speed_win": 9,
        "accel_win": 9,
        "zspeed_min": 0.3,
        "zaccel_min": 0.2,
    },
    "ADAUSDT": {
        "mom_win": 4,
        "speed_win": 7,
        "accel_win": 5,
        "zspeed_min": 0.2,
        "zaccel_min": 0.3,
    },
    "XRPUSDT": {
        "mom_win": 5,
        "speed_win": 7,
        "accel_win": 9,
        "zspeed_min": 0.2,
        "zaccel_min": 0.0,
    },
    "BNBUSDT": {
        "mom_win": 6,
        "speed_win": 7,
        "accel_win": 9,
        "zspeed_min": 0.3,
        "zaccel_min": 0.0,
    }
}

HISTORY_LIMIT_5M = 900  # ~3 días de velas


# ==========================================================
# 🔧 FUNCIÓN PARA PROCESAR CADA TOKEN
# ==========================================================
def procesar_symbol(symbol):
    # 1) Descargar histórico 5m
    df = get_binance_5m_data(symbol, limit=HISTORY_LIMIT_5M)

    # 2) Parámetros específicos para este símbolo
    params = SYMBOL_PARAMS.get(symbol)

    df = calcular_momentum_fisico_speed(
        df,
        mom_win=params["mom_win"],
        speed_win=params["speed_win"],
        accel_win=params["accel_win"],
        zspeed_min=params["zspeed_min"],
        zaccel_min=params["zaccel_min"]
    )

    # 3) Limpiar señales consecutivas
    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')

    return df


# ==========================================================
# 🔹 MOSTRAR ÚLTIMA SEÑAL POR TOKEN
# ==========================================================
st.markdown("### 🔹 Últimas Señales por Token (5m)")

for symbol in symbols:
    df = procesar_symbol(symbol)

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
        color = '#D3D3D3'
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


# ==========================================================
# 📊 GRÁFICOS POR TOKEN
# ==========================================================
st.markdown("### 📊 Gráficos de Señales por Token (últimos 3 días, 5m)")

for symbol in symbols:
    df = procesar_symbol(symbol)

    # Filtrar últimos 3 días
    tz = df['Open time'].dt.tz
    fecha_limite = pd.Timestamp.now(tz=tz) - pd.Timedelta(days=3)
    df_filtrado = df[df['Open time'] >= fecha_limite].copy()

    if df_filtrado.empty:
        st.warning(f"No hay datos en ventana de 3 días para {symbol}.")
        continue

    # Preparar gráfico
    fig = go.Figure()

    # Velas
    fig.add_trace(go.Candlestick(
        x=df_filtrado['Open time'],
        open=df_filtrado['Open'], high=df_filtrado['High'],
        low=df_filtrado['Low'], close=df_filtrado['Close'],
        name='Candlestick'
    ))

    # Señales filtradas
    df_filtrado['prev_signal'] = df_filtrado['Signal Final'].shift(1)

    # BUY
    for _, row in df_filtrado[(df_filtrado['Signal Final']=='BUY') &
                              (df_filtrado['prev_signal']!='BUY')].iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Open time']], y=[row['Low']],
            mode='text', text=["🟢BUY"],
            textposition="bottom center", showlegend=False
        ))

    # SELL
    for _, row in df_filtrado[(df_filtrado['Signal Final']=='SELL') &
                              (df_filtrado['prev_signal']!='SELL')].iterrows():
        fig.add_trace(go.Scatter(
            x=[row['Open time']], y=[row['High']],
            mode='text', text=["🔴SELL"],
            textposition="top center", showlegend=False
        ))

    fig.update_layout(
        height=500,
        title=f"Señales — {symbol} (5m Momentum Físico)",
        showlegend=False,
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        hovermode="x unified"
    )

    st.plotly_chart(fig, use_container_width=True)


# ==========================================================
# TRADINGVIEW (opcional)
# ==========================================================
st.markdown("### 📊 Visualización en TradingView (BTCUSDT)")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark" 
    width="100%" height="500" frameborder="0" allowtransparency="true" scrolling="no"></iframe>
""", height=500)
