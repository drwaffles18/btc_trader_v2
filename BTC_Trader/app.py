import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import streamlit.components.v1 as components

# --- IMPORTACIONES PERSONALIZADAS ---
from utils.indicators import calcular_momentum_fisico_speed
from utils.signal_postprocessing import limpiar_señales_consecutivas
from utils.load_from_sheets import load_symbol_df

# --- CONFIGURACIÓN INICIAL ---
st.set_page_config(page_title="Cripto Señales Multi-Token (5m)", layout="wide")
st.title("📊 Señales Automatizadas por Token — 5m Momentum Físico")

st_autorefresh(interval=300000, key="auto_refresh_5m")

symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

SYMBOL_PARAMS = {
    "BTCUSDT": {"mom_win": 4, "speed_win": 9, "accel_win": 7, "zspeed_min": 0.3, "zaccel_min": 0.1},
    "ETHUSDT": {"mom_win": 7, "speed_win": 9, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.2},
    "ADAUSDT": {"mom_win": 4, "speed_win": 7, "accel_win": 5, "zspeed_min": 0.2, "zaccel_min": 0.3},
    "XRPUSDT": {"mom_win": 5, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.2, "zaccel_min": 0.0},
    "BNBUSDT": {"mom_win": 6, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.0},
}

def procesar_symbol(symbol):
    df = load_symbol_df(symbol)

    params = SYMBOL_PARAMS.get(symbol)
    df = calcular_momentum_fisico_speed(
        df,
        mom_win=params["mom_win"],
        speed_win=params["speed_win"],
        accel_win=params["accel_win"],
        zspeed_min=params["zspeed_min"],
        zaccel_min=params["zaccel_min"]
    )

    df = limpiar_señales_consecutivas(df, columna='Momentum Signal')
    return df

# ==============================
# ÚLTIMAS SEÑALES
# ==============================
st.markdown("### 🔹 Últimas Señales por Token (5m)")

for symbol in symbols:
    df = procesar_symbol(symbol)
    df_valid = df.dropna(subset=['Signal Final'])

    if df_valid.empty:
        st.info(f"Sin señales aún para {symbol}.")
        continue

    ultima = df_valid.iloc[-1]

    senal = ultima['Signal Final']
    fecha = ultima['Open time']

    if senal == "BUY":
        bg, color, emoji = "#90EE90", "#000", "🟢"
    elif senal == "SELL":
        bg, color, emoji = "#FF7F7F", "#FFF", "🔴"
    else:
        bg, color, emoji = "#D3D3D3", "#000", "⏸️"

    st.markdown(f"""
    <div style="background:{bg};color:{color};
         padding:12px;border-radius:10px;margin-bottom:10px;">
         <b>{symbol}</b> {emoji} {senal}<br>
         <small>{fecha}</small>
    </div>
    """, unsafe_allow_html=True)

# ==============================
# GRÁFICOS
# ==============================
st.markdown("### 📊 Gráficos de Señales (últimos 3 días)")

for symbol in symbols:
    df = procesar_symbol(symbol)

    fecha_lim = pd.Timestamp.now(tz=df["Open time"].dt.tz) - pd.Timedelta(days=3)
    dff = df[df["Open time"] >= fecha_lim]

    if dff.empty:
        st.warning(f"No hay datos recientes para {symbol}.")
        continue

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=dff['Open time'],
        open=dff['Open'], high=dff['High'],
        low=dff['Low'], close=dff['Close']
    ))

    dff["prev"] = dff["Signal Final"].shift(1)

    buys = dff[(dff["Signal Final"]=="BUY") & (dff["prev"]!="BUY")]
    sells = dff[(dff["Signal Final"]=="SELL") & (dff["prev"]!="SELL")]

    fig.add_trace(go.Scatter(
        x=buys["Open time"], y=buys["Low"],
        mode="text", text="🟢BUY"
    ))

    fig.add_trace(go.Scatter(
        x=sells["Open time"], y=sells["High"],
        mode="text", text="🔴SELL"
    ))

    fig.update_layout(
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        height=500,
        title=f"{symbol} — Señales 5m"
    )

    st.plotly_chart(fig, use_container_width=True)

# TradingView
st.markdown("### BTCUSDT — TradingView")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark"
width="100%" height="500"></iframe>
""", height=500)
