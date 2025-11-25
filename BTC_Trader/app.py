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
st.title("📊 Señales Automatizadas — Momentum Físico (5m)")

# Auto refresh cada 5 minutos
st_autorefresh(interval=300000, key="auto_refresh_5m")

symbols = ["BTCUSDT", "ETHUSDT", "ADAUSDT", "XRPUSDT", "BNBUSDT"]

# --- PARÁMETROS POR TOKEN ---
SYMBOL_PARAMS = {
    "BTCUSDT": {"mom_win": 4, "speed_win": 9, "accel_win": 7, "zspeed_min": 0.3, "zaccel_min": 0.1},
    "ETHUSDT": {"mom_win": 7, "speed_win": 9, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.2},
    "ADAUSDT": {"mom_win": 4, "speed_win": 7, "accel_win": 5, "zspeed_min": 0.2, "zaccel_min": 0.3},
    "XRPUSDT": {"mom_win": 5, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.2, "zaccel_min": 0.0},
    "BNBUSDT": {"mom_win": 6, "speed_win": 7, "accel_win": 9, "zspeed_min": 0.3, "zaccel_min": 0.0},
}

# ---------------------------------------
# 🚀 CACHE INTELIGENTE PARA ACELERAR TODO
# ---------------------------------------
@st.cache_data(ttl=240)   # cache 4 minutos
def procesar_symbol(symbol):
    """Carga el DF desde Sheets, calcula momentum físico y limpia señales consecutivas."""
    df = load_symbol_df(symbol).copy()

    params = SYMBOL_PARAMS[symbol]
    df = calcular_momentum_fisico_speed(
        df,
        mom_win=params["mom_win"],
        speed_win=params["speed_win"],
        accel_win=params["accel_win"],
        zspeed_min=params["zspeed_min"],
        zaccel_min=params["zaccel_min"]
    )

    # limpiar duplicados: BUY-BUY-BUY / SELL-SELL-SELL
    df = limpiar_señales_consecutivas(df, columna="Momentum Signal")

    return df


# ==============================
# 🔹 ÚLTIMAS SEÑALES
# ==============================
st.markdown("### 🔹 Últimas Señales por Token (5m)")

for symbol in symbols:
    try:
        df = procesar_symbol(symbol)
        df_valid = df.dropna(subset=['Momentum Signal'])

        if df_valid.empty:
            st.info(f"Sin señales aún para {symbol}.")
            continue

        ultima = df_valid.iloc[-1]
        senal = ultima["Momentum Signal"]
        fecha = ultima["Open time"]

        # estilos por señal
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

    except Exception as e:
        st.error(f"Error procesando {symbol}: {e}")


# ==============================
# 📊 GRÁFICOS
# ==============================
st.markdown("### 📊 Gráficos de Señales (últimas 180 velas)")

MAX_VELAS = 180

for symbol in symbols:
    try:
        df = procesar_symbol(symbol)
        dff = df.tail(MAX_VELAS).copy()

        if dff.empty:
            st.warning(f"No hay datos recientes para {symbol}.")
            continue

        # evitar warnings pandas
        dff = dff.copy()
        dff.loc[:, "prev"] = dff["Momentum Signal"].shift(1)

        buys = dff[(dff["Momentum Signal"] == "BUY") & (dff["prev"] != "BUY")]
        sells = dff[(dff["Momentum Signal"] == "SELL") & (dff["prev"] != "SELL")]

        fig = go.Figure()

        # --- Candlesticks ---
        fig.add_trace(go.Candlestick(
            name=f"{symbol} Price",
            x=dff['Open time'],
            open=dff['Open'], high=dff['High'],
            low=dff['Low'], close=dff['Close']
        ))

        # --- Señales ---
        fig.add_trace(go.Scatter(
            name="BUY",
            x=buys["Open time"],
            y=buys["Low"] * 0.999,
            mode="text",
            text="🟢 BUY"
        ))
        fig.add_trace(go.Scatter(
            name="SELL",
            x=sells["Open time"],
            y=sells["High"] * 1.001,
            mode="text",
            text="🔴 SELL"
        ))

        # --- Crosshair estilo TradingView ---
        fig.update_layout(
            template="plotly_dark",
            xaxis_rangeslider_visible=False,
            height=480,
            title=f"{symbol} — Señales 5m (últimas {MAX_VELAS} velas)",
            hovermode="x unified",
            xaxis=dict(
                showspikes=True, spikemode="across",
                spikesnap="cursor", spikethickness=1,
                spikecolor="#888", showline=True
            ),
            yaxis=dict(
                showspikes=True, spikemode="across",
                spikesnap="cursor", spikethickness=1,
                spikecolor="#888", showline=True
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Error graficando {symbol}: {e}")


# ==============================
# 📈 TRADINGVIEW
# ==============================
st.markdown("### BTCUSDT — TradingView")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark"
width="100%" height="500"></iframe>
""", height=500)
