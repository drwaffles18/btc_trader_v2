import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import streamlit.components.v1 as components

from utils.load_from_sheets import load_symbol_df

# ✅ estrategia actual (winner/champion)
from utils.strategy_winner_champion import (
    build_features_winner,
    buy_signal_champion,
    simulate_sellraw_only,
    struct_modulated_threshold,
)

# ==============================
# CONFIG
# ==============================
st.set_page_config(page_title="BTCUSDT — Winner/Champion (5m)", layout="wide")
st.title("📊 BTCUSDT — Winner/Champion (5m)")

# Auto refresh cada 5 minutos
st_autorefresh(interval=300000, key="auto_refresh_5m")

SYMBOL = "BTCUSDT"
MAX_VELAS = 220  # ajusta si quieres

# ==============================
# PARÁMETROS (igual que alert_bot)
# ==============================
P = dict(
    mom_win=4,
    speed_win=9,
    accel_win=7,
    z_win=20,
    zspeed_min=0.30,
    zaccel_min=0.10,
    zaccel_gate=4.0
)

ENERGY_ZWIN = 120
STRUCT_ZWIN = 120
STRUCT_WIN  = 48
DON_WIN     = 48

ENTRY_ZENERGY_MIN = 1.8
ENTRY_K_STRUCT    = 0.4
ENTRY_USE_ASYM    = False
ENTRY_N_DOWN      = 1

# ==============================
# UI helpers
# ==============================
def status_card(title: str, value: str, ok: bool, subtitle: str = ""):
    # ✅ verde con letra blanca | ❌ rojo con letra blanca (como pediste)
    bg = "#198754" if ok else "#dc3545"
    fg = "#ffffff"

    glow = "0 0 10px rgba(25,135,84,0.7)" if ok else "0 0 10px rgba(220,53,69,0.6)"

    st.markdown(
        f"""
        <div style="
            background:{bg};
            color:{fg};
            padding:12px 14px;
            border-radius:12px;
            margin-bottom:10px;
            border:1px solid rgba(255,255,255,0.10);
            box-shadow:{glow};
            transition: all 0.25s ease;
        ">
          <div style="font-weight:800;font-size:13px; letter-spacing:0.2px;">
            {title}
          </div>

          <div style="font-size:18px;font-weight:900;margin-top:4px;">
            {value}
          </div>

          <div style="font-size:12px;opacity:0.95;margin-top:6px; line-height:1.25;">
            {subtitle}
          </div>
        </div>
        """,
        unsafe_allow_html=True
    )

@st.cache_data(ttl=240)
def load_btc_df() -> pd.DataFrame:
    df = load_symbol_df(SYMBOL).copy()
    return df

def prep_ohlcv_for_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza DF de Sheets → OHLCV indexado por tiempo y numérico.
    La estrategia solo requiere: Open, High, Low, Close, Volume.
    """
    d = df.copy()

    # Elegir columna de tiempo para index (preferimos Open time)
    if "Open time" in d.columns:
        d["Open time"] = pd.to_datetime(d["Open time"], errors="coerce")
        d = d.dropna(subset=["Open time"]).sort_values("Open time").set_index("Open time")
    elif "Close time" in d.columns:
        d["Close time"] = pd.to_datetime(d["Close time"], errors="coerce")
        d = d.dropna(subset=["Close time"]).sort_values("Close time").set_index("Close time")
    else:
        # sin tiempo, igual intentamos pero el chart quedará raro
        d = d.reset_index(drop=True)

    # Numéricos
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in d.columns:
            d[c] = pd.to_numeric(d[c], errors="coerce")

    d = d.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    if "Volume" in d.columns:
        d["Volume"] = d["Volume"].fillna(0.0)

    return d

# ==============================
# LOAD + STRATEGY
# ==============================
try:
    df_raw = load_btc_df()
    df = prep_ohlcv_for_strategy(df_raw)

    d = build_features_winner(
        df,
        P=P,
        ENERGY_ZWIN=ENERGY_ZWIN,
        STRUCT_ZWIN=STRUCT_ZWIN,
        STRUCT_WIN=STRUCT_WIN,
        DON_WIN=DON_WIN,
    )

    buy_ok = buy_signal_champion(
        d,
        P=P,
        ENTRY_ZENERGY_MIN=ENTRY_ZENERGY_MIN,
        ENTRY_K_STRUCT=ENTRY_K_STRUCT,
        ENTRY_USE_ASYM=ENTRY_USE_ASYM,
        ENTRY_N_DOWN=ENTRY_N_DOWN,
    )

    sig = simulate_sellraw_only(d, buy_ok)

    ts_last = sig.index.max()
    if pd.isna(ts_last):
        st.error("No se pudo determinar la última vela (ts_last es NaT).")
        st.stop()

except Exception as e:
    st.error(f"Error cargando/procesando BTCUSDT: {e}")
    st.stop()

# ==============================
# HEADER / STATUS
# ==============================
row = sig.loc[ts_last]
curr = "BUY" if bool(row.get("BUY", False)) else "SELL" if bool(row.get("SELL", False)) else "NONE"
btc_price = float(d.loc[ts_last, "Close"])

st.markdown("### 🧠 Estado actual (última vela cerrada)")
st.write(f"🕒 **{ts_last}**  |  💵 **BTC Close:** `{btc_price:,.2f}`  |  🎯 **Señal (simulada):** `{curr}`")

# ==============================
# CONDITION CARDS (BTC only)
# ==============================
st.markdown("### ✅ Condiciones para ejecutar BUY (Champion)")

zspeed = float(d.loc[ts_last, "zspeed"])
zaccel = float(d.loc[ts_last, "zaccel"])
zenergy = float(d.loc[ts_last, "zenergy"])
energy = float(d.loc[ts_last, "energy"])
struct_score = float(d.loc[ts_last, "struct_score"])
buy_raw = bool(d.loc[ts_last, "buy_raw"])

thr_eff = float(struct_modulated_threshold(d.loc[[ts_last]], ENTRY_ZENERGY_MIN, ENTRY_K_STRUCT).iloc[0])

gate_ok = (zaccel >= float(P["zaccel_gate"]))
energy_ok = (energy > 0)
zenergy_ok = (zenergy >= thr_eff)

c1, c2, c3, c4 = st.columns(4)

with c1:
    status_card(
        "1) buy_raw",
        "OK" if buy_raw else "NO",
        buy_raw,
        f"Regla base (zspeed prev<0, zspeed>{P['zspeed_min']}, zaccel>{P['zaccel_min']})"
    )

with c2:
    status_card(
        "2) zaccel gate",
        f"{zaccel:.3f} ≥ {P['zaccel_gate']}",
        gate_ok,
        "Gate fuerte de aceleración"
    )

with c3:
    status_card(
        "3) energy > 0",
        f"{energy:.6f}",
        energy_ok,
        "energy = speed_smooth × accel_smooth"
    )

with c4:
    status_card(
        "4) zenergy ≥ thr_eff",
        f"{zenergy:.3f} ≥ {thr_eff:.3f}",
        zenergy_ok,
        f"struct_score={struct_score:.3f} | base={ENTRY_ZENERGY_MIN} k={ENTRY_K_STRUCT}"
    )

# ==============================
# 🔥 RADAR / READINESS PANEL
# ==============================
st.markdown("### 🧭 Radar de Momentum BTC (0–100)")

def _sigmoid_score(x: float) -> float:
    # 0..100, suave, robusto
    return float(100.0 / (1.0 + np.exp(-x)))

# Scores (0..100)
score_speed = _sigmoid_score(zspeed)
score_accel = _sigmoid_score(zaccel)
score_energy = _sigmoid_score(zenergy)
score_struct = float(np.clip(struct_score, 0, 1) * 100.0)

# Readiness (0..4)
readiness = int(buy_raw) + int(gate_ok) + int(energy_ok) + int(zenergy_ok)

r1, r2, r3, r4 = st.columns([1.1, 1.1, 1.1, 1.6])

with r1:
    st.metric("Readiness BUY", f"{readiness}/4")
with r2:
    st.metric("zspeed", f"{zspeed:.3f}")
with r3:
    st.metric("zaccel", f"{zaccel:.3f}")
with r4:
    st.metric("zenergy", f"{zenergy:.3f}  |  thr_eff", f"{thr_eff:.3f}")

radar_df = pd.DataFrame({
    "factor": ["Momentum Speed", "Momentum Accel", "Energy (z)", "Structure"],
    "score":  [score_speed, score_accel, score_energy, score_struct],
    "raw":    [zspeed, zaccel, zenergy, struct_score],
})

# Orden bonito (arriba→abajo)
radar_df = radar_df.iloc[::-1].reset_index(drop=True)

fig_radar = go.Figure()

fig_radar.add_trace(go.Bar(
    x=radar_df["score"],
    y=radar_df["factor"],
    orientation="h",
    text=[f"{v:.0f}" for v in radar_df["score"]],
    textposition="outside",
))

fig_radar.update_layout(
    template="plotly_dark",
    height=260,
    margin=dict(l=10, r=10, t=10, b=10),
    xaxis=dict(range=[0, 110], title="Score"),
    yaxis=dict(title=""),
)

st.plotly_chart(fig_radar, use_container_width=True)

# (Opcional) lista rápida de qué falta para BUY
missing = []
if not buy_raw: missing.append("buy_raw")
if not gate_ok: missing.append("zaccel gate")
if not energy_ok: missing.append("energy>0")
if not zenergy_ok: missing.append("zenergy>=thr_eff")

if readiness == 4:
    st.success("✅ BUY está completamente habilitado (4/4).")
else:
    st.warning(f"⏳ BUY aún NO: faltan {', '.join(missing)}.")

# ==============================
# BTC CHART ONLY
# ==============================
st.markdown("### 📊 BTCUSDT — Señales Winner/Champion (últimas velas)")

plot_sig = sig.tail(MAX_VELAS).copy()
plot_d = d.loc[plot_sig.index].copy()

plot_sig["prev_buy"] = plot_sig["BUY"].shift(1).fillna(False)
plot_sig["prev_sell"] = plot_sig["SELL"].shift(1).fillna(False)

buys = plot_sig[(plot_sig["BUY"]) & (~plot_sig["prev_buy"])]
sells = plot_sig[(plot_sig["SELL"]) & (~plot_sig["prev_sell"])]

fig = go.Figure()

fig.add_trace(go.Candlestick(
    name="BTC Price",
    x=plot_d.index,
    open=plot_d["Open"], high=plot_d["High"],
    low=plot_d["Low"], close=plot_d["Close"]
))

fig.add_trace(go.Scatter(
    name="BUY",
    x=buys.index,
    y=plot_d.loc[buys.index, "Low"] * 0.999,
    mode="text",
    text="🟢 BUY",
    showlegend=False
))

fig.add_trace(go.Scatter(
    name="SELL",
    x=sells.index,
    y=plot_d.loc[sells.index, "High"] * 1.001,
    mode="text",
    text="🔴 SELL",
    showlegend=False
))

fig.update_layout(
    template="plotly_dark",
    xaxis_rangeslider_visible=False,
    height=560,
    title=f"BTCUSDT — Winner/Champion (últimas {min(MAX_VELAS, len(plot_sig))} velas)",
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

# ==============================
# TRADINGVIEW (KEEP)
# ==============================
st.markdown("### BTCUSDT — TradingView")
components.html("""
<iframe src="https://www.tradingview.com/embed-widget/advanced-chart/?symbol=BINANCE:BTCUSDT&interval=240&theme=dark"
width="100%" height="500"></iframe>
""", height=500)



