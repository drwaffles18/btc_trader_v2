# --- utils/indicators.py ---
# Cálculo de EMAs, MACD, RSI y Stochastic RSI
import numpy as np
import pandas as pd

def calculate_stochastic_rsi(df, rsi_length=14, stoch_length=14, k_period=3, d_period=3):
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=rsi_length, min_periods=rsi_length).mean()
    avg_loss = loss.rolling(window=rsi_length, min_periods=rsi_length).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    stoch_rsi = (rsi - rsi.rolling(window=stoch_length).min()) / (rsi.rolling(window=stoch_length).max() - rsi.rolling(window=stoch_length).min())
    df['%K'] = stoch_rsi.rolling(window=k_period).mean() * 100
    df['%D'] = df['%K'].rolling(window=d_period).mean()
    return df

def calculate_indicators(df):
    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
    df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['Signal_Line'] = df['MACD'].ewm(span=9, adjust=False).mean()
    df['Histogram'] = df['MACD'] - df['Signal_Line']
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))
    df = calculate_stochastic_rsi(df)
    df['MACD Comp'] = np.where(df['MACD'] > df['Signal_Line'], 'MACD', 'Signal')
    df['Cross Check'] = df['MACD Comp'] != df['MACD Comp'].shift(1)
    df['Cross Check'] = np.where(df['Cross Check'], df['MACD Comp'] + " Cross", df['MACD Comp'])
    df['EMA20 Check'] = (df['Close'] > df['EMA20']).astype(int)
    df['EMA50 Check'] = (df['Close'] > df['EMA50']).astype(int)
    df['EMA 200 Check'] = (df['Close'] > df['EMA200']).astype(int)
    df['RSI Check'] = ((df['%K'] < 90) & (df['%D'] < 90)).astype(int)
    return df
def calcular_momentum_integral(df, window=6):
    df = df.copy()
    df['momentum'] = df['Close'].diff()
    df['integral_momentum'] = df['momentum'].rolling(window=window).sum()
    df['slope_integral'] = df['integral_momentum'].diff()
    std_slope = df['slope_integral'].rolling(window=window).std()

    df['Momentum Signal'] = np.where(
        (df['slope_integral'] < -std_slope) & (df['momentum'] < 0),
        'SELL',
        np.where(
            (df['slope_integral'] > std_slope) & (df['momentum'] > 0),
            'BUY',
            None
        )
    )
    return df
