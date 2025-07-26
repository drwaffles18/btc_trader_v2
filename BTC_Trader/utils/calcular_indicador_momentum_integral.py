# utils.py
import numpy as np

def calcular_indicador_momentum_integral(df, window=6):
    df = df.copy()
    df['momentum'] = df['close'].diff()
    df['integral_momentum'] = df['momentum'].rolling(window=window).sum()
    df['slope_integral'] = df['integral_momentum'].diff()

    std_slope = df['slope_integral'].rolling(window=window).std()

    df['signal'] = np.where(
        (df['slope_integral'] < -std_slope) & (df['momentum'] < 0),
        'SELL',
        np.where(
            (df['slope_integral'] > std_slope) & (df['momentum'] > 0),
            'BUY',
            None
        )
    )
    return df
