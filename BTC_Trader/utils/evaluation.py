import pandas as pd

def calcular_hit_rate(df, señal_col='B-H-S Signal', tiempo_col='Open time', precio_col='Close'):
    pares = []
    last_buy_index = None

    for idx, row in df.iterrows():
        if row[señal_col] == 'B':
            last_buy_index = idx
        elif row[señal_col] == 'S' and last_buy_index is not None:
            buy_price = df.at[last_buy_index, precio_col]
            sell_price = row[precio_col]
            pares.append((buy_price, sell_price))
            last_buy_index = None  # reset para siguiente par

    if not pares:
        return 0.0, 0

    hits = sum(1 for buy, sell in pares if sell > buy)
    total = len(pares)
    return hits / total * 100, total
