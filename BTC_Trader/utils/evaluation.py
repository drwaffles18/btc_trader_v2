import pandas as pd

def calcular_hit_rate(df, señal_col='B-H-S Signal', tiempo_col='Open time', precio_col='Close'):
    pares = []
    last_buy_index = None

    for idx, row in df.iterrows():
        señal = row[señal_col]

        # Ignorar valores NA
        if pd.isna(señal):
            continue

        if señal == 'B':
            last_buy_index = idx
        elif señal == 'S' and last_buy_index is not None:
            buy_price = df.at[last_buy_index, precio_col]
            sell_price = row[precio_col]

            # Asegurarse de que no haya NA en los precios
            if pd.notna(buy_price) and pd.notna(sell_price):
                pares.append((buy_price, sell_price))

            last_buy_index = None  # reiniciar para el siguiente par

    if not pares:
        return 0.0, 0

    hits = sum(1 for buy, sell in pares if sell > buy)
    total = len(pares)
    return hits / total * 100, total
