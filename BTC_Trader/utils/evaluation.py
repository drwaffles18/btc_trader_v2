# utils/evaluation.py

import numpy as np
import pandas as pd

def calcular_estadisticas_modelo(df, señal_col='B-H-S Signal', precio_col='Close'):
    pares = []
    buy_price = None

    for _, row in df.iterrows():
        señal = row[señal_col]
        precio = row[precio_col]

        if pd.isna(señal):
            continue

        if señal == 'B':
            buy_price = precio
        elif señal == 'S' and buy_price is not None:
            pares.append(precio - buy_price)
            buy_price = None

    if not pares:
        return 0.0, 0, 0.0, 0.0, 0.0

    pares = np.array(pares)
    ganancias = pares[pares > 0]
    perdidas = pares[pares <= 0]

    hit_rate = 100 * len(ganancias) / len(pares)
    ganancia_media = ganancias.mean() if len(ganancias) > 0 else 0.0
    perdida_media = perdidas.mean() if len(perdidas) > 0 else 0.0
    profit_factor = ganancias.sum() / abs(perdidas.sum()) if perdidas.sum() != 0 else np.inf

    return hit_rate, len(pares), ganancia_media, perdida_media, profit_factor

def calcular_estadisticas_long_only(df, señal_col='Signal Final', precio_col='Close'):
    """
    Evalúa pares BUY → SELL secuenciales.
    Solo cuenta trades largos. Ignora señales que no formen pares.
    """
    df = df.reset_index(drop=True)
    pares = []
    en_compra = False
    precio_compra = None

    for _, row in df.iterrows():
        señal = row[señal_col]
        precio = row[precio_col]

        if pd.isna(señal):
            continue

        if señal in ['B', 'BUY'] and not en_compra:
            # Inicio de operación
            precio_compra = precio
            en_compra = True

        elif señal in ['S', 'SELL'] and en_compra:
            # Cierre de operación
            ganancia = precio - precio_compra
            pares.append(ganancia)
            en_compra = False
            precio_compra = None

    if not pares:
        return 0.0, 0, 0.0, 0.0, 0.0

    pares = np.array(pares)
    ganancias = pares[pares > 0]
    perdidas = pares[pares <= 0]

    hit_rate = 100 * len(ganancias) / len(pares)
    ganancia_media = ganancias.mean() if len(ganancias) > 0 else 0.0
    perdida_media = perdidas.mean() if len(perdidas) > 0 else 0.0
    profit_factor = ganancias.sum() / abs(perdidas.sum()) if perdidas.sum() != 0 else np.inf

    return hit_rate, len(pares), ganancia_media, perdida_media, profit_factor

