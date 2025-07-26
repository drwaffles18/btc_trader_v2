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

def simular_capital_long_only(df, capital_inicial, señal_col='Eval Signal', precio_col='Close'):
    """
    Simula la evolución del capital siguiendo una estrategia long-only basada en señales de compra/venta.

    Args:
        df (pd.DataFrame): DataFrame con señales 'B' (buy) y 'S' (sell) en señal_col.
        capital_inicial (float): Capital inicial a invertir.
        señal_col (str): Nombre de la columna con las señales ('B', 'S').
        precio_col (str): Nombre de la columna con el precio de ejecución.

    Returns:
        float: Capital final tras aplicar la estrategia.
    """
    df = df.copy().reset_index(drop=True)
    capital = capital_inicial
    en_posicion = False
    precio_entrada = 0

    for i in range(len(df)):
        señal = df.at[i, señal_col]
        precio = df.at[i, precio_col]

        if señal == 'B' and not en_posicion:
            # Entramos al mercado
            precio_entrada = precio
            en_posicion = True

        elif señal == 'S' and en_posicion:
            # Salimos del mercado, calculamos ganancia
            cambio = precio / precio_entrada
            capital *= cambio
            en_posicion = False

    # Si quedamos con una posición abierta al final, asumimos que se cierra con el último precio
    if en_posicion:
        capital *= df[precio_col].iloc[-1] / precio_entrada

    return capital



