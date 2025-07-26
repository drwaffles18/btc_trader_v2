import pandas as pd

def eliminar_señales_consecutivas(df, columna='B-H-S Signal', señal='B'):
    """
    Elimina señales consecutivas iguales (como múltiples 'Buy') dejando solo la primera.
    Mantiene la primera aparición de la señal, y pone NaN en las subsiguientes hasta que cambie.

    Parámetros:
    - df: DataFrame con las señales.
    - columna: nombre de la columna donde están las señales.
    - señal: valor de la señal a filtrar (por defecto 'B' de Buy).

    Retorna:
    - DataFrame con las señales ajustadas.
    """
    df = df.copy()
    mask = (df[columna] == señal)
    
    # Shift para comparar con la anterior
    consecutivos = mask & mask.shift(fill_value=False)
    
    # Poner NaN donde haya repetidos
    df.loc[consecutivos, columna] = pd.NA

    return df

def limpiar_señales_consecutivas(df, columna='Momentum Signal'):
    """
    Elimina señales consecutivas iguales propagando solo cambios de estado (ej: BUY seguido solo de SELL)
    """
    df = df.copy()
    
    # Solo dejamos los cambios (eliminamos señales consecutivas)
    df['Signal Final'] = df[columna]
    for i in range(1, len(df)):
        if df.at[i, 'Signal Final'] == df.at[i-1, 'Signal Final']:
            df.at[i, 'Signal Final'] = None

    # Propagamos la última señal válida hacia adelante
    df['Signal Final'] = df['Signal Final'].ffill()

    return df

