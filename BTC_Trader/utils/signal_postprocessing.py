# --- utils/signal_postprocessing.py ---
import pandas as pd

def eliminar_señales_consecutivas(df, columna='B-H-S Signal', señal='B'):
    """
    Elimina señales consecutivas iguales (como múltiples 'Buy') dejando solo la primera.
    Mantiene la primera aparición de la señal y pone NaN en las siguientes hasta que cambie.
    """
    df = df.copy()
    mask = (df[columna] == señal)
    consecutivos = mask & mask.shift(fill_value=False)
    df.loc[consecutivos, columna] = pd.NA
    return df

def limpiar_señales_consecutivas(df, columna='Momentum Signal'):
    """
    Elimina señales consecutivas iguales y propaga el último estado válido.
    Crea/actualiza la columna 'Signal Final'.
    """
    df = df.copy()
    df['Signal Final'] = df[columna]

    for i in range(1, len(df)):
        if df.at[i, 'Signal Final'] == df.at[i-1, 'Signal Final']:
            df.at[i, 'Signal Final'] = None

    df['Signal Final'] = df['Signal Final'].ffill()
    return df
