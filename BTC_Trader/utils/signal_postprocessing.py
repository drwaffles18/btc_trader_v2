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
