# --- utils/model_bayes.py ---
# Modelo Bayesiano basado en modelo entrenado guardado como .pkl

from sklearn.preprocessing import StandardScaler, LabelEncoder
import pandas as pd
import pickle

class BayesSignalPredictor:
    def __init__(self, model_path='modelos/best_model_bayes.pkl'):
        # Cargar el modelo de Bayes
        with open(model_path, 'rb') as model_file:
            self.model = pickle.load(model_file)
        self.scaler = StandardScaler()
        self.label_encoders = {
            'MACD Comp': LabelEncoder(),
            'Cross Check': LabelEncoder()
        }
        self.initialized = False

    def prepare_data(self, df):
        # Filtrar el DataFrame para solo las columnas necesarias
        columns_to_keep = ['EMA20', 'EMA50', 'EMA200', 'EMA_12', 'EMA_26', 'MACD', 'Signal_Line', 'RSI', '%K', '%D', 
                           'MACD Comp', 'Cross Check', 'EMA20 Check', 'EMA 200 Check', 'RSI Check']

        df = df.copy()
        df = df[columns_to_keep]

        # Escalar columnas numéricas
        features_to_scale = ['EMA20', 'EMA50', 'EMA200', 'EMA_12', 'EMA_26', 'MACD', 'Signal_Line', 'RSI', '%K', '%D']
        if not self.initialized:
            self.scaler.fit(df[features_to_scale])
            self.initialized = True
        df[features_to_scale] = self.scaler.transform(df[features_to_scale])

        # Codificar las columnas categóricas
        df['MACD Comp'] = self.label_encoders['MACD Comp'].fit_transform(df['MACD Comp'])
        df['Cross Check'] = self.label_encoders['Cross Check'].fit_transform(df['Cross Check'])

        return df

    def predict_signals(self, df):
        # Crear la columna si no existe
        if 'B-H-S Signal' not in df.columns:
            df['B-H-S Signal'] = np.nan
    
        # Definir columnas requeridas
        required_columns = ['EMA20', 'EMA50', 'EMA200', 'EMA_12', 'EMA_26', 'MACD', 'Signal_Line', 
                            'RSI', '%K', '%D', 'MACD Comp', 'Cross Check', 'EMA20 Check', 
                            'EMA 200 Check', 'RSI Check']
    
        # Filtrar filas candidatas a predecir
        candidatas = df[df['B-H-S Signal'].isna()].dropna(subset=required_columns)
        print("✅ Filas después del dropna (solo en columnas del modelo):", candidatas.shape[0])
    
        if not candidatas.empty:
            prepared_data = self.prepare_data(candidatas)
            predictions = self.model.predict(prepared_data)
            df.loc[candidatas.index, 'B-H-S Signal'] = predictions
    
        return df


