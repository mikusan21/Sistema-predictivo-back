from pathlib import Path
import pandas as pd
import numpy as np
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import matplotlib.pyplot as plt
import joblib


PATH = Path(__file__).resolve().parent
df = pd.read_csv(PATH / 'df.csv')
# print(df.head())

# categorical = ['MEDTIP', 'MEDPET', 'MEDFF', 'MEDEST'] NINGUNA tiene buena corr asi que no voy a usar ninguna categoria
x = df[['STOCK_FIN', 'ds', 'CODIGO_MED', 'PRECIO']].copy()
x['ds'] = pd.to_datetime(x['ds'])
x['month'] = x['ds'].dt.month
x['year'] = x['ds'].dt.year
x.drop('ds', axis=1, inplace=True)
x['CODIGO_MED'] = x['CODIGO_MED'].astype('category')
x = pd.get_dummies(x, columns=['CODIGO_MED'], prefix='MED')
# print(x.head())

# idx = x['MED_91'] == True
# print(idx)

y = df['TOTAL_CONSUMO'].copy()
total_records = df['CODIGO_MED'].nunique()

X_train, X_test, y_train, y_test = train_test_split(x, y, test_size=.2, shuffle=False)


model = XGBRegressor()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

mae = mean_absolute_error(y_test, y_pred)
rmse = mean_squared_error(y_test, y_pred)
y_test_nonzero = y_test[y_test != 0]
y_pred_nonzero = y_pred[y_test != 0]
mape =  mean_absolute_percentage_error(y_test_nonzero, y_pred_nonzero)
print(f"global stadictics = MAE: {mae}, RMSE: {rmse}, MAPE: {mape}")
# joblib.dump(model, 'model_xgboost.pkl')

# codigo = 23438  
# columna = f"MED_{codigo}"
# pastillas_presentes = [col for col in X_test.columns if col.startswith("MED_") and X_test[col].any()]
# print(pastillas_presentes)

# if columna in X_test.columns and X_test[columna].any():
#     idx = X_test[columna] == True
#     y_test_p = y_test[idx]
#     y_pred_p = model.predict(X_test[idx])

#     mae = mean_absolute_error(y_test_p, y_pred_p)
#     rmse = np.sqrt(mean_squared_error(y_test_p, y_pred_p))
#     mape = mean_absolute_percentage_error(y_test_p[y_test_p != 0], y_pred_p[y_test_p != 0])

#     plt.plot(y_test_p.values, label="Real")
#     plt.plot(y_pred_p, label="Predicción")
#     plt.legend()
#     plt.title(f"Predicción vs Real para {columna}")
#     plt.show()

#     print(f"{columna} → MAE: {mae}, RMSE: {rmse}, MAPE: {mape}")
# else:
#     print(f"No hay datos de {columna} en el set de prueba")
