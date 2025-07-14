import os
import xgboost as xgb
import pandas as pd
import numpy as np
from datetime import timedelta
import joblib  # Only used if you switch to .pkl later
import psycopg2

# Database connection
DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

def fetch_latest_data():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM public_marts.time_series_data;")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def create_features(df, lag_list=[1, 7, 14], window_list=[7, 14, 30]):
    df = df.rename(columns={'date': 'ds', 'stock_diff_hors_restock': 'y'})
    df = df[['ds', 'y']]
    df['ds'] = pd.to_datetime(df['ds'])
    df = df.sort_values('ds')
    df['y'] = pd.to_numeric(df['y'], errors='coerce')

    df_features = df.copy()
    df_features['dayofweek'] = df_features['ds'].dt.dayofweek
    df_features['month'] = df_features['ds'].dt.month
    df_features['year'] = df_features['ds'].dt.year
    df_features['day'] = df_features['ds'].dt.day
    df_features['quarter'] = df_features['ds'].dt.quarter

    for lag in lag_list:
        df_features[f'lag_{lag}'] = df_features['y'].shift(lag)
    for window in window_list:
        df_features[f'rolling_mean_{window}'] = df_features['y'].rolling(window=window).mean().shift(1)
        df_features[f'rolling_std_{window}'] = df_features['y'].rolling(window=window).std().shift(1)
        df_features[f'rolling_min_{window}'] = df_features['y'].rolling(window=window).min().shift(1)
        df_features[f'rolling_max_{window}'] = df_features['y'].rolling(window=window).max().shift(1)

    df_features.fillna(0, inplace=True)
    return df_features

# Load full data
df = fetch_latest_data()
df['stock'] = pd.to_numeric(df['stock'], errors='coerce')
df = df[df['stock'] != 9995]

# Focus on most relevant products
valid_images = df["product_id"].value_counts()
valid_images = valid_images[valid_images > 90].index
df = df[df["product_id"].isin(valid_images)]

# Group data by product
product_ids = df['product_id'].unique()
forecast_results = {}

models_dir = os.path.join(os.getcwd(), "models")

for product_id in product_ids:
    df_product = df[df['product_id'] == product_id]
    df_product = df_product[['date', 'stock_diff_hors_restock']].rename(columns={'date': 'ds', 'stock_diff_hors_restock': 'y'})
    df_product['ds'] = pd.to_datetime(df_product['ds'])
    df_product = df_product.sort_values('ds')
    df_product['y'] = pd.to_numeric(df_product['y'], errors='coerce')
    df_product.dropna(inplace=True)
    model_path = os.path.join(models_dir, f"model_df_f{product_id}.json")
    if not os.path.exists(model_path):
        print(f"Model not found for product {product_id}")
        continue

    model = xgb.XGBRegressor()
    model.load_model(model_path)

    future_predictions = []
    history = df_product.copy()

    for step in range(30):
        df_feat = create_features(history)
        latest_row = df_feat.iloc[-1:].copy()
        latest_row['ds'] = latest_row['ds'] + timedelta(days=1)
        future_date = latest_row['ds'].values[0]

        X_pred = latest_row.drop(columns=['ds', 'y'])
        y_pred = model.predict(X_pred)[0]

        # Append predicted value
        future_predictions.append({'product_id': product_id, 'ds': future_date, 'predicted_y': y_pred})

        # Append to history for next step's lag/rolling
        history = pd.concat([history, pd.DataFrame({'ds': [future_date], 'y': [y_pred]})], ignore_index=True)

    forecast_results[product_id] = future_predictions

# Convert to a single DataFrame
forecast_df = pd.concat([pd.DataFrame(preds) for preds in forecast_results.values()], ignore_index=True)

# Save or print





import psycopg2
from psycopg2.extras import execute_values

TARGET_TABLE = "public_intermediate.predicted_stock_diff"

# Ensure datetime and float types
forecast_df['ds'] = pd.to_datetime(forecast_df['ds'])
forecast_df['predicted_y'] = forecast_df['predicted_y'].astype(float)



try:
    conn = psycopg2.connect(**DB_PARAMS)
    cursor = conn.cursor()

    # Create table if it doesn't exist

    # Optional: clear existing predictions
    cursor.execute(f"DELETE FROM {TARGET_TABLE};")

    # Insert new predictions (id auto-generated)
    insert_query = f"INSERT INTO {TARGET_TABLE} (product_id, ds, predicted_y) VALUES %s"
    values = list(forecast_df[['product_id', 'ds', 'predicted_y']].itertuples(index=False, name=None))
    execute_values(cursor, insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Predictions inserted into PostgreSQL.")

except Exception as e:
    print(f"❌ PostgreSQL error: {e}")




