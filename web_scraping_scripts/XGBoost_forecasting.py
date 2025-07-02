import psycopg2
from sklearn.model_selection import RandomizedSearchCV
import xgboost as xgb
import pandas as pd
import numpy as np
import os
import mlflow
import matplotlib.pyplot as plt
from datetime import datetime
from mlflow.tracking import MlflowClient
from sklearn.metrics import r2_score

mlflow.set_tracking_uri("http://mlflow:5000")  # Change if needed


DB_PARAMS = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "postgres",
    "port": "5432"
}


# %%
def fetch_biocoop_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_PARAMS)
        cursor = conn.cursor()

        # Query to fetch data
        cursor.execute("SELECT * FROM public_marts.time_series_data;")
        rows = cursor.fetchall()

        # Dynamically get column names from cursor description
        columns = [desc[0] for desc in cursor.description]

        # Create the DataFrame with all columns
        df = pd.DataFrame(rows, columns=columns)

        # Close connection
        cursor.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error: {e}")


# Run the function
df = fetch_biocoop_data()
# %%
# Identify image_urls with more than 40 occurrences
valid_images = df["image_url"].value_counts()
valid_images = valid_images[valid_images > 90].index

# Filter the DataFrame
df = df[df["image_url"].isin(valid_images)]
# %%
df['stock'] = pd.to_numeric(df['stock'], errors='coerce')

# %%
product_ids_to_remove = df.loc[df['stock'] == 9995, 'product_id'].unique()


#%%
df = df[~df['product_id'].isin(product_ids_to_remove)]
df_grouped = (
    df.groupby('product_id')['stock_diff_hors_restock'].sum()
    .reset_index()
    .sort_values(by='stock_diff_hors_restock', ascending=False)
    .head(20)
)

unique_id = [df_grouped['product_id'].unique()]

product_dfs = {}

for pid in df_grouped['product_id'].unique():
    product_dfs[f"df_f{pid}"] = df[df['product_id'] == pid]


def create_features(df, lag_list=[1, 7, 14], window_list=[7, 14, 30]):
    df = df.rename(columns={'date': 'ds', 'stock_diff_hors_restock': 'y'})
    df = df[['ds', 'y']]  # Include stock
    df['y'] = pd.to_numeric(df['y'], errors='coerce')

    df_features = df.copy()

    # Ensure datetime format
    df_features['ds'] = pd.to_datetime(df_features['ds'])
    df_features = df_features.sort_values('ds')

    # Date-based features
    df_features['dayofweek'] = df_features['ds'].dt.dayofweek
    df_features['month'] = df_features['ds'].dt.month
    df_features['year'] = df_features['ds'].dt.year
    df_features['day'] = df_features['ds'].dt.day
    df_features['quarter'] = df_features['ds'].dt.quarter

    # Lag features for y (target)
    for lag in lag_list:
        df_features[f'lag_{lag}'] = df_features['y'].shift(lag)

    # Window features for y (rolling stats)
    for window in window_list:
        df_features[f'rolling_mean_{window}'] = df_features['y'].rolling(window=window).mean().shift(1)
        df_features[f'rolling_std_{window}'] = df_features['y'].rolling(window=window).std().shift(1)
        df_features[f'rolling_min_{window}'] = df_features['y'].rolling(window=window).min().shift(1)
        df_features[f'rolling_max_{window}'] = df_features['y'].rolling(window=window).max().shift(1)

    # Optionally: You could also include lag/rolling for `stock` if relevant

    # Fill missing values
    df_features.fillna(0, inplace=True)

    return df_features

today = datetime.today().strftime('%Y-%m-%d')

base_mlruns_dir = os.path.join(os.getcwd(), "mlruns")
os.makedirs(base_mlruns_dir, exist_ok=True) # Ensure the base mlruns directory exists


param_dist = {
    'n_estimators': [100, 300, 500, 1000],
    'max_depth': [3, 5, 7],
    'learning_rate': [0.01, 0.05, 0.1],
    'subsample': [0.7, 0.8, 1.0],
    'colsample_bytree': [0.7, 0.8, 1.0],
}

tolerance = 0.1
l = 0

for i, (product_id, df) in enumerate(product_dfs.items()):
    # Feature engineering & preprocessing
    df_features = create_features(df)
    for col in ['lag_1', 'lag_7', 'lag_14', 'y']:
        df_features[col] = pd.to_numeric(df_features[col], errors='coerce')
    df_features.dropna(inplace=True)

    # Train/test split
    X = df_features.drop(columns=['ds', 'y'])
    y_series = df_features['y']
    train_size = int(len(df_features) * 0.8)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y_series[:train_size], y_series[train_size:]

    # Set experiment
    experiment_name = f"{product_id}_product_forecasting_models"

    client = MlflowClient()
    def ensure_experiment_exists(experiment_name):
        experiment = client.get_experiment_by_name(experiment_name)

        if experiment is None:
            # Experiment doesn't exist at all – safe to create
            print(f"Creating new experiment: {experiment_name}")
            mlflow.create_experiment(experiment_name)
        elif experiment.lifecycle_stage == 'deleted':
            # Restore the deleted experiment (soft-deleted)
            print(f"Restoring deleted experiment: {experiment_name}")
            client.restore_experiment(experiment.experiment_id)
        else:
            print(f"Experiment {experiment_name} already exists and is active.")


    # Usage before setting the experiment
    ensure_experiment_exists(experiment_name)
    mlflow.set_experiment(experiment_name)
    print(f"Using experiment: {experiment_name}")

    with mlflow.start_run(run_name=f"product_{product_id}") as run:
        print(f"Started run: {run.info.run_id}")

        # Hyperparameter tuning using RandomizedSearchCV
        base_model = xgb.XGBRegressor(objective='reg:squarederror', n_jobs=-1, verbosity=0)
        search = RandomizedSearchCV(
            base_model,
            param_distributions=param_dist,
            n_iter=10,             # number of random param combinations to try (adjust as needed)
            scoring='neg_root_mean_squared_error',
            cv=3,
            verbose=0,
            random_state=42,
            n_jobs=-1
        )
        search.fit(X_train, y_train)

        best_params = search.best_params_
        print(f"Best params for {product_id}: {best_params}")

        # Train best model on full train data
        model = xgb.XGBRegressor(objective='reg:squarederror', **best_params, verbosity=0)
        model.fit(X_train, y_train)

        # Predict and calculate RMSE, accuracy within tolerance
        preds = model.predict(X_test)
        rmse = ((preds - y_test) ** 2).mean() ** 0.5
        y_test_safe = y_test.replace(0, 1e-8)
        relative_errors = np.abs(preds - y_test) / np.abs(y_test_safe)
        accuracy = np.mean(relative_errors <= tolerance)
        r2 = r2_score(y_test, preds)

        # Log params and metrics
        mlflow.log_params(best_params)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("accuracy_10pct", accuracy)

        # Plotting (same as your original code)
        fig, ax = plt.subplots(figsize=(14, 7))
        ax.plot(df_features['ds'], df_features['y'], label='Full Dataset', color='blue', alpha=0.5)
        test_df = df_features.iloc[train_size:]
        ax.plot(test_df['ds'], test_df['y'], label='Test Set (True)', color='blue', marker='o', markersize=4)
        ax.plot(test_df['ds'], preds, label='Test Set (Predicted)', color='red', linestyle='--', marker='x', markersize=4)
        ax.axvline(x=test_df['ds'].iloc[0], color='green', linestyle='-.', alpha=0.7, label='Train/Test Split')
        ax.set_xlabel('Date')
        ax.set_ylabel('Stock Difference')
        ax.set_title(f'XGBoost: True vs Predicted for {product_id}')
        ax.grid(True, alpha=0.3)
        ax.legend()
        fig.tight_layout()

        # Save and log artifacts as before
        artifact_uri = mlflow.get_artifact_uri()
        local_artifact_root = artifact_uri.replace("file://", "")
        relative_path = local_artifact_root.replace("/mlflow/artifacts/", "", 1)
        local_run_dir = os.path.join(base_mlruns_dir, relative_path)
        plots_dir = os.path.join(local_run_dir, "plots")
        models_dir = os.path.join(local_run_dir, "xgb_model")
        os.makedirs(plots_dir, exist_ok=True)
        os.makedirs(models_dir, exist_ok=True)

        plot_filename = f"plot_{product_id}_{today}_{run.info.run_id[:8]}.png"
        plot_local_path = os.path.join(plots_dir, plot_filename)
        fig.savefig(plot_local_path)
        mlflow.log_artifact(plot_local_path, "plots")
        plt.close(fig)

        model_filename = f"model_{product_id}_{today}_{run.info.run_id[:8]}.bin"
        model_local_path = os.path.join(models_dir, model_filename)
        model.save_model(model_local_path)
        mlflow.log_artifact(model_local_path, "xgb_model")

        mlflow.set_tag("product_id", product_id)
        mlflow.set_tag("date", today)

        registered_model_name = f"xgb_model_{product_id}"
        result = mlflow.xgboost.log_model(
            model,
            artifact_path="xgb_model",
            registered_model_name=registered_model_name
        )

        # Get latest version and promote to Staging
        client = mlflow.tracking.MlflowClient()
        latest_versions = client.get_latest_versions(name=registered_model_name, stages=["None"])
        model_version = latest_versions[-1].version if latest_versions else 1


        l += 1
        print(f"✅ Tracked {product_id} ({l}/{len(product_dfs)}), RMSE: {rmse:.3f}, Accuracy_10pct: {accuracy:.3f}")
        print(f"🔗 Run: {mlflow.get_tracking_uri()}/#/experiments/{run.info.experiment_id}/runs/{run.info.run_id}")
