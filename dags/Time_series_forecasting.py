from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 19),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'time_series_forecasting',
    default_args=default_args,
    description='DAG to train ml models',
    schedule_interval='@weekly',  # Runs daily
)

ml_training = BashOperator(
    task_id='ml_training',
    bash_command='python /opt/airflow/web_scraping_scripts/XGBoost_forecasting.py',
    dag=dag,
)

ml_training