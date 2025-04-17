from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'retries': 1,
}

# Define DAG
dag = DAG(
    'auchan_scraper',
    default_args=default_args,
    description='DAG to scrape data from Auchan',
    schedule_interval='@daily',  # Runs daily
)

# Scraping task using BashOperator
scrape_auchan = BashOperator(
    task_id='scrape_auchan',
    bash_command='python /opt/airflow/web_scraping_scripts/auchan.py',
    dag=dag,
)

categorize_auchan = BashOperator(
    task_id='categorize_auchan',
    bash_command='python /opt/airflow/web_scraping_scripts/auchan_cat.py',
    dag=dag,
)
scrape_auchan >> categorize_auchan
