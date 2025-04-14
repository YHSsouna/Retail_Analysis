from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 24),
    'retries': 1,
}

dag = DAG(
    'carrefour_scraper',
    default_args=default_args,
    description='DAG to scrape data from Carrefour',
    schedule_interval='@daily',
)

scrape_carrefour = BashOperator(
    task_id='scrape_carrefour',
    bash_command='python /opt/airflow/web_scraping_scripts/carrefour.py',
    dag=dag,
)

categorize_carrefour = BashOperator(
    task_id='categorize_carrefour',
    bash_command='python /opt/airflow/web_scraping_scripts/carrefour_cat.py',
    dag=dag,
)

scrape_carrefour >> categorize_carrefour
