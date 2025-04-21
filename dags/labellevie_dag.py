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
    'labellevie_scraper',
    default_args=default_args,
    description='DAG to scrape data from La Belle Vie',
    schedule_interval='@daily',
)

scrape_labellevie = BashOperator(
    task_id='scrape_labellevie',
    bash_command='python /opt/airflow/web_scraping_scripts/labellevie.py',
    dag=dag,
)

categorize_labellevie = BashOperator(
    task_id='categorize_labellevie',
    bash_command='python /opt/airflow/web_scraping_scripts/labellevie_cat.py',
    dag=dag,
)

normalize_labellevie = BashOperator(
    task_id='normalize_labellevie',
    bash_command='python /opt/airflow/web_scraping_scripts/labellevie_norm.py',
    dag=dag,
)

scrape_labellevie >> categorize_labellevie >> normalize_labellevie
