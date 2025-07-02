from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 13),
    'retries': 1,
}

dag = DAG(
    'biocoop_scraper',
    default_args=default_args,
    description='DAG to scrape data from biocoop',
    schedule_interval='@daily',
)

scrape_biocoop = BashOperator(
    task_id='scrape_biocoop',
    bash_command='python /opt/airflow/web_scraping_scripts/biocoop.py',
    dag=dag,
)

categorize_biocoop = BashOperator(
    task_id='categorize_biocoop',
    bash_command='python /opt/airflow/web_scraping_scripts/biocoop_cat.py',
    dag=dag,
)

section_biocoop = BashOperator(
    task_id='section_biocoop',
    bash_command='python /opt/airflow/web_scraping_scripts/biocoop_section.py',
    dag=dag,
)

scrape_biocoop >> section_biocoop >> categorize_biocoop
