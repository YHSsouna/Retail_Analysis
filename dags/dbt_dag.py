from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='dbt_fact_sales_pipeline_airflow_native',
    default_args=default_args,
    description='A DAG to run dbt fact_sales model directly in Airflow container',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['dbt', 'sales'],
) as dag:

    dbt_run_fact_sales_task = BashOperator(
        task_id='dbt_run',
        bash_command="""
            pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
            dbt run --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
        """,
    )

    dbt_run_fact_sales_task
