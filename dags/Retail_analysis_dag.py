from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import ShortCircuitOperator
from datetime import datetime


def is_sunday():
    return datetime.now().weekday() == 6

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 7, 4),
    'depends_on_past': False,
    'retries': 1
}

with DAG('all_scrapers_combined',
         default_args=default_args,
         description='All supermarket scrapers + ETL convergence + ml_forecasting',
         schedule_interval='@daily',
         catchup=False) as dag:

    # Auchan pipeline
    with TaskGroup("auchan_pipeline") as auchan:
        scrape = BashOperator(task_id='scrape', bash_command='python /opt/airflow/web_scraping_scripts/auchan.py')
        section = BashOperator(task_id='section', bash_command='python /opt/airflow/web_scraping_scripts/auchan_section.py')
        categorize = BashOperator(task_id='categorize', bash_command='python /opt/airflow/web_scraping_scripts/auchan_cat.py')
        normalize = BashOperator(task_id='normalize', bash_command='python /opt/airflow/web_scraping_scripts/auchan_norm.py')


        scrape >> section >> categorize
        scrape >> normalize

    # Carrefour pipeline
    with TaskGroup("carrefour_pipeline") as carrefour:
        scrape = BashOperator(task_id='scrape', bash_command='python /opt/airflow/web_scraping_scripts/carrefour.py')
        section = BashOperator(task_id='section', bash_command='python /opt/airflow/web_scraping_scripts/carrefour_section.py')
        categorize = BashOperator(task_id='categorize', bash_command='python /opt/airflow/web_scraping_scripts/carrefour_cat.py')



        scrape >> section >> categorize

    # Géant pipeline
    with TaskGroup("biocoop_pipeline") as biocoop:
        scrape = BashOperator(task_id='scrape', bash_command='python /opt/airflow/web_scraping_scripts/biocoop.py')
        section = BashOperator(task_id='section',
                               bash_command='python /opt/airflow/web_scraping_scripts/biocoop_section.py')
        categorize = BashOperator(task_id='categorize',
                                  bash_command='python /opt/airflow/web_scraping_scripts/biocoop_cat.py')



        scrape >> section >> categorize

    # Monoprix pipeline
    with TaskGroup("labellevie_pipeline") as labellevie:
        scrape = BashOperator(task_id='scrape', bash_command='python /opt/airflow/web_scraping_scripts/labellevie.py')
        section = BashOperator(task_id='section', bash_command='python /opt/airflow/web_scraping_scripts/labellevie_section.py')
        categorize = BashOperator(task_id='categorize', bash_command='python /opt/airflow/web_scraping_scripts/labellevie_cat.py')
        normalize = BashOperator(task_id='normalize', bash_command='python /opt/airflow/web_scraping_scripts/labellevie_norm.py')


        scrape >> section >> categorize
        scrape >> normalize


    with TaskGroup("ML_pipeline") as ML_pipeline:
        run_if_sunday = ShortCircuitOperator(task_id='check_if_sunday',python_callable=is_sunday)
        ml_training = BashOperator(task_id='ml_training',bash_command='python /opt/airflow/web_scraping_scripts/XGBoost_forecasting.py')
        predictions = BashOperator(task_id='predictions',bash_command='python /opt/airflow/web_scraping_scripts/predictions.py')
        run_if_sunday >> ml_training >> predictions

    with TaskGroup("DBT_Models") as DBT_pipeline:
        stg_auchan = BashOperator(
            task_id='staging_auchan',
            bash_command="""
                    pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                    dbt run --select stg_auchan --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                """,
            trigger_rule='all_done'
        )

        inter_auchan = BashOperator(
            task_id='intermediate_auchan',
            bash_command="""
                    pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                    dbt run --select inter_auchan --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                """,
        )
        stg_carrefour = BashOperator(
            task_id='staging_carrefour',
            bash_command="""
                    pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                    dbt run --select stg_carrefour --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                """,
            trigger_rule='all_done'
        )
        inter_carrefour = BashOperator(
            task_id='Intermediate_Carrefour',
            bash_command="""
                    pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                    dbt run --select inter_carrefour --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                """,
        )
        stg_biocoop = BashOperator(
            task_id='staging_biocoop',
            bash_command="""
                            pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                            dbt run --select stg_biocoop --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                        """,
            trigger_rule='all_done'
        )

        inter_biocoop = BashOperator(
            task_id='Intermediate_biocoop',
            bash_command="""
                            pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                            dbt run --select inter_biocoop --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                        """,
        )
        stg_labellevie = BashOperator(
            task_id='staging_labellevie',
            bash_command="""
                                    pip install dbt-core==1.5.1 dbt-postgres==1.5.1 &&
                                    dbt run --select stg_labellevie --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
            trigger_rule='all_done'
        )
        inter_labellevie = BashOperator(
            task_id='Intermediate_Labellevie',
            bash_command="""
                                    dbt run --select inter_labellevie --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_date = BashOperator(
            task_id='dim_date',
            bash_command="""
                                    dbt run --select dim_date --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_categories = BashOperator(
            task_id='dim_categories',
            bash_command="""
                                    dbt run --select dim_category --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_product = BashOperator(
            task_id='dim_product',
            bash_command="""
                                    dbt run --select dim_product --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_picture = BashOperator(
            task_id='dim_picture',
            bash_command="""
                                    dbt run --select dim_picture --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_predictions = BashOperator(
            task_id='dim_predictions',
            bash_command="""
                                    dbt run --select dim_predictions --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        dim_store = BashOperator(
            task_id='dim_store',
            bash_command="""
                                    dbt run --select dim_store --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                """,
        )
        fact_sales = BashOperator(
            task_id='fact_sales',
            bash_command="""
                                            dbt run --select fact_sales --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
                                        """,
        )



    Test = BashOperator(
            task_id='Data_Quality_Tests',
            bash_command="""
                dbt test --project-dir /opt/airflow/retail/retail --profiles-dir /opt/airflow/retail
            """,
        )
    stg_auchan >> inter_auchan >> dim_date >> dim_categories >> dim_picture >>  dim_predictions >> dim_store >> dim_product >> fact_sales
    stg_labellevie >> inter_labellevie >> dim_date >> dim_categories >> dim_picture >>  dim_predictions >> dim_store >> dim_product >> fact_sales
    stg_biocoop >> inter_biocoop >> dim_date >> dim_categories >> dim_picture >>  dim_predictions >> dim_store >> dim_product >> fact_sales
    stg_carrefour >> inter_carrefour >> dim_date >> dim_categories >> dim_picture >>  dim_predictions >> dim_store >> dim_product >> fact_sales





    # Set dependencies — run ETL after all TaskGroups complete
    [auchan, carrefour, labellevie, biocoop, ML_pipeline] >> DBT_pipeline >> Test