from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

# Update these imports
from ingestion.apiaq_ingestion import main as ingest_openaq
from transformations.bronze_to_silver import clean_location_data as bronze_to_silver
from transformations.silver_to_gold_firstAnalysis import transform_location_data as silver_to_gold_first
from transformations.silver_to_gold_secondAnalysis import analyze_sensor_instrument_data as silver_to_gold_second

default_args = {
    'owner': 'yvann',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}

with DAG('env_monitoring_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         max_active_runs=1) as dag:

    # Data ingestion
    ingest_task = PythonOperator(
        task_id='ingest_openaq_data',
        python_callable=ingest_openaq
    )

    # Data validation
    validate_raw_data = S3ListOperator(
        task_id='validate_raw_files',
        aws_conn_id='aws_default',
        bucket='openaq-locations-data',
        prefix='bronze/locations/',
        do_xcom_push=True
    )

    # Bronze to Silver transformation
    bronze_to_silver_task = PythonOperator(
        task_id='bronze_to_silver_transformation',
        python_callable=bronze_to_silver
    )

    # Parallel Silver to Gold transformations
    silver_to_gold_first = PythonOperator(
        task_id='silver_to_gold_first_analysis',
        python_callable=silver_to_gold_first
    )

    silver_to_gold_second = PythonOperator(
        task_id='silver_to_gold_second_analysis',
        python_callable=silver_to_gold_second
    )

    # Data loading
    load_to_postgis = PostgresOperator(
        task_id='load_to_postgis',
        postgres_conn_id='postgres_default',
        sql=[
            """
            CREATE TABLE IF NOT EXISTS country_stats (
                country_code VARCHAR(3) PRIMARY KEY,
                location_count INT,
                avg_latitude FLOAT,
                avg_longitude FLOAT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS sensor_stats (
                location_id INT,
                parameter_name VARCHAR(50),
                sensor_count INT
            )
            """
        ]
    )

    # Define workflow
    (ingest_task 
     >> validate_raw_data
     >> bronze_to_silver_task
     >> [silver_to_gold_first, silver_to_gold_second]
     >> load_to_postgis)