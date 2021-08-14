from datetime import timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Spotify_ETL import spotify_ETL_func

etl_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' : days_ago(2),
    'email': ['paripontt@gmail.com'],
    'email_on_failure' :True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 7)

}

etl_dag = DAG (
    'spotify_dag',
    default_args=etl_args,
    description= 'Spotify ETL',
    schedule_interval= '0 12 * * 0'
)

run_etl = PythonOperator(
    task_id  = 'spotify_etl_postgresql',
    python_callable= spotify_ETL_func,
    dag = etl_dag
)

run_etl
