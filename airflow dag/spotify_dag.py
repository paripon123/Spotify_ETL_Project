from datetime import timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Spotify_ETL import spotify_ETL_func
from Weekly_Email import weekly_email

default_agrs =  {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' : days_ago(2),
    # Put Email In
    'email': ['email'],
    'email_on_failure' :True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 7)
}

dag = DAG(
    'spotify_dag',
    default_args= default_agrs,
    description= 'Spotify Weekly Run',
    schedule_interval= '0 12 * * 0'
)

t1 = PythonOperator(
    task_id = "RUN_ETL",
    python_callable= spotify_ETL_func,
    dag = dag
)
t2 = PythonOperator(
    task_id = 'Weekly_Email',
    python_callable= weekly_email,
    dag = dag
)

t1 >> t2
