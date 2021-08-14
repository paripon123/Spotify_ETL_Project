from datetime import timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from Weekly_Email import weekly_email


email_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date' : days_ago(2),
    # Put Email in
    'email': ['email'],
    'email_on_failure' :True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes = 7)

}


email_dag = DAG (
    'spotify_email_dag',
    default_args=email_args,
    description= 'Spotify Weekly Email',
    schedule_interval= '0 12 * * 0'
)


run_email = PythonOperator(
    task_id  = 'spotify_email_weekly',
    python_callable= weekly_email,
    dag = email_dag
)

run_email
