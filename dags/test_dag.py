from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_hello():
    print("Hello, Airflow!")  # Это появится в логах Airflow


# Аргументы DAG по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Определяем DAG
dag = DAG(
    dag_id='simplest_test_dag',  # Уникальное имя
    default_args=default_args,
    schedule_interval='@daily',  # Запускать раз в день
    catchup=False,  # Не запускать пропущенные за прошлые периоды
    tags=['test'],
)

# Задача (PythonOperator)
task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)