from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from common.print_utils import print_hello

with DAG(
    dag_id="hello_world_dag",
    schedule="@once",
    start_date=None,
    catchup=False,
) as dag:
    start_task = EmptyOperator(
        task_id="start_task",
    )

    hello_task = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )

    end_task = EmptyOperator(
        task_id="end_task",
    )

    start_task >> hello_task >> end_task
