from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="demo_schedule_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/3 * * * *",  # chạy mỗi 1 phút
    catchup=False,
    tags=["demo", "schedule"],
) as dag:

    print_time = BashOperator(
        task_id="print_time",
        bash_command="date",
    )