from airflow import DAG 
from airflow.operators.empty import EmptyOperator
from datetime import datetime
with DAG(dag_id="dag2",
         description="Este será el proceso main",
         start_date=datetime(2023,7,6),
         schedule_interval="@once") as dag:
    t1 = EmptyOperator(task_id="dummy")