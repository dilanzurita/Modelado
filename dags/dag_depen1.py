from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def saludar():
    print("HOLA COMO ESTÁS??")

with DAG(dag_id="dependencias",
         description="Plantilla para crear dependencias",
         schedule_interval="@once",
         start_date=datetime(2023,7,6)) as dag:
    t1 = PythonOperator(task_id="tarea1",
                        python_callable=saludar)
    t2 = BashOperator(task_id="tarea2",
                      bash_command="echo 'tarea2'")
    t3 = BashOperator(task_id="tarea3",
                      bash_command="echo 'tarea3'")
    t4 = BashOperator(task_id="tarea4",
                      bash_command="echo 'tarea4'")
    
    t1.set_downstream(t2)
    t2.set_downstream([t3,t4])