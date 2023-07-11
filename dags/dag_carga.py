from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'Dilan_Zurita',
    'start_date': datetime(2023, 7, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('csv_to_postgres', default_args=default_args, schedule_interval=timedelta(minutes=1))

# Tarea para establecer la conexión a PostgreSQL
establish_connection_task = PostgresOperator(
    task_id='establish_connection',
    postgres_conn_id='postgres_default',  # Debes configurar una conexión a PostgreSQL en Airflow
    sql='SELECT 1;',
    dag=dag
)

# Tarea para borrar la tabla 'carros'
drop_table_task = PostgresOperator(
    task_id='drop_table_carros',
    postgres_conn_id='postgres_default',  # Debes configurar una conexión a PostgreSQL en Airflow
    sql='DROP TABLE IF EXISTS carros;',
    dag=dag
)

# Tarea para crear la tabla 'carros'
create_table_task = PostgresOperator(
    task_id='create_table_carros',
    postgres_conn_id='postgres_default',  # Debes configurar una conexión a PostgreSQL en Airflow
    sql='''
    CREATE TABLE carros (
        Marca VARCHAR(255),
        Placa VARCHAR(255),
        precio VARCHAR(255)
    );
    ''',
    dag=dag
)

# Tarea para insertar 200 registros aleatorios en la tabla 'carros'
insert_records_task = PostgresOperator(
    task_id='insert_records_to_carros',
    postgres_conn_id='postgres_default',  # Debes configurar una conexión a PostgreSQL en Airflow
    sql='''
    INSERT INTO carros (Marca, Placa, precio)
    SELECT
        'Marca ' || id,
        'Placa ' || id,
        'Precio ' || id
    FROM generate_series(1, 200) AS id;
    ''',
    dag=dag
)

# Tarea para eliminar 50 registros de la tabla 'carros'
delete_records_task = PostgresOperator(
    task_id='delete_records_from_carros',
    postgres_conn_id='postgres_default',  # Debes configurar una conexión a PostgreSQL en Airflow
    sql='DELETE FROM carros WHERE ctid IN (SELECT ctid FROM carros LIMIT 50);',
    dag=dag
)

# Definir las dependencias entre las tareas
establish_connection_task >> drop_table_task >> create_table_task >> insert_records_task >> delete_records_task
