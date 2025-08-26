from airflow import DAG
from airflow.decorators import task
import datetime
from clean import clear_dir, unzip_gz, spark_count

default_args = {
    "owner" : "Nick",
    "depends_on_past" : False,
    "retries" : 1
}

with DAG (
    dag_id="cleaning_spinoff",
    default_args=default_args,
    start_date=datetime.datetime(2025, 8, 1),
    schedule=None,
    catchup=False
) as dag:
    
    # or
    # task_clear = PythonOperator(
    #     task_id='task_clear',
    #     python_callable=clear_dir,
    # )

    @task
    def task_clear():
        return clear_dir()
    
    @task
    def task_unzip():
        return unzip_gz()
    
    @task
    def task_count():
        return spark_count
    

    # dependencies
    c = task_clear()
    u = task_unzip()
    k = task_count()

c >> u >> k
