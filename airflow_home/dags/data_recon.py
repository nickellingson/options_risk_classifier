# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime

# def say_hi():
#     print(">>> Hello World, Airflow is working!")

from pyspark.sql import SparkSession
import os
import gzip

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime

spark = SparkSession.builder.appName("MoreCleaning").getOrCreate()

def clear_dir():
    for dirpath, _, filenames in os.walk("./cleaning_spinoff/dest"):
        print(dirpath, filenames)
        for file in filenames:
            file_path = os.path.join(dirpath, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
    print("Directory cleared")

def unzip_gz():

    for dirpath, _, filenames in os.walk("./data/day_aggs"):
        print(dirpath, filenames)

        for file in filenames:

            file_path = os.path.join(dirpath, file)
            # remove .gz
            file = "".join(file.split(".")[:-1])
            destination_path = os.path.join("./cleaning_spinoff/dest", file)
            try:
                with gzip.open(file_path, "rb") as f_in:
                    with open(destination_path, "wb") as f_out:
                        f_out.write(f_in.read())
            except FileNotFoundError:
                print("No file found")
            except Exception as e:
                print("Error occurred", e)
    print("New files unzipped and sent")

def spark_count():
    files = "./cleaning_spinoff/dest"

    df_raw = spark.read.csv(files, header=True, inferSchema=True)

    df_raw.printSchema()
    print("Count:", df_raw.count())

# with DAG(
#     dag_id="hello_world",
#     start_date=datetime(2024,1,1),
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
#     hi_task = PythonOperator(
#         task_id="hi_task",
#         python_callable=say_hi,
#     )

with DAG (
    dag_id="data_recon",
    start_date=datetime(2024,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    task_clear = PythonOperator(
        task_id="task_clear",
        python_callable=clear_dir,
    )

    task_unzip = PythonOperator(
        task_id="task_unzip",
        python_callable=unzip_gz,
    )

    task_count = PythonOperator(
        task_id="task_count",
        python_callable=spark_count,
    )
    task_clear >> task_unzip >> task_count

# or

#     @task
#     def task_clear():
#         return clear_dir()
    
#     @task
#     def task_unzip():
#         return unzip_gz()
    
#     @task
#     def task_count():
#         return spark_count
    

#     # dependencies
#     c = task_clear()
#     u = task_unzip()
#     k = task_count()

# c >> u >> k