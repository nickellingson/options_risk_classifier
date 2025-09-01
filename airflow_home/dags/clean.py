from pyspark.sql import SparkSession
import os
import gzip

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


# if __name__=="__main__":
    # clear_dir()
    # unzip_gz()
    # spark_count()