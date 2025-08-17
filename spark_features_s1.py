import os, re, math, requests, datetime as dt, pandas as pd, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, from_unixtime, regexp_extract, when, datediff, lit, round as sround
)
from pyspark.sql.types import StringType
from pyspark.sql import functions as F

# ---- config ----
DATA_DIR    = os.path.abspath(os.getenv("DAY_AGGS_DIR", "data/day_aggs"))   # where your .csv.gz files are
UNDERLYING  = os.getenv("UNDERLYING", "SPY")               # for moneyness & sigma
RISK_FREE   = float(os.getenv("RISK_FREE", "0.04"))
POLY_KEY    = os.getenv("POLYGON_API_KEY")

print(DATA_DIR)
spark = SparkSession.builder.appName("OptionsFeaturesV1").getOrCreate()

files = glob.glob(os.path.join(DATA_DIR, "*.csv.gz"))
print("Found:", len(files), "files")

df_raw = spark.read.csv(files, header=True, inferSchema=True)

print("---- raw schema ----")
df_raw.printSchema()
print("raw count:", df_raw.count())

# window_start_ms is actually any “epoch” scale; rename to generic
df_step1 = df_raw.select(
    col("ticker").alias("contract_symbol"),
    col("volume").alias("volume"),
    col("close").alias("option_close"),
    col("window_start").alias("window_start_raw"),
)

# Convert epoch (seconds/ms/µs/ns) → seconds
df_step1 = (
    df_step1
    .withColumn(
        "epoch_seconds",
        when(col("window_start_raw") > 1_000_000_000_000_000_000,  col("window_start_raw") / 1_000_000_000)  # ns → s
        .when(col("window_start_raw") > 1_000_000_000_000,         col("window_start_raw") / 1_000_000)      # µs → s
        .when(col("window_start_raw") > 1_000_000_000,             col("window_start_raw") / 1_000)          # ms → s
        .otherwise(col("window_start_raw").cast("double"))                                             # s
    )
    .withColumn("trade_ts", from_unixtime(col("epoch_seconds").cast("double")).cast("timestamp"))
    .withColumn("trade_date", to_date(col("trade_ts")))
    .drop("trade_ts")
)

df_step1.select("trade_date","contract_symbol","volume","option_close","window_start_raw").show(10, truncate=False)

# (Optional) write to parquet (this only writes the derived dataset; your CSVs stay put)
df_step1.write.mode("overwrite").parquet("stage/day_aggs_step1.parquet")
print("[done] wrote stage/day_aggs_step1.parquet")

print("distinct dates:", df_step1.select("trade_date").distinct().count())
df_step1.groupBy("trade_date").count().orderBy("trade_date").show(5)

# OCC regex parts (fast, vectorized)
root_re   = r"^O:([A-Z]+)\d{6}[CP]\d{8}$"
yymmdd_re = r"^O:[A-Z]+(\d{6})[CP]\d{8}$"
cp_re     = r"^O:[A-Z]+\d{6}([CP])\d{8}$"
strike_re = r"^O:[A-Z]+\d{6}[CP](\d{8})$"

print(df_step1)
df_step1.show()

