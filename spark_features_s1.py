import os, re, math, requests, datetime as dt, pandas as pd, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    format_string, col, to_date, from_unixtime, regexp_extract, when, datediff, lit, round as sround
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

print(df_step1)
df_step1.show()

# OCC regex clean up
root_re   = r"^O:([A-Z0-9]+)\d{6}[CP]\d{8}$"
yymmdd_re = r"^O:[A-Z0-9]+(\d{6})[CP]\d{8}$"
cp_re     = r"^O:[A-Z0-9]+\d{6}([CP])\d{8}$"
strike_re = r"^O:[A-Z0-9]+\d{6}[CP](\d{8})$"

df_occ = (
    df_step1
    .withColumn("underlying_root", regexp_extract(col("contract_symbol"), root_re, 1))
    .withColumn("yymmdd",          regexp_extract(col("contract_symbol"), yymmdd_re, 1))
    .withColumn("cp",              regexp_extract(col("contract_symbol"), cp_re,   1))
    .withColumn("strike_raw",      regexp_extract(col("contract_symbol"), strike_re, 1))
    .withColumn("strike_price",    (col("strike_raw").cast("double") / lit(1000.0)))
)

# Build expiration date safely using numeric pieces
df_occ = (
    df_occ
    .withColumn("yy", col("yymmdd").substr(1, 2).cast("int"))
    .withColumn("mm", col("yymmdd").substr(3, 2))
    .withColumn("dd", col("yymmdd").substr(5, 2))
    .withColumn("yyyy", when(col("yy") <= 69, col("yy") + lit(2000)).otherwise(col("yy") + lit(1900)))
    .withColumn("expiration_date_str", format_string("%04d-%s-%s", col("yyyy"), col("mm"), col("dd")))
    .withColumn("expiration_date", to_date(col("expiration_date_str")))
    .drop("yy","mm","dd","yyyy","expiration_date_str")
)

# Call/Put → 1/0 (Binary)
df_occ = df_occ.withColumn("option_is_call", when(col("cp") == "C", lit(1)).otherwise(lit(0)))

# Parse failure
parse_fail = df_occ.filter(
    (col("underlying_root") == "") | col("underlying_root").isNull() |
    col("yymmdd").isNull() | col("expiration_date").isNull() |
    col("strike_price").isNull()
).count()
print(f"[check] parse failures: {parse_fail}")

df_occ.select(
    "trade_date","contract_symbol","underlying_root","expiration_date",
    "option_is_call","strike_price","volume","option_close"
).show(15, truncate=False)

# Stage output
df_occ.write.mode("overwrite").parquet("stage/day_aggs_step2_occ.parquet")
print("[done] wrote stage/day_aggs_step2_occ.parquet")