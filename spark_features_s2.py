# step3_features.py
import os, datetime as dt, requests, pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, when, to_date, datediff, lit, round as sround

UNDERLYING  = os.getenv("UNDERLYING", "SPY")
POLY_KEY    = os.getenv("POLYGON_API_KEY")
IN_PARQUET  = "stage/day_aggs_step2_occ.parquet"
OUT_PARQUET = f"stage/features_{UNDERLYING.lower()}.parquet"

spark = SparkSession.builder.appName("OptionsFeatures-JoinUnderlying").getOrCreate()
df = spark.read.parquet(IN_PARQUET)

# Filter to one underlying to keep things fast
dfu = df.filter(col("underlying_root") == UNDERLYING)

# Check for data
print("rows for", UNDERLYING, "=", dfu.count())

# 2) Figure out the trade_date span we need
min_date = dfu.agg(F.min("trade_date")).first()[0]
max_date = dfu.agg(F.max("trade_date")).first()[0]
print("date range:", min_date, "to", max_date)

# Fetch daily closes for the underlying via Polygon REST
def fetch_underlying_closes(ticker, start_date, end_date, api_key):
    url = (f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
           f"{start_date}/{end_date}?adjusted=true&limit=50000&apiKey={api_key}")
    r = requests.get(url); r.raise_for_status()
    res = r.json().get("results", []) or []
    rows = []
    for it in res:
        day = dt.datetime.fromtimestamp(it["t"]/1000, tz=dt.timezone.utc).date().isoformat()
        rows.append({"trade_date": day, "underlying_close": float(it["c"])})
    return pd.DataFrame(rows)

pdf = fetch_underlying_closes(UNDERLYING, str(min_date), str(max_date), POLY_KEY)
print(pdf)
assert not pdf.empty, f"No closes returned for {UNDERLYING}."
sclose = spark.createDataFrame(pdf).withColumn("trade_date", to_date(col("trade_date")))

# Join & compute features
feat = (
    dfu.join(sclose, on="trade_date", how="left")
       .withColumn("days_to_expiry", datediff(col("expiration_date"), col("trade_date")))
       .withColumn("T_years", col("days_to_expiry")/lit(365.0))
       .withColumn("moneyness", sround((col("underlying_close")-col("strike_price"))/col("strike_price"), 6))
)

# Peek & write
feat.select(
    "trade_date","contract_symbol","expiration_date","option_is_call",
    "strike_price","underlying_close","T_years","moneyness","volume","option_close"
).show(15, truncate=False)

# Example
# +----------+--------------------+---------------+--------------+------------+----------------+---------------------+---------+------+------------+
# |trade_date|contract_symbol     |expiration_date|option_is_call|strike_price|underlying_close|T_years              |moneyness|volume|option_close|
# +----------+--------------------+---------------+--------------+------------+----------------+---------------------+---------+------+------------+
# |2025-04-03|O:SPY250404C00375000|2025-04-04     |1             |375.0       |536.7           |0.0027397260273972603|0.4312   |17    |136.34      |

# Interpretation:
# Deep ITM call (strike 375 vs 536 spot) = high close price (136.34).
# At-the-money call (strike ~536, not shown yet) should be around 0.50 delta and priced much lower than deep ITM.
# OTM calls (strike > 536, not shown yet) should have very low or near-zero prices.

feat.write.mode("overwrite").parquet(OUT_PARQUET)
print("[done] wrote", OUT_PARQUET)

# Attach underlying close ON EXPIRATION DATE
# reuse the same sclose (trade_date, underlying_close), just rename columns
sclose_exp = sclose.select(
    col("trade_date").alias("expiration_date"),
    col("underlying_close").alias("underlying_close_at_expiry")
)

feat2 = (
    feat.join(sclose_exp, on="expiration_date", how="left")
         # optional guards: keep realistic rows
         .filter(col("days_to_expiry") >= 0) # no negative TTE
         .filter(col("underlying_close").isNotNull()) # have trade-day S
         .filter(col("underlying_close_at_expiry").isNotNull()) # have expiry S
)


# Label: will this contract expire ITM? (1=yes, 0=no)
labelled = feat2.withColumn(
    "label_expire_itm",
    when((col("option_is_call")==1) & (col("underlying_close_at_expiry") > col("strike_price")), 1)
    .when((col("option_is_call")==0) & (col("underlying_close_at_expiry") < col("strike_price")), 1)
    .otherwise(0)
)

labelled = labelled.filter(
    (col("strike_price") > 0) &
    (col("underlying_close") > 0) &
    (col("underlying_close_at_expiry") > 0)
)

# Peek & write labelled dataset
labelled.select(
    "trade_date","contract_symbol","expiration_date","option_is_call",
    "strike_price","underlying_close","underlying_close_at_expiry",
    "T_years","moneyness","volume","option_close","label_expire_itm"
).show(15, truncate=False)

labelled.write.mode("overwrite").parquet(OUT_PARQUET.replace(".parquet", "_labelled.parquet"))
print("[done] wrote", OUT_PARQUET.replace(".parquet", "_labelled.parquet"))




############################# Check data
#############################

print(labelled.filter(labelled.days_to_expiry < 0).count())
# Calls that finished OTM (sanity that we have some 0s)
labelled.filter( (col("option_is_call")==1) & (col("underlying_close_at_expiry") <= col("strike_price")) ) \
        .select("trade_date","contract_symbol","strike_price","underlying_close_at_expiry","label_expire_itm") \
        .show(10, False)
# Puts that finished ITM (should be 1)
labelled.filter( (col("option_is_call")==0) & (col("underlying_close_at_expiry") < col("strike_price")) ) \
        .select("trade_date","contract_symbol","strike_price","underlying_close_at_expiry","label_expire_itm") \
        .show(10, False)

labelled.select("moneyness").summary().show()

labelled.select(
    "option_is_call","strike_price","underlying_close_at_expiry","label_expire_itm"
).show(20, False)

print(feat.count())
# Checking class weight stuff before training
# Load the labelled dataset (written at the end of step 2 script)
df = spark.read.parquet("stage/features_spy_labelled.parquet")
df.printSchema()  # should show label_expire_itm

# Class balance
df.groupBy("label_expire_itm").count().orderBy("label_expire_itm").show()

total = df.count()
pos = df.filter(F.col("label_expire_itm")==1).count()
print("total:", total, "pos:", pos, "neg:", total-pos, "pos_rate:", round(pos/total, 3))

# Hygiene (nulls)
checks = [
  "option_is_call","strike_price","underlying_close","underlying_close_at_expiry",
  "T_years","moneyness","volume","option_close","label_expire_itm"
]
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in checks]).show(truncate=False)

df.select("moneyness").summary().show()
# And quick view by label (should skew positive for label=1 calls, negative for label=1 puts overall)
df.groupBy("label_expire_itm").agg(
    F.expr("percentile_approx(moneyness, 0.5)").alias("median_moneyness"),
    F.avg("moneyness").alias("avg_moneyness")
).show()

##############################
##############################

# (Optional) write full dataset for PyTorch
labelled.coalesce(10).write.mode("overwrite").parquet("stage/pytorch_full.parquet")