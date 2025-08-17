import os
import boto3
from botocore.client import Config

ENDPOINT = "https://files.polygon.io"
BUCKET   = "flatfiles"
PREFIX   = "us_options_opra/day_aggs_v1/"

# Day aggregates (options) live under this prefix
ROOT_PREFIX = "us_options_opra/day_aggs_v1/"

ACCESS_KEY = os.getenv("POLY_S3_KEY")
SECRET_KEY = os.getenv("POLY_S3_SECRET")

OUTDIR = "data/day_aggs"
os.makedirs(OUTDIR, exist_ok=True)

# Initialize client
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    config=Config(signature_version="s3v4"),
)

def download_files(objs):
    for obj in objs:
        key = obj["Key"] # e.g. us_options_opra/day_aggs_v1/2025/08/15.csv.gz
        fname = os.path.basename(key)
        parts = key.split("/")
        year, month = parts[-3], parts[-2] # '2025', '08'
        day = fname.replace(".csv.gz", "") # '15' or '2025-08-15'

        # Normalize to YYYY-MM-DD.csv.gz
        if len(day) == 2: # '15' -> '2025-08-15'
            local_name = f"{year}-{month}-{day}.csv.gz"
        else: # already '2025-08-15'
            local_name = f"{day}.csv.gz"

        local_path = os.path.join(OUTDIR, local_name)

        if os.path.exists(local_path):
            print(f"[keep] {local_path}")
            continue

        print(f"[get ] {key} -> {local_path}")
        s3.download_file(BUCKET, key, local_path)

def get_dates(prefix, do_download=True):
    token = None
    count = 0
    while True:
        kwargs = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        contents = resp.get("Contents", [])

        # Print keys
        for obj in contents:
            print(obj["Key"])
        count += len(contents)

        # Download if requested
        if do_download and contents:
            download_files(contents)

        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")

    print(f"[done] total files found: {count}")

if __name__=="__main__":

    for i in range(4,9):
        print("Month", i)
        YEAR = "2025"
        MONTH = str(i).zfill(2)
        prefix = f"{ROOT_PREFIX}{YEAR}/{MONTH}/"  # e.g. us_options_opra/day_aggs_v1/2025/08/
        print(f"[info] listing: s3://{BUCKET}/{prefix}")
        # Set to True to download
        get_dates(prefix, False)