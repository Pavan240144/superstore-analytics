# Databricks notebook source
# %pip install kaggle

# COMMAND ----------

# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Untitled
# Kaggle API Credentials
# TODO: Move to Databricks Secrets

import os
import json
from pathlib import Path

# Set credentials
KAGGLE_USERNAME = "pavankumarpathi"   
KAGGLE_KEY      = "KGAT_d24fb036529e89eed8226241212d2a5a"    

# Set environment variables for kaggle library
os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
os.environ["KAGGLE_KEY"]      = KAGGLE_KEY

# kaggle_dir = Path("/tmp/.kaggle")
# kaggle_dir.mkdir(parents=True, exist_ok=True)

# kaggle_json = kaggle_dir / "kaggle.json"
# kaggle_json.write_text(json.dumps({"username": KAGGLE_USERNAME,
#                                    "key"     : KAGGLE_KEY}))
# kaggle_json.chmod(0o600)

# os.environ["KAGGLE_USERNAME"]    = KAGGLE_USERNAME
# os.environ["KAGGLE_KEY"]         = KAGGLE_KEY
# os.environ["KAGGLE_CONFIG_DIR"]  = str(kaggle_dir)

# print(f"Kaggle Config Dir: {kaggle_dir}")
# print(f"Kaggle Config File: {kaggle_json}")

# # Verify file was created correctly
# with open(kaggle_json, "r") as f:
#     saved = json.load(f)

# print(f"Saved Config: {saved}")

# COMMAND ----------

# Create all required DBFS directories

folders = [
    "dbfs:/FileStore/superstore",
    "dbfs:/FileStore/superstore/raw",
    "dbfs:/FileStore/superstore/bronze",
    "dbfs:/FileStore/superstore/silver",
    "dbfs:/FileStore/superstore/gold",
    "dbfs:/FileStore/superstore/gold/sales_trend",
    "dbfs:/FileStore/superstore/gold/category_performance",
    "dbfs:/FileStore/superstore/gold/regional_performance",
    "dbfs:/FileStore/superstore/gold/loss_making_products",
    "dbfs:/FileStore/superstore/gold/discount_impact",
    "dbfs:/FileStore/superstore/gold/executive_kpis",
    "dbfs:/FileStore/superstore/gold/validation_report",
]

for folder in folders:
    dbutils.fs.mkdirs(folder)

print(folders)
print("All required DBFS directories created.")


# COMMAND ----------

# Download SuperStore dataset from Kaggle API

import os
import io
import zipfile
import requests
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

KAGGLE_DATASET   = "vivek468/superstore-dataset-final"
DBFS_RAW         = "dbfs:/FileStore/superstore/raw"
DBFS_RAW_PARQUET = f"{DBFS_RAW}/superstore_raw_parquet/"
DBFS_RAW_SPARK   = f"{DBFS_RAW}/superstore_raw_spark/"


api = KaggleApi()
api.authenticate()

dataset_owner   = KAGGLE_DATASET.split("/")[0]
dataset_name    = KAGGLE_DATASET.split("/")[1]

kaggle_url = (
    f"https://www.kaggle.com/api/v1/datasets/download/"
    f"{dataset_owner}/{dataset_name}"
)

kaggle_username = os.environ.get("KAGGLE_USERNAME", "")
kaggle_key      = os.environ.get("KAGGLE_KEY", "")

response = requests.get(
    kaggle_url,
    auth    = (kaggle_username, kaggle_key),
    stream  = True,
    timeout = 120,
)

if response.status_code != 200:
    raise ConnectionError(
        f"Kaggle download failed: "
        f"HTTP {response.status_code} — {response.text}"
    )

# Load ZIP into memory buffer
zip_buffer = io.BytesIO(response.content)

with zipfile.ZipFile(zip_buffer, "r") as zip_ref:
    # List contents
    all_files = zip_ref.namelist()

    csv_files = [
        f for f in all_files
        if f.endswith(".csv")
    ]

    if not csv_files:
        raise FileNotFoundError(
            f"No CSV found in ZIP. "
            f"Contents: {all_files}"
        )

    csv_filename = csv_files[0]
    print(f"CSV found    : {csv_filename}")

    # Read CSV directly from ZIP into memory
    with zip_ref.open(csv_filename) as csv_file:
        csv_bytes = csv_file.read()

print(f"Parsing CSV...")

df = None
for encoding in ["utf-8", "latin-1", "cp1252"]:
    try:
        df = pd.read_csv(
            io.BytesIO(csv_bytes),
            encoding = encoding
        )
        print(f"Parsed with encoding : {encoding}")
        break
    except (UnicodeDecodeError, Exception):
        continue

if df is None:
    raise ValueError(
        "Could not parse CSV with any encoding"
    )

# Clean column names
df.columns = df.columns.str.strip()

df_spark = spark.createDataFrame(df)

# Write CSV
(
    df_spark
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(DBFS_RAW_SPARK)
)
print(f"CSV     → {DBFS_RAW_SPARK}")

# Write Parquet 
(
    df_spark
    .write
    .mode("overwrite")
    .parquet(DBFS_RAW_PARQUET)
)
print(f"Parquet → {DBFS_RAW_PARQUET}")

# COMMAND ----------

# Verify downloaded file using Spark

# Read with Spark to verify
# df_raw = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv("dbfs:/FileStore/superstore/raw/superstore_raw.csv")

df_raw = spark.read.parquet(
    "dbfs:/FileStore/superstore/raw/superstore_raw_parquet/"
)

row_count = df_raw.count()
col_count = len(df_raw.columns)

print("Downloaded {row_count:,} rows and {col_count} columns")
df_raw.printSchema()

# Check mandatory columns
mandatory = ["Order ID", "Sales", "Profit", "Discount", "Region", "Category"]
missing   = [c for c in mandatory if c not in df_raw.columns]

if missing:
    print("Missing columns: {missing}")
else:
    print("All mandatory columns present")

# Preview data
display(df_raw.limit(5))

# COMMAND ----------

# Create Medallion Architecture Databases
# One database per layer: Bronze, Silver, Gold

databases = {
    "superstore_bronze" : " Raw data landed as-is",
    "superstore_silver" : " Cleaned and standardized data",
    "superstore_gold"   : " Business-ready aggregated tables",
}

for db_name, description in databases.items():
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
 

# Verify databases exist 
print("Verifying databases:")
databases_df = spark.sql("SHOW DATABASES")
display(databases_df)

print("ALL DATABASES CREATED")
