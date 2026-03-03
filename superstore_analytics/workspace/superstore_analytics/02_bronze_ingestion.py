# Databricks notebook source
# Imports & Configuration

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Path Config
DBFS_RAW            = "dbfs:/FileStore/superstore/raw"
BRONZE_DATABASE     = "superstore_bronze"
BRONZE_TABLE        = "orders_raw"
BRONZE_FULL_NAME    = f"{BRONZE_DATABASE}.{BRONZE_TABLE}"

# Source File Paths
YEARLY_PATHS = [
    f"{DBFS_RAW}/superstore_2019/",
    f"{DBFS_RAW}/superstore_2020/",
    f"{DBFS_RAW}/superstore_2021/",
    f"{DBFS_RAW}/superstore_2022/",
    f"{DBFS_RAW}/superstore_2023/",
]
ORDERS_PATH      = f"{DBFS_RAW}/superstore_orders/"
RETURNS_PATH     = f"{DBFS_RAW}/superstore_returns/"
SALESPERSON_PATH = f"{DBFS_RAW}/superstore_salesperson/"

# Mandatory Columns
MANDATORY_COLS = [
    "Order ID", "Order Date", "Sales",
    "Profit", "Discount", "Category", "Region"
]

# Batch Tracking 
BATCH_ID       = datetime.now().strftime("%Y%m%d_%H%M%S")
INGESTION_TIME = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


print(f"Target Table : {BRONZE_FULL_NAME}")
print(f"Batch ID     : {BATCH_ID}")
print(f"Ingested At  : {INGESTION_TIME}")


# COMMAND ----------

# Verify All Source Files Exist in DBFS

all_paths = YEARLY_PATHS + [
    ORDERS_PATH,
    RETURNS_PATH,
    SALESPERSON_PATH
]

all_exist = True

for path in all_paths:
    try:
        files = dbutils.fs.ls(path)
        data_files = [
            f for f in files
            if not f.name.startswith("_")
            and not f.name.startswith(".")
        ]
    except Exception as e:
        print("MISSING: {path}")
        all_exist = False

print()
if not all_exist:
    raise FileNotFoundError(
        "Some source files missing! "
    )

print("All source files verified")


# COMMAND ----------

# Load All 5 Yearly CSV Files
# Validate schema on each before combining

yearly_dfs  = []
total_rows  = 0

for path in YEARLY_PATHS:
    year = path.split("superstore_")[1].replace("/", "")

    # Load CSV
    df_year = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(path)

    row_count = df_year.count()

    # Schema Validation 
    missing_cols = [
        c for c in MANDATORY_COLS
        if c not in df_year.columns
    ]

    if missing_cols:
        raise ValueError(
            f"Year {year} missing mandatory "
            f"columns: {missing_cols}"
        )

    # Add Source Tracking
    df_year = df_year \
        .withColumn("Source_Year", F.lit(int(year))) \
        .withColumn("Source_File", F.lit(f"superstore_{year}"))

    yearly_dfs.append(df_year)
    total_rows += row_count

    print(f"{year} loaded")
   

# Combine All Years
df_all_orders = yearly_dfs[0]
for df in yearly_dfs[1:]:
    df_all_orders = df_all_orders.unionByName(
        df,
        allowMissingColumns=True
    )

combined_count = df_all_orders.count()

print(f"All yearly files combined")

# COMMAND ----------

# Load Returns & Salesperson Source Files

# Load Returns
df_returns = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(RETURNS_PATH)

# Add Is_Returned flag
df_returns = df_returns \
    .withColumn("Is_Returned", F.lit(True))

returns_count = df_returns.count()
print(f"Returns loaded  : {returns_count:,} rows")

# Load Salesperson
df_salesperson = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(SALESPERSON_PATH)

sales_count = df_salesperson.count()
print(f"Salesperson loaded : {sales_count} rows")

# Preview both
print("Returns preview:")
display(df_returns.limit(3))

print("Salesperson preview:")
display(df_salesperson)


# COMMAND ----------

# Join Orders + Returns + Salesperson
# Join Logic:
#   orders LEFT JOIN returns     ON Order ID
#   orders LEFT JOIN salesperson ON Region

initial_count = df_all_orders.count()

# Join 1: Orders LEFT JOIN Returns
# Deduplicate returns on Order ID first
df_returns_dedup = df_returns \
    .select("Order ID", "Return Reason", "Is_Returned") \
    .dropDuplicates(["Order ID"])

df_joined = df_all_orders.join(
    df_returns_dedup,
    on  = "Order ID",
    how = "left"
)

# Fill non-returned orders
df_joined = df_joined \
    .withColumn(
        "Is_Returned",
        F.when(
            F.col("Is_Returned").isNull(),
            False
        ).otherwise(F.col("Is_Returned"))
    ) \
    .withColumn(
        "Return Reason",
        F.when(
            F.col("Return Reason").isNull(),
            "Not Returned"
        ).otherwise(F.col("Return Reason"))
    )

returned_count = df_joined.filter(
    F.col("Is_Returned") == True
).count()

print(f"Join 1: Orders ⟕ Returns")

# Join 2: Orders LEFT JOIN Salesperson
df_salesperson_slim = df_salesperson.select(
    "Region",
    "Salesperson",
    "Target_Sales"
)

df_final = df_joined.join(
    df_salesperson_slim,
    on  = "Region",
    how = "left"
)

final_count = df_final.count()

# Check for unmatched regions
unmatched = df_final.filter(
    F.col("Salesperson").isNull()
).count()

print(f"Join 2: Orders ⟕ Salesperson")

# Verify no row inflation
# if final_count != initial_count:
#     print(f"Row count changed: "
#           f"{initial_count:,} → {final_count:,}")
# else:
#     print(f"\n Row count consistent: {final_count:,}")



# COMMAND ----------

# Add Bronze Layer Metadata Columns

df_bronze = df_final \
    .withColumn(
        "_ingestion_timestamp",
        F.lit(INGESTION_TIME).cast(TimestampType())
    ) \
    .withColumn(
        "_batch_id",
        F.lit(BATCH_ID)
    ) \
    .withColumn(
        "_source_layer",
        F.lit("BRONZE")
    ) \
    .withColumn(
        "_record_hash",
        F.md5(F.concat_ws(
            "|",
            F.col("Order ID"),
            F.col("Product ID"),
            F.col("Sales").cast("string"),
            F.col("Source_Year").cast("string")
        ))
    )

print(f"Metadata columns added:")
df_bronze.printSchema()


# COMMAND ----------

# DBTITLE 1,Cell 8
# Write to Bronze Delta Table
# superstore_bronze.orders_raw
# Partitioned by Source_Year for query efficiency


spark.sql(f"USE {BRONZE_DATABASE}")

# Write as Delta Table
(
    df_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .partitionBy("Source_Year")
    .saveAsTable(BRONZE_FULL_NAME)
)

print(f"Delta table written successfully")

# Verify Write 
df_verify = spark.read \
    .format("delta") \
    .table(BRONZE_FULL_NAME)

# verify_count = df_verify.count()

display(df_verify.limit(5))