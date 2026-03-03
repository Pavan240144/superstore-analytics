# Databricks notebook source
# Imports & Configuration

import pandas as pd
import numpy as np
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F

# Path Config
DBFS_RAW    = "dbfs:/FileStore/superstore/raw"
RAW_PARQUET = f"{DBFS_RAW}/superstore_raw_parquet/"

# Generation Config
YEARS        = [2019, 2020, 2021, 2022, 2023]
RANDOM_SEED  = 42
DATE_FORMAT  = "%m/%d/%Y"

# Quality Injection Config 
INJECT_NULL_PCT      = 0.02   # 2% nulls
INJECT_DUPLICATE_PCT = 0.01   # 1% duplicates

np.random.seed(RANDOM_SEED)


# COMMAND ----------

# Load Raw SuperStore Data from DBFS Parquet

# Read Parquet from DBFS into Spark
df_spark = spark.read.parquet(RAW_PARQUET)

# Convert to Pandas for generation logic
df_raw = df_spark.toPandas()

# Preview raw data
display(df_spark.limit(5))

# COMMAND ----------

# Split Raw Data into 3 Source Files

# File 1: Orders
order_cols = [
    "Row ID", "Order ID", "Order Date", "Ship Date",
    "Ship Mode", "Customer ID", "Customer Name",
    "Segment", "Country", "City", "State",
    "Postal Code", "Region", "Product ID",
    "Category", "Sub-Category", "Product Name",
    "Sales", "Quantity", "Discount", "Profit"
]

# Keeping only columns that exist in raw data
order_cols = [c for c in order_cols if c in df_raw.columns]
df_orders  = df_raw[order_cols].copy()

(
    spark.createDataFrame(df_orders)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{DBFS_RAW}/superstore_orders/")
)

# File 2: Returns
# Simulate 10% of orders being returned
return_sample = df_orders.sample(
    frac        = 0.10,
    random_state= RANDOM_SEED
)

df_returns = pd.DataFrame({
    "Order ID"      : return_sample["Order ID"].values,
    "Return Reason" : np.random.choice(
        [
            "Wrong Item",
            "Defective Product",
            "Changed Mind",
            "Better Price Found",
            "Damaged in Shipping"
        ],
        size=len(return_sample)
    )
})

(
    spark.createDataFrame(df_returns)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{DBFS_RAW}/superstore_returns/")
)

# File 3: Salesperson
# Region to Salesperson mapping
df_salesperson = pd.DataFrame({
    "Region"       : ["East", "West", "Central", "South"],
    "Salesperson"  : [
        "Alex Johnson",
        "Maria Chen",
        "Robert Smith",
        "Emily Davis"
    ],
    "Target_Sales" : [250000, 300000, 200000, 180000],
    "Email"        : [
        "alex.johnson@superstore.com",
        "maria.chen@superstore.com",
        "robert.smith@superstore.com",
        "emily.davis@superstore.com"
    ]
})

(
    spark.createDataFrame(df_salesperson)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{DBFS_RAW}/superstore_salesperson/")
)

print("ALL 3 SOURCE FILES CREATED")


# COMMAND ----------

# Helper Functions for Yearly Data Generation

def shift_dates_to_year(df: pd.DataFrame,
                        target_year: int) -> pd.DataFrame:
    """
    Shift Order Date and Ship Date to target year
    preserving original month/day patterns.
    
    Args:
        df          : Orders DataFrame
        target_year : Year to shift dates to
    Returns:
        DataFrame with shifted dates
    """
    df = df.copy()

    # Parse dates
    df["Order Date"] = pd.to_datetime(
        df["Order Date"],
        format  = DATE_FORMAT,
        errors  = "coerce"
    )
    df["Ship Date"] = pd.to_datetime(
        df["Ship Date"],
        format  = DATE_FORMAT,
        errors  = "coerce"
    )

    # Calculate year difference
    original_year = df["Order Date"].dt.year.mode()[0]
    year_diff     = target_year - original_year

    # Shift both dates
    df["Order Date"] = df["Order Date"].apply(
        lambda d: d + relativedelta(years=year_diff)
        if pd.notnull(d) else d
    )
    df["Ship Date"] = df["Ship Date"].apply(
        lambda d: d + relativedelta(years=year_diff)
        if pd.notnull(d) else d
    )

    # Convert back to string format
    df["Order Date"] = df["Order Date"].dt.strftime(DATE_FORMAT)
    df["Ship Date"]  = df["Ship Date"].dt.strftime(DATE_FORMAT)

    return df


def apply_yearly_variation(df: pd.DataFrame,
                            year: int) -> pd.DataFrame:
    """
    Apply realistic year-over-year business growth
    and decline patterns to Sales, Profit, Quantity.

    Args:
        df   : Orders DataFrame
        year : Target year
    Returns:
        DataFrame with adjusted metrics
    """
    # Real-world inspired multipliers
    multipliers = {
        2019: {"sales": 0.82, "profit": 0.78, "qty": 0.85},
        2020: {"sales": 0.91, "profit": 0.75, "qty": 0.88},  # COVID
        2021: {"sales": 1.05, "profit": 0.95, "qty": 1.02},  # Recovery
        2022: {"sales": 1.12, "profit": 1.08, "qty": 1.10},  # Growth
        2023: {"sales": 1.20, "profit": 1.15, "qty": 1.18},  # Peak
    }

    m  = multipliers.get(
        year,
        {"sales": 1.0, "profit": 1.0, "qty": 1.0}
    )
    df = df.copy()

    # Add ±5% random noise for realism
    noise = lambda n: np.random.uniform(0.95, 1.05, size=n)

    df["Sales"]    = (
        df["Sales"].astype(float) * m["sales"] * noise(len(df))
    ).round(2)

    df["Profit"]   = (
        df["Profit"].astype(float) * m["profit"] * noise(len(df))
    ).round(2)

    df["Quantity"] = (
        df["Quantity"].astype(float) * m["qty"] * noise(len(df))
    ).round(0).astype(int)

    # Unique Row IDs per year
    df["Row ID"] = range(
        (year - 2018) * 10000 + 1,
        (year - 2018) * 10000 + len(df) + 1
    )

    return df


def inject_quality_issues(df: pd.DataFrame,
                           year: int) -> pd.DataFrame:
    """
    Inject realistic data quality issues to make
    the Silver cleaning notebook meaningful.

    Issues injected:
      - Nulls in non-critical columns
      - Duplicate rows (~1%)
      - Inconsistent text casing in Region
      - Invalid Discount values (>1)

    Args:
        df   : Orders DataFrame
        year : Used for reproducible randomness
    Returns:
        DataFrame with injected quality issues
    """
    df = df.copy()
    n  = len(df)

    # ── Inject Nulls ──────────────────────────────────
    null_cols = ["Postal Code", "Ship Mode", "Customer Name"]
    for col in null_cols:
        if col in df.columns:
            null_idx = df.sample(
                frac         = INJECT_NULL_PCT,
                random_state = year
            ).index
            df.loc[null_idx, col] = np.nan

    # ── Inject Duplicates ─────────────────────────────
    n_dupes   = max(1, int(n * INJECT_DUPLICATE_PCT))
    dupe_rows = df.sample(n=n_dupes, random_state=year)
    df        = pd.concat([df, dupe_rows], ignore_index=True)

    # ── Inconsistent Casing in Region ─────────────────
    if "Region" in df.columns:
        casing_idx = df.sample(
            frac         = 0.03,
            random_state = year + 1
        ).index
        df.loc[casing_idx, "Region"] = \
            df.loc[casing_idx, "Region"].str.upper()

    # ── Invalid Discount Values (>1 = >100%) ──────────
    if "Discount" in df.columns:
        bad_idx = df.sample(
            frac         = 0.01,
            random_state = year + 2
        ).index
        df.loc[bad_idx, "Discount"] = \
            df.loc[bad_idx, "Discount"] * 10

    return df


# COMMAND ----------

# Generate 5 Yearly Files (2019 — 2023)
# Each file = date shifted + varied + quality issues

yearly_summary = {}

for year in YEARS:

    # Step 1 — Shift dates to target year
    df_year = shift_dates_to_year(df_orders, year)

    # Step 2 — Apply business growth/decline
    df_year = apply_yearly_variation(df_year, year)

    # Step 3 — Inject quality issues
    df_year = inject_quality_issues(df_year, year)

    # Step 4 — Add source tracking columns
    df_year["Source_Year"] = year
    df_year["Source_File"] = f"superstore_{year}"

    # Step 5 — Write to DBFS via Spark
    (
        spark.createDataFrame(df_year)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(f"{DBFS_RAW}/superstore_{year}/")
    )

    # Track summary
    yearly_summary[year] = {
        "rows"  : len(df_year),
        "path"  : f"{DBFS_RAW}/superstore_{year}/",
    }

    print("Saved to DBFS")


# Overall summary
total_rows = sum(v["rows"] for v in yearly_summary.values())
print(f"ALL 5 YEARLY FILES GENERATED")
print(f"Total rows  : {total_rows:,}")

# COMMAND ----------

# Verify All Files in DBFS Raw Layer

all_items           = dbutils.fs.ls(DBFS_RAW)
total_rows_verified = 0

print(f"{DBFS_RAW}/")

for item in sorted(all_items, key=lambda x: x.name):
    try:
        df_check  = spark.read \
            .option("header", True) \
            .csv(item.path)
        row_count = df_check.count()
        total_rows_verified += row_count
        print(
            f"  ✅ {item.name:<40} {row_count:>7,} rows"
        )
    except Exception:
        print(f"  📁 {item.name:<40} (parquet folder)")

print(f"Total rows: {total_rows_verified:,}")
print("=" * 55)

# COMMAND ----------

# Verify Quality Issues Were Injected Correctly

# Load 2023 file as sample check
df_check = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{DBFS_RAW}/superstore_2023/")

total = df_check.count()

# Check nulls
null_ship  = df_check.filter(F.col("Ship Mode").isNull()).count()
null_post  = df_check.filter(F.col("Postal Code").isNull()).count()
null_cust  = df_check.filter(F.col("Customer Name").isNull()).count()

# Check duplicates
unique_rows = df_check.dropDuplicates(
    ["Order ID", "Product ID"]
).count()
dupes = total - unique_rows

# Check casing issues
# regions = df_check.select("Region") \
#     .distinct() \
#     .rdd.flatMap(lambda x: x) \
#     .collect()
regions = [
    row["Region"]
    for row in df_check
    .select("Region")
    .distinct()
    .collect()
]

# Check invalid discounts
bad_disc = df_check.filter(
    F.col("Discount").cast("double") > 1.0
).count()

print(f"  Sample File : superstore_2023/")
print(f"   Total rows          : {total:,}")
print(f"   Null Ship Mode      : {null_ship:,}")
print(f"   Null Postal Code    : {null_post:,}")
print(f"   Null Customer Name  : {null_cust:,}")
print(f"   Duplicate rows      : {dupes:,}")
print(f"   Region values       : {sorted(regions)}")
print(f"   Invalid discounts   : {bad_disc:,}")
print()
print("   Quality issues confirmed injected")
print("=" * 55)