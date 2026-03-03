# Databricks notebook source
# Imports & Configuration

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime

# Database Config 
BRONZE_DATABASE  = "superstore_bronze"
BRONZE_TABLE     = "orders_raw"
BRONZE_FULL_NAME = f"{BRONZE_DATABASE}.{BRONZE_TABLE}"

SILVER_DATABASE  = "superstore_silver"
SILVER_TABLE     = "orders_cleaned"
SILVER_FULL_NAME = f"{SILVER_DATABASE}.{SILVER_TABLE}"

# Data Quality Thresholds
MIN_RETENTION_PCT = 95.0   # Must retain >= 95% rows
MAX_NULL_PCT      = 5.0    # Max null % per column

# Tracking
SILVER_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# COMMAND ----------

# Read Bronze Delta Table

df_bronze = spark.read \
    .format("delta") \
    .table(BRONZE_FULL_NAME)

bronze_count = df_bronze.count()
df_bronze.printSchema()

# Store original count for retention check later
ORIGINAL_COUNT = bronze_count

# Preview
display(df_bronze.limit(5))

# COMMAND ----------

# Remove Duplicates

before_count = df_bronze.count()
df_deduped = df_bronze.dropDuplicates([
    "Order ID",
    "Product ID",
    "Source_Year"
])

after_count = df_deduped.count()
removed     = before_count - after_count
removed_pct = (removed / before_count * 100)

print(f"  Deduplication complete:")
print(f"  Before   : {before_count:,}")
print(f"  After    : {after_count:,}")
print(f"  Removed  : {removed:,}")
print(f"  Removed %: {removed_pct:.2f}%")


# COMMAND ----------

# Handle Null Values
# Strategy:
#   Drop   → rows where MANDATORY columns are null
#   Fill   → non-critical nulls with defaults

before_null = df_deduped.count()

print("Null counts before cleaning:")
for col_name in df_deduped.columns:
    # Skip metadata columns
    if col_name.startswith("_"):
        continue
    null_count = df_deduped.filter(
        F.col(f"`{col_name}`").isNull()
    ).count()
    if null_count > 0:
        print(f" {col_name:<25} : {null_count:,} nulls")

# Drop Mandatory Null Rows
mandatory_cols = [
    "Order ID", "Order Date", "Sales",
    "Profit", "Discount", "Category", "Region"
]

df_no_nulls = df_deduped.dropna(subset=mandatory_cols)

dropped = before_null - df_no_nulls.count()
print(f"Mandatory null rows dropped: {dropped:,}")

# Fill Non-Critical Nulls
df_filled = df_no_nulls \
    .fillna({
        "Postal Code"   : "00000",
        "Ship Mode"     : "Standard Class",
        "Customer Name" : "Unknown Customer",
        "Salesperson"   : "Unassigned",
        "Return Reason" : "Not Returned",
        "Target_Sales"  : 0,
    })

# Verify No Nulls Remain in Mandatory 
print("Null counts after cleaning:")
for col_name in mandatory_cols:
    null_count = df_filled.filter(
        F.col(f"`{col_name}`").isNull()
    ).count()
    # status = "✅" if null_count == 0 else "❌"
    # print(f"  {status} {col_name:<25} : {null_count:,} nulls")

print(f" Null handling complete")
print(f" Rows retained: {df_filled.count():,}")


# COMMAND ----------

# Standardize Text Fields
# Fix: Inconsistent casing injected in Notebook 01
# Example: "EAST" → "East", "west" → "West"

# Columns to standardize to Title Case
text_cols = [
    "Region",
    "Category",
    "Sub-Category",
    "Segment",
    "Ship Mode",
    "State",
    "City",
    "Country"
]

df_text = df_filled

for col_name in text_cols:
    if col_name in df_text.columns:
        # Check distinct values before
        before_unique = df_text.select(
            f"`{col_name}`"
        ).distinct().count()

        # Apply Title Case + trim whitespace
        df_text = df_text.withColumn(
            col_name,
            F.initcap(F.trim(F.col(f"`{col_name}`")))
        )

        # Check distinct values after
        after_unique = df_text.select(
            f"`{col_name}`"
        ).distinct().count()

        # print(
        #     f"  ✅ {col_name:<20} : "
        #     f"{before_unique} → {after_unique} unique values"
        # )

# Verify Region values are now consistent

print(" Region values after standardization:")
regions = [
    row["Region"]
    for row in df_text
    .select("Region")
    .distinct()
    .collect()
]
print(f"{sorted(regions)}")


# COMMAND ----------

# Parse Dates & Cast Numeric Types

df_typed = df_text

# Parse Date Columns 
date_formats = ["M/d/yyyy", "MM/dd/yyyy", "yyyy-MM-dd"]

for date_col in ["Order Date", "Ship Date"]:
    if date_col in df_typed.columns:

        # Try parsing with primary format
        parsed = F.to_date(
            F.col(f"`{date_col}`"),
            "M/d/yyyy"
        )

        df_typed = df_typed.withColumn(
            date_col,
            parsed
        )

        # Count unparseable dates
        bad_dates = df_typed.filter(
            F.col(f"`{date_col}`").isNull()
        ).count()

        print(f" '{date_col}' parsed")
        if bad_dates > 0:
            print(f"  {bad_dates} unparseable dates")

# Cast Numeric Columns 
numeric_casts = {
    "Sales"    : DoubleType(),
    "Profit"   : DoubleType(),
    "Discount" : DoubleType(),
    "Quantity" : IntegerType(),
    "Row ID"   : IntegerType(),
}

for col_name, dtype in numeric_casts.items():
    if col_name in df_typed.columns:
        df_typed = df_typed.withColumn(
            col_name,
            F.col(f"`{col_name}`").cast(dtype)
        )
        print(f" '{col_name}' cast to {dtype}")

# Cap Invalid Discount Values
# Discount > 1 means > 100% — data error from injection
bad_disc = df_typed.filter(
    F.col("Discount") > 1.0
).count()

df_typed = df_typed.withColumn(
    "Discount",
    F.least(
        F.greatest(F.col("Discount"), F.lit(0.0)),
        F.lit(1.0)
    )
)

print(f"Invalid discounts capped: {bad_disc:,} values")

# Remove Invalid Sales
invalid_sales = df_typed.filter(F.col("Sales") <= 0).count()
df_typed      = df_typed.filter(F.col("Sales") > 0)

print(f"Invalid sales removed  : {invalid_sales:,} rows")
print(f"Type casting complete")
print(f"  Rows retained: {df_typed.count():,}")


# COMMAND ----------

# 7 Derived Columns
# Derived Columns:
#   1. Profit_Margin_Pct  = (Profit / Sales) × 100
#   2. Discount_Amount    = Sales × Discount
#   3. Is_Loss            = True where Profit < 0
#   4. Order_Year         = Year from Order Date
#   5. Order_Month        = Month from Order Date
#   6. Order_Quarter      = Quarter from Order Date
#   7. Shipping_Days      = Ship Date - Order Date

df_silver = df_typed \
    \
    .withColumn(
        "Profit_Margin_Pct",
        F.when(
            F.col("Sales") != 0,
            F.round(
                (F.col("Profit") / F.col("Sales")) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    ) \
    \
    .withColumn(
        "Discount_Amount",
        F.round(F.col("Sales") * F.col("Discount"), 2)
    ) \
    \
    .withColumn(
        "Is_Loss",
        F.col("Profit") < 0
    ) \
    \
    .withColumn(
        "Order_Year",
        F.year(F.col("`Order Date`"))
    ) \
    \
    .withColumn(
        "Order_Month",
        F.month(F.col("`Order Date`"))
    ) \
    \
    .withColumn(
        "Order_Quarter",
        F.quarter(F.col("`Order Date`"))
    ) \
    \
    .withColumn(
        "Shipping_Days",
        F.greatest(
            F.datediff(
                F.col("`Ship Date`"),
                F.col("`Order Date`")
            ),
            F.lit(0)
        )
    ) \
    \
    .withColumn(
        "_silver_timestamp",
        F.lit(SILVER_TIMESTAMP).cast(TimestampType())
    ) \
    .withColumn(
        "_source_layer",
        F.lit("SILVER")
    )

# Derived Column Stats 
loss_count   = df_silver.filter(F.col("Is_Loss") == True).count()
avg_margin   = df_silver.agg(
    F.round(F.avg("Profit_Margin_Pct"), 2)
).collect()[0][0]
avg_shipping = df_silver.agg(
    F.round(F.avg("Shipping_Days"), 1)
).collect()[0][0]

print(f"   1. Profit_Margin_Pct  → Avg: {avg_margin}%")
print(f"   2. Discount_Amount    → engineered")
print(f"   3. Is_Loss            → {loss_count:,} loss records")
print(f"   4. Order_Year         → engineered")
print(f"   5. Order_Month        → engineered")
print(f"   6. Order_Quarter      → engineered")
print(f"   7. Shipping_Days      → Avg: {avg_shipping} days")
print()
print(f"  Total columns: {len(df_silver.columns)}")


# COMMAND ----------

# Validate Row Retention Rate
# Must retain >= 95% of Bronze rows

silver_count   = df_silver.count()
retention_pct  = (silver_count / ORIGINAL_COUNT) * 100
dropped_rows   = ORIGINAL_COUNT - silver_count

print(f"  Bronze rows  : {ORIGINAL_COUNT:,}")
print(f"  Silver rows  : {silver_count:,}")
print(f"  Dropped rows : {dropped_rows:,}")
print(f"  Retention    : {retention_pct:.2f}%")
print()

if retention_pct < MIN_RETENTION_PCT:
    raise ValueError(
        f"Retention {retention_pct:.2f}% below "
        f"threshold {MIN_RETENTION_PCT}%"
    )

print(f"  Retention rate acceptable: {retention_pct:.2f}%")


# COMMAND ----------

# Write to Silver Delta Table
# superstore_silver.orders_cleaned
# Partitioned by Order_Year

spark.sql(f"USE {SILVER_DATABASE}")

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .partitionBy("Order_Year")
    .saveAsTable(SILVER_FULL_NAME)
)

print(f" Delta table written successfully")
print(f" Table : {SILVER_FULL_NAME}")

# Verify Write 
df_verify = spark.read \
    .format("delta") \
    .table(SILVER_FULL_NAME)

# verify_count = df_verify.count()

display(df_verify.limit(5))


# COMMAND ----------

# Silver Table Analysis


# Year Distribution 
print(" Year Distribution:")
display(
    spark.sql(f"""
        SELECT
            Order_Year,
            COUNT(*)                    AS Total_Rows,
            COUNT(DISTINCT `Order ID`)  AS Unique_Orders,
            ROUND(SUM(Sales), 2)        AS Total_Sales,
            ROUND(SUM(Profit), 2)       AS Total_Profit,
            ROUND(AVG(Profit_Margin_Pct), 2)
                                        AS Avg_Margin_Pct,
            SUM(CASE WHEN Is_Loss = true
                THEN 1 ELSE 0 END)      AS Loss_Count
        FROM {SILVER_FULL_NAME}
        GROUP BY Order_Year
        ORDER BY Order_Year
    """)
)

# Category Distribution 
print("Category Distribution:")
display(
    spark.sql(f"""
        SELECT
            Category,
            COUNT(*)                    AS Total_Rows,
            ROUND(SUM(Sales), 2)        AS Total_Sales,
            ROUND(SUM(Profit), 2)       AS Total_Profit,
            ROUND(AVG(Profit_Margin_Pct), 2)
                                        AS Avg_Margin_Pct
        FROM {SILVER_FULL_NAME}
        GROUP BY Category
        ORDER BY Total_Sales DESC
    """)
)

# Region Distribution 
print("Region Distribution:")
display(
    spark.sql(f"""
        SELECT
            Region,
            COUNT(*)                    AS Total_Rows,
            ROUND(SUM(Sales), 2)        AS Total_Sales,
            ROUND(SUM(Profit), 2)       AS Total_Profit
        FROM {SILVER_FULL_NAME}
        GROUP BY Region
        ORDER BY Total_Sales DESC
    """)
)

# Derived Column Sanity Check 
print("Derived Column Sanity Check:")
derived_stats = spark.sql(f"""
    SELECT
        MIN(Profit_Margin_Pct)  AS Min_Margin,
        MAX(Profit_Margin_Pct)  AS Max_Margin,
        AVG(Profit_Margin_Pct)  AS Avg_Margin,
        MIN(Shipping_Days)      AS Min_Ship_Days,
        MAX(Shipping_Days)      AS Max_Ship_Days,
        AVG(Shipping_Days)      AS Avg_Ship_Days,
        SUM(CASE WHEN Is_Loss = true
            THEN 1 ELSE 0 END)  AS Total_Loss_Records,
        MIN(Order_Year)         AS Min_Year,
        MAX(Order_Year)         AS Max_Year
    FROM {SILVER_FULL_NAME}
""")
display(derived_stats)
