# Databricks notebook source
# Imports & Configuration

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
from datetime import datetime
import os

# Database Config 
SILVER_DATABASE  = "superstore_silver"
SILVER_TABLE     = "orders_cleaned"
SILVER_FULL_NAME = f"{SILVER_DATABASE}.{SILVER_TABLE}"

GOLD_DATABASE    = "superstore_gold"

# Gold Table Names
GOLD_TABLES = {
    "sales_trend"           : f"{GOLD_DATABASE}.sales_trend",
    "category_performance"  : f"{GOLD_DATABASE}.category_performance",
    "regional_performance"  : f"{GOLD_DATABASE}.regional_performance",
    "loss_making_products"  : f"{GOLD_DATABASE}.loss_making_products",
    "discount_impact"       : f"{GOLD_DATABASE}.discount_impact",
    "executive_kpis"        : f"{GOLD_DATABASE}.executive_kpis",
}

# CSV Export Path (for Power BI)
CSV_EXPORT_BASE = "dbfs:/FileStore/superstore/gold_csv"

# Tracking 
GOLD_TIMESTAMP = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# COMMAND ----------

# Read Silver Delta Table
# Cache it — we read it 6 times for Gold tables

# Read Silver Delta Table
df_silver = spark.read \
    .format("delta") \
    .table(SILVER_FULL_NAME)

# Cache Silver — used across all 6 Gold tables
df_silver.cache()
silver_count = df_silver.count()   # Trigger cache

print(f"Silver table loaded & cached:")

# Use Gold database
spark.sql(f"USE {GOLD_DATABASE}")

# Create CSV export folder
dbutils.fs.mkdirs(CSV_EXPORT_BASE)
print(f" CSV export folder ready: {CSV_EXPORT_BASE}")


# COMMAND ----------

# Helper Function — Write Gold Delta Table + Export CSV
# Used by all 6 Gold table cells

def write_gold_table(
    df          : object,
    table_key   : str,
    partition_col: str = None
) -> int:
    """
    Write a Gold Delta Table and export as CSV for Power BI.

    Args:
        df            : Spark DataFrame
        table_key     : Key from GOLD_TABLES dict
        partition_col : Optional partition column

    Returns:
        int: Row count written
    """
    table_name = GOLD_TABLES[table_key]

    # Add Gold Metadata
    df_gold = df \
        .withColumn("_gold_table",   F.lit(table_key)) \
        .withColumn("_created_at",
                    F.lit(GOLD_TIMESTAMP).cast(TimestampType()))

    # Write Delta Table 
    writer = df_gold.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option("delta.columnMapping.mode", "name")

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.saveAsTable(table_name)

    # Export as CSV for Power BI
    csv_path = f"{CSV_EXPORT_BASE}/{table_key}/"
    (
        df_gold
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(csv_path)
    )

    # Verify 
    row_count = spark.read \
        .format("delta") \
        .table(table_name) \
        .count()

    return row_count


# COMMAND ----------

# GOLD TABLE 1 — Sales Trend
# Monthly & yearly sales and profit trend analysis

df_sales_trend = df_silver \
    .groupBy("Order_Year", "Order_Month", "Order_Quarter") \
    .agg(
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.sum("Profit"),  2) .alias("Total_Profit"),
        F.countDistinct("`Order ID`") .alias("Total_Orders"),
        F.sum("Quantity")            .alias("Total_Quantity"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount"),
        F.round(F.avg("Profit_Margin_Pct"), 2)
                                     .alias("Avg_Margin_Pct"),
        F.sum(F.col("Is_Loss").cast(IntegerType()))
                                     .alias("Loss_Count")
    ) \
    .withColumn(
        "Profit_Margin_Pct",
        F.when(
            F.col("Total_Sales") != 0,
            F.round(
                (F.col("Total_Profit") / F.col("Total_Sales"))
                * 100, 2
            )
        ).otherwise(F.lit(0.0))
    ) \
    .orderBy("Order_Year", "Order_Month")

# Preview
display(df_sales_trend)

# Write Gold Table + CSV
write_gold_table(
    df          = df_sales_trend,
    table_key   = "sales_trend",
    partition_col = "Order_Year"
)


# COMMAND ----------

# GOLD TABLE 2 — Category Performance
# Sales & profit by Category and Sub-Category


df_category = df_silver \
    .groupBy("Category", "`Sub-Category`") \
    .agg(
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.sum("Profit"),  2) .alias("Total_Profit"),
        F.countDistinct("`Order ID`") .alias("Total_Orders"),
        F.sum("Quantity")            .alias("Total_Quantity"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount"),
        F.sum(F.col("Is_Loss").cast(IntegerType()))
                                     .alias("Loss_Count"),
        F.round(F.avg("Profit_Margin_Pct"), 2)
                                     .alias("Avg_Margin_Pct")
    ) \
    .withColumn(
        "Profit_Margin_Pct",
        F.round(
            (F.col("Total_Profit") / F.col("Total_Sales"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Loss_Rate_Pct",
        F.round(
            (F.col("Loss_Count") / F.col("Total_Orders"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Sales_Rank",
        F.rank().over(
            Window
            .partitionBy("Category")
            .orderBy(F.desc("Total_Sales"))
        )
    ) \
    .orderBy(F.desc("Total_Sales"))

# Preview
display(df_category)

# Write Gold Table + CSV
write_gold_table(
    df        = df_category,
    table_key = "category_performance"
)


# COMMAND ----------

# GOLD TABLE 3 — Regional Performance
# Sales & profit by Region and State

df_regional = df_silver \
    .groupBy("Region", "State", "Salesperson", "Target_Sales") \
    .agg(
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.sum("Profit"),  2) .alias("Total_Profit"),
        F.countDistinct("`Order ID`") .alias("Total_Orders"),
        F.sum("Quantity")            .alias("Total_Quantity"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount"),
        F.sum(F.col("Is_Loss").cast(IntegerType()))
                                     .alias("Loss_Count"),
        F.round(F.avg("Shipping_Days"), 1)
                                     .alias("Avg_Shipping_Days")
    ) \
    .withColumn(
        "Profit_Margin_Pct",
        F.round(
            (F.col("Total_Profit") / F.col("Total_Sales"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Sales_vs_Target_Pct",
        F.when(
            F.col("Target_Sales") > 0,
            F.round(
                (F.col("Total_Sales") / F.col("Target_Sales"))
                * 100, 2
            )
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn(
        "Loss_Rate_Pct",
        F.round(
            (F.col("Loss_Count") / F.col("Total_Orders"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Region_Sales_Rank",
        F.rank().over(
            Window
            .partitionBy("Region")
            .orderBy(F.desc("Total_Sales"))
        )
    ) \
    .orderBy(F.desc("Total_Sales"))

# Preview
display(df_regional)

# Write Gold Table + CSV
write_gold_table(
    df          = df_regional,
    table_key   = "regional_performance",
    partition_col = "Region"
)


# COMMAND ----------

# GOLD TABLE 4 — Loss Making Products
# Products and sub-categories with negative profit

# Filter only loss-making transactions
df_loss = df_silver.filter(F.col("Is_Loss") == True)

df_loss_products = df_loss \
    .groupBy(
        "Category",
        "`Sub-Category`",
        "`Product Name`"
    ) \
    .agg(
        F.round(F.sum("Profit"),  2) .alias("Total_Loss"),
        F.countDistinct("`Order ID`") .alias("Loss_Transactions"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount"),
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.avg("Profit_Margin_Pct"), 2)
                                     .alias("Avg_Loss_Margin"),
        F.round(F.avg("Shipping_Days"), 1)
                                     .alias("Avg_Shipping_Days")
    ) \
    .withColumn(
        "Loss_Per_Transaction",
        F.round(
            F.col("Total_Loss") / F.col("Loss_Transactions"),
            2
        )
    ) \
    .withColumn(
        "Loss_Rank",
        F.rank().over(
            Window.orderBy("Total_Loss")
        )
    ) \
    .orderBy("Total_Loss")   # Most negative = worst

total_loss_amount = df_loss \
    .agg(F.round(F.sum("Profit"), 2)) \
    .collect()[0][0]

# Preview top 20 loss products
display(df_loss_products.limit(20))

print(f"Total Loss Amount : ${total_loss_amount:,.2f}")
print(f"Loss Products     : {df_loss_products.count():,}")

# Write Gold Table + CSV
write_gold_table(
    df        = df_loss_products,
    table_key = "loss_making_products"
)


# COMMAND ----------

# GOLD TABLE 5 — Discount Impact Analysis
# How discount ranges affect profitability

df_discount = df_silver \
    .withColumn(
        "Discount_Bucket",
        F.when(F.col("Discount") == 0,    F.lit("1. No Discount"))
         .when(F.col("Discount") <= 0.10, F.lit("2. 1-10%"))
         .when(F.col("Discount") <= 0.20, F.lit("3. 11-20%"))
         .when(F.col("Discount") <= 0.30, F.lit("4. 21-30%"))
         .when(F.col("Discount") <= 0.40, F.lit("5. 31-40%"))
         .when(F.col("Discount") <= 0.50, F.lit("6. 41-50%"))
         .otherwise(                      F.lit("7. 51%+"))
    ) \
    .groupBy("Discount_Bucket") \
    .agg(
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.sum("Profit"),  2) .alias("Total_Profit"),
        F.countDistinct("`Order ID`") .alias("Total_Orders"),
        F.round(F.avg("Sales"),   2) .alias("Avg_Order_Sales"),
        F.round(F.avg("Profit"),  2) .alias("Avg_Order_Profit"),
        F.sum(F.col("Is_Loss").cast(IntegerType()))
                                     .alias("Loss_Transactions"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount")
    ) \
    .withColumn(
        "Profit_Margin_Pct",
        F.round(
            (F.col("Total_Profit") / F.col("Total_Sales"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Loss_Rate_Pct",
        F.round(
            (F.col("Loss_Transactions") / F.col("Total_Orders"))
            * 100, 2
        )
    ) \
    .orderBy("Discount_Bucket")

# Preview all discount buckets
display(df_discount)

# Write Gold Table + CSV
write_gold_table(
    df        = df_discount,
    table_key = "discount_impact"
)


# COMMAND ----------

# GOLD TABLE 6 — Executive KPIs
# High-level yearly KPIs for management dashboard
# Used for: KPI cards + YoY comparison in dashboard

year_window = Window.orderBy("Order_Year")

df_kpis = df_silver \
    .groupBy("Order_Year") \
    .agg(
        F.round(F.sum("Sales"),   2) .alias("Total_Sales"),
        F.round(F.sum("Profit"),  2) .alias("Total_Profit"),
        F.countDistinct("`Order ID`") .alias("Total_Orders"),
        F.sum("Quantity")            .alias("Total_Units_Sold"),
        F.round(F.avg("Sales"),   2) .alias("Avg_Order_Value"),
        F.round(F.avg("Discount"), 4).alias("Avg_Discount"),
        F.round(F.avg("Shipping_Days"), 2)
                                     .alias("Avg_Shipping_Days"),
        F.sum(F.col("Is_Loss").cast(IntegerType()))
                                     .alias("Loss_Transactions"),
        F.count("`Order ID`")        .alias("Total_Transactions"),
        F.sum(F.col("Is_Returned").cast(IntegerType()))
                                     .alias("Returned_Orders"),
        F.round(F.sum("Discount_Amount"), 2)
                                     .alias("Total_Discount_Given")
    ) \
    .withColumn(
        "Profit_Margin_Pct",
        F.round(
            (F.col("Total_Profit") / F.col("Total_Sales"))
            * 100, 2
        )
    ) \
    .withColumn(
        "Loss_Rate_Pct",
        F.round(
            (F.col("Loss_Transactions") /
             F.col("Total_Transactions")) * 100, 2
        )
    ) \
    .withColumn(
        "Return_Rate_Pct",
        F.round(
            (F.col("Returned_Orders") /
             F.col("Total_Orders")) * 100, 2
        )
    ) \
    .withColumn(
        "Prev_Year_Sales",
        F.lag("Total_Sales").over(year_window)
    ) \
    .withColumn(
        "YoY_Sales_Growth_Pct",
        F.when(
            F.col("Prev_Year_Sales").isNotNull() &
            (F.col("Prev_Year_Sales") != 0),
            F.round(
                ((F.col("Total_Sales") -
                  F.col("Prev_Year_Sales")) /
                  F.col("Prev_Year_Sales")) * 100, 2
            )
        ).otherwise(F.lit(None))
    ) \
    .withColumn(
        "Prev_Year_Profit",
        F.lag("Total_Profit").over(year_window)
    ) \
    .withColumn(
        "YoY_Profit_Growth_Pct",
        F.when(
            F.col("Prev_Year_Profit").isNotNull() &
            (F.col("Prev_Year_Profit") != 0),
            F.round(
                ((F.col("Total_Profit") -
                  F.col("Prev_Year_Profit")) /
                  F.col("Prev_Year_Profit")) * 100, 2
            )
        ).otherwise(F.lit(None))
    ) \
    .drop("Prev_Year_Sales", "Prev_Year_Profit") \
    .orderBy("Order_Year")

# Preview all years
display(df_kpis)

# Write Gold Table + CSV
write_gold_table(
    df        = df_kpis,
    table_key = "executive_kpis"
)


# COMMAND ----------

# Unpersist Silver Cache & Verify All Gold Tables

# Release cached Silver DataFrame
df_silver.unpersist()

all_good        = True
total_gold_rows = 0

for table_key, table_name in GOLD_TABLES.items():
    try:
        count = spark.read \
            .format("delta") \
            .table(table_name) \
            .count()
        total_gold_rows += count
    except Exception as e:
        print(f" {table_key:<30} : MISSING — {e}")
        all_good = False

print(f"Total Gold rows : {total_gold_rows:,}")

if all_good:
    print(" ALL 6 GOLD TABLES VERIFIED")
else:
    print(" SOME GOLD TABLES MISSING — Check errors")



# COMMAND ----------

# Verify CSV Exports for Dashboard

# csv_folders = dbutils.fs.ls(CSV_EXPORT_BASE)

# print(f" {CSV_EXPORT_BASE}/")

# for folder in sorted(csv_folders, key=lambda x: x.name):
#     try:
#         csv_files = [
#             f for f in dbutils.fs.ls(folder.path)
#             if f.name.endswith(".csv")
#         ]
#         if csv_files:
#             size_kb = csv_files[0].size / 1024
#             print(
#                 f"{folder.name:<35} "
#                 f"{size_kb:>8.1f} KB"
#             )
#         else:
#             print(f"{folder.name:<35} No CSV found")
#     except Exception as e:
#         print(f"{folder.name} — {e}")

# print("CSV files are ready for Power BI!")
