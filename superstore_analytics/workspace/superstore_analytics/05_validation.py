# Databricks notebook source
# Imports & Configuration

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

# Database & Table Config
BRONZE_TABLE     = "superstore_bronze.orders_raw"
SILVER_TABLE     = "superstore_silver.orders_cleaned"
GOLD_TABLES = {
    "sales_trend"          : "superstore_gold.sales_trend",
    "category_performance" : "superstore_gold.category_performance",
    "regional_performance" : "superstore_gold.regional_performance",
    "loss_making_products" : "superstore_gold.loss_making_products",
    "discount_impact"      : "superstore_gold.discount_impact",
    "executive_kpis"       : "superstore_gold.executive_kpis",
}
VALIDATION_TABLE = "superstore_gold.validation_report"

# Thresholds
MIN_RETENTION_PCT  = 95.0   # Bronze → Silver retention
MAX_NULL_PCT       = 5.0    # Max null % in Silver
MAX_DUPLICATE_PCT  = 2.0    # Max duplicate % in Silver
MAX_VARIANCE_PCT   = 5.0    # Sales/Profit variance %

# Mandatory Columns
MANDATORY_COLS = [
    "Order ID", "Order Date", "Sales",
    "Profit", "Discount", "Category", "Region"
]

# Derived Columns
DERIVED_COLS = [
    "Profit_Margin_Pct", "Discount_Amount",
    "Is_Loss", "Order_Year", "Order_Month",
    "Order_Quarter", "Shipping_Days"
]

# Tracking
VALIDATION_TIME    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
validation_results = []

print(f"Validation Time : {VALIDATION_TIME}")

# COMMAND ----------

# Validation Result Tracker

def add_result(
    check_name  : str,
    category    : str,
    status      : str,
    expected    : str,
    actual      : str,
    layer       : str = "GENERAL"
) -> None:
    """
    Add a validation result to tracker.

    Args:
        check_name : Name of the check
        category   : Category 1 / 2 / 3
        status     : PASS / FAIL / WARN
        expected   : Expected value/condition
        actual     : Actual value/condition
        layer      : BRONZE / SILVER / GOLD
    """
    validation_results.append({
        "category"        : category,
        "check_name"      : check_name,
        "status"          : status,
        "expected"        : str(expected),
        "actual"          : str(actual),
        "layer"           : layer,
        "validation_time" : VALIDATION_TIME
    })

    icon = "✅" if status == "PASS" \
        else "❌" if status == "FAIL" \
        else "⚠️ "

    print(f"  {icon} [{status}] {check_name}")
    print(f"     ├── Expected : {expected}")
    print(f"     └── Actual   : {actual}")


print("VALIDATION TRACKER READY")


# COMMAND ----------

# Load All Delta Tables for Validation

# Bronze 
df_bronze     = spark.read.format("delta").table(BRONZE_TABLE)
bronze_count  = df_bronze.count()
print(f"Bronze : {bronze_count:,} rows")

# Silver
df_silver     = spark.read.format("delta").table(SILVER_TABLE)
silver_count  = df_silver.count()
print(f"Silver : {silver_count:,} rows")

# Gold
df_gold = {}
for key, table_name in GOLD_TABLES.items():
    try:
        df_gold[key] = spark.read \
            .format("delta").table(table_name)
        count = df_gold[key].count()
        print(f" Gold [{key:<25}] : {count:,} rows")
    except Exception as e:
        df_gold[key] = None
        print(f" Gold [{key:<25}] : FAILED — {e}")

print("All tables loaded successfully")


# COMMAND ----------

# Data Accuracy & Consistency After ETL
#
# Checks:
#   1a. Row count consistency Bronze → Silver
#   1b. Schema completeness (mandatory columns)
#   1c. Null values in mandatory columns
#   1d. Duplicate records after cleaning
#   1e. Date format validity
#   1f. Numeric field validity
#   1g. Text field standardization
#   1h. Discount range validity [0,1]

CAT1 = "Category 1: Data Accuracy & Consistency"

# 1a. Row Count Consistency
print("\n  1a. Row Count Consistency")
retention_pct = (silver_count / bronze_count * 100) \
    if bronze_count > 0 else 0

add_result(
    check_name = "Row Retention Bronze→Silver",
    category   = CAT1,
    status     = "PASS" if retention_pct >= MIN_RETENTION_PCT
                 else "FAIL",
    expected   = f">= {MIN_RETENTION_PCT}% retention",
    actual     = f"{retention_pct:.2f}% "
                 f"({silver_count:,}/{bronze_count:,} rows)",
    layer      = "SILVER"
)

# 1b. Schema Completeness
print("\n  1b. Schema Completeness")
missing_cols = [
    c for c in MANDATORY_COLS
    if c not in df_silver.columns
]
add_result(
    check_name = "Mandatory Columns Present",
    category   = CAT1,
    status     = "PASS" if not missing_cols else "FAIL",
    expected   = f"All {len(MANDATORY_COLS)} mandatory cols",
    actual     = "All present" if not missing_cols
                 else f"Missing: {missing_cols}",
    layer      = "SILVER"
)

# 1c. Null Values in Mandatory Columns
print("\n  1c. Null Values in Mandatory Columns")
for col_name in MANDATORY_COLS:
    if col_name in df_silver.columns:
        null_count = df_silver.filter(
            F.col(f"`{col_name}`").isNull()
        ).count()
        add_result(
            check_name = f"Null Check: {col_name}",
            category   = CAT1,
            status     = "PASS" if null_count == 0 else "FAIL",
            expected   = "0 nulls",
            actual     = f"{null_count:,} nulls",
            layer      = "SILVER"
        )

# 1d. Duplicate Records After Cleaning
print("\n  1d. Duplicate Records After Cleaning")
unique_rows = df_silver.dropDuplicates([
    "Order ID", "Product ID", "Order_Year"
]).count()
dup_count   = silver_count - unique_rows
dup_pct     = (dup_count / silver_count * 100) \
    if silver_count > 0 else 0

add_result(
    check_name = "Duplicate Rate After Cleaning",
    category   = CAT1,
    status     = "PASS" if dup_pct <= MAX_DUPLICATE_PCT
                 else "FAIL",
    expected   = f"<= {MAX_DUPLICATE_PCT}% duplicates",
    actual     = f"{dup_pct:.2f}% ({dup_count:,} rows)",
    layer      = "SILVER"
)

# 1e. Date Format Validity
print("\n  1e. Date Format Validity")
for date_col in ["Order Date", "Ship Date"]:
    if date_col in df_silver.columns:
        invalid_dates = df_silver.filter(
            F.col(f"`{date_col}`").isNull()
        ).count()
        add_result(
            check_name = f"Date Validity: {date_col}",
            category   = CAT1,
            status     = "PASS" if invalid_dates == 0
                         else "WARN",
            expected   = "All dates parseable",
            actual     = f"{invalid_dates:,} unparseable dates",
            layer      = "SILVER"
        )

# 1f. Numeric Field Validity
print("\n  1f. Numeric Field Validity")

# Sales must be > 0
invalid_sales = df_silver.filter(
    F.col("Sales") <= 0
).count()
add_result(
    check_name = "Sales > 0",
    category   = CAT1,
    status     = "PASS" if invalid_sales == 0 else "FAIL",
    expected   = "All Sales > 0",
    actual     = f"{invalid_sales:,} invalid sales",
    layer      = "SILVER"
)

# Quantity must be > 0
invalid_qty = df_silver.filter(
    F.col("Quantity") <= 0
).count()
add_result(
    check_name = "Quantity > 0",
    category   = CAT1,
    status     = "PASS" if invalid_qty == 0 else "FAIL",
    expected   = "All Quantity > 0",
    actual     = f"{invalid_qty:,} invalid quantities",
    layer      = "SILVER"
)

# 1g. Text Standardization
print("\n  1g. Text Standardization — Region Casing")
invalid_casing = df_silver.filter(
    F.col("Region") != F.initcap(F.col("Region"))
).count()
add_result(
    check_name = "Region Title Case",
    category   = CAT1,
    status     = "PASS" if invalid_casing == 0 else "FAIL",
    expected   = "All regions in Title Case",
    actual     = f"{invalid_casing:,} inconsistent values",
    layer      = "SILVER"
)

# 1h. Discount Range Validity
print("\n  1h. Discount Range Validity [0, 1]")
bad_discount = df_silver.filter(
    (F.col("Discount") < 0) | (F.col("Discount") > 1)
).count()
add_result(
    check_name = "Discount Range [0, 1]",
    category   = CAT1,
    status     = "PASS" if bad_discount == 0 else "FAIL",
    expected   = "All Discount in [0, 1]",
    actual     = f"{bad_discount:,} invalid discounts",
    layer      = "SILVER"
)


# COMMAND ----------

# Sales & Profit Calculation Validation
#
# Checks:
#   2a. Sales total reconciliation Bronze → Silver
#   2b. Profit total reconciliation Bronze → Silver
#   2c. Profit Margin calculation accuracy
#   2d. Discount Amount calculation accuracy
#   2e. Is_Loss flag accuracy
#   2f. Shipping Days calculation accuracy
#   2g. YoY Growth calculation in executive KPIs
#   2h. All 7 derived columns present and correct

CAT2 = "Category 2: Sales & Profit Calculations"

# 2a. Sales Total Reconciliation
print("\n  2a. Sales Total Reconciliation Bronze → Silver")
bronze_sales = df_bronze \
    .agg(F.sum(F.col("Sales").cast(DoubleType()))) \
    .collect()[0][0] or 0
silver_sales = df_silver \
    .agg(F.sum(F.col("Sales"))) \
    .collect()[0][0] or 0

sales_variance = abs(silver_sales - bronze_sales) \
    / abs(bronze_sales) * 100 if bronze_sales != 0 else 0

add_result(
    check_name = "Sales Total Reconciliation",
    category   = CAT2,
    status     = "PASS" if sales_variance <= MAX_VARIANCE_PCT
                 else "FAIL",
    expected   = f"Variance <= {MAX_VARIANCE_PCT}%",
    actual     = f"Bronze=${bronze_sales:,.2f} | "
                 f"Silver=${silver_sales:,.2f} | "
                 f"Variance={sales_variance:.2f}%",
    layer      = "SILVER"
)

# 2b. Profit Total Reconciliation
print("\n  2b. Profit Total Reconciliation Bronze → Silver")
bronze_profit = df_bronze \
    .agg(F.sum(F.col("Profit").cast(DoubleType()))) \
    .collect()[0][0] or 0
silver_profit = df_silver \
    .agg(F.sum(F.col("Profit"))) \
    .collect()[0][0] or 0

profit_variance = abs(silver_profit - bronze_profit) \
    / abs(bronze_profit) * 100 if bronze_profit != 0 else 0

add_result(
    check_name = "Profit Total Reconciliation",
    category   = CAT2,
    status     = "PASS" if profit_variance <= MAX_VARIANCE_PCT
                 else "FAIL",
    expected   = f"Variance <= {MAX_VARIANCE_PCT}%",
    actual     = f"Bronze=${bronze_profit:,.2f} | "
                 f"Silver=${silver_profit:,.2f} | "
                 f"Variance={profit_variance:.2f}%",
    layer      = "SILVER"
)

# 2c. Profit Margin Calculation Accuracy
print("\n  2c. Profit Margin Calculation Accuracy")

# Recalculate Profit Margin and compare
df_margin_check = df_silver \
    .withColumn(
        "Expected_Margin",
        F.when(
            F.col("Sales") != 0,
            F.round(
                (F.col("Profit") / F.col("Sales")) * 100, 2
            )
        ).otherwise(F.lit(0.0))
    ) \
    .withColumn(
        "Margin_Diff",
        F.abs(
            F.col("Profit_Margin_Pct") -
            F.col("Expected_Margin")
        )
    )

margin_errors = df_margin_check.filter(
    F.col("Margin_Diff") > 0.01   # tolerance 0.01%
).count()

add_result(
    check_name = "Profit Margin Calculation",
    category   = CAT2,
    status     = "PASS" if margin_errors == 0 else "FAIL",
    expected   = "Margin = (Profit/Sales)*100",
    actual     = f"{margin_errors:,} calculation errors",
    layer      = "SILVER"
)

# 2d. Discount Amount Calculation Accuracy
print("\n  2d. Discount Amount Calculation Accuracy")

df_disc_check = df_silver \
    .withColumn(
        "Expected_Disc_Amt",
        F.round(F.col("Sales") * F.col("Discount"), 2)
    ) \
    .withColumn(
        "Disc_Diff",
        F.abs(
            F.col("Discount_Amount") -
            F.col("Expected_Disc_Amt")
        )
    )

disc_errors = df_disc_check.filter(
    F.col("Disc_Diff") > 0.01
).count()

add_result(
    check_name = "Discount Amount Calculation",
    category   = CAT2,
    status     = "PASS" if disc_errors == 0 else "FAIL",
    expected   = "Discount_Amount = Sales × Discount",
    actual     = f"{disc_errors:,} calculation errors",
    layer      = "SILVER"
)

# 2e. Is_Loss Flag Accuracy
print("\n  2e. Is_Loss Flag Accuracy")

is_loss_errors = df_silver.filter(
    (
        (F.col("Profit") < 0) &
        (F.col("Is_Loss") == False)
    ) | (
        (F.col("Profit") >= 0) &
        (F.col("Is_Loss") == True)
    )
).count()

add_result(
    check_name = "Is_Loss Flag Accuracy",
    category   = CAT2,
    status     = "PASS" if is_loss_errors == 0 else "FAIL",
    expected   = "Is_Loss = True where Profit < 0",
    actual     = f"{is_loss_errors:,} incorrect flags",
    layer      = "SILVER"
)

# 2f. Shipping Days Accuracy
print("\n  2f. Shipping Days Accuracy")

neg_shipping = df_silver.filter(
    F.col("Shipping_Days") < 0
).count()

add_result(
    check_name = "Shipping Days >= 0",
    category   = CAT2,
    status     = "PASS" if neg_shipping == 0 else "FAIL",
    expected   = "All Shipping_Days >= 0",
    actual     = f"{neg_shipping:,} negative values",
    layer      = "SILVER"
)

# 2g. YoY Growth Calculation in Executive KPIs
print("\n  2g. YoY Growth Calculation")

if df_gold.get("executive_kpis") is not None:
    df_kpi = df_gold["executive_kpis"]

    # 2019 should have null YoY (no prior year)
    null_yoy = df_kpi.filter(
        (F.col("Order_Year") == 2019) &
        (F.col("YoY_Sales_Growth_Pct").isNull())
    ).count()

    # 2020+ should have non-null YoY
    valid_yoy = df_kpi.filter(
        (F.col("Order_Year") > 2019) &
        (F.col("YoY_Sales_Growth_Pct").isNotNull())
    ).count()

    add_result(
        check_name = "YoY Growth Calculation",
        category   = CAT2,
        status     = "PASS"
                     if null_yoy == 1 and valid_yoy == 4
                     else "WARN",
        expected   = "2019=null, 2020-2023 have YoY%",
        actual     = f"2019 null={null_yoy==1} | "
                     f"2020-2023 valid={valid_yoy}/4",
        layer      = "GOLD"
    )
else:
    add_result(
        check_name = "YoY Growth Calculation",
        category   = CAT2,
        status     = "FAIL",
        expected   = "executive_kpis table exists",
        actual     = "Table not found",
        layer      = "GOLD"
    )

# 2h. All 7 Derived Columns Present
print("\n  2h. All 7 Derived Columns Present")
missing_derived = [
    c for c in DERIVED_COLS
    if c not in df_silver.columns
]
add_result(
    check_name = "All 7 Derived Columns Present",
    category   = CAT2,
    status     = "PASS" if not missing_derived else "FAIL",
    expected   = f"All {len(DERIVED_COLS)} derived columns",
    actual     = "All present" if not missing_derived
                 else f"Missing: {missing_derived}",
    layer      = "SILVER"
)

# COMMAND ----------

# Dashboard Data Verification
#
# Checks:
#   3a. All 6 Gold tables exist and non-empty
#   3b. Sales trend covers all 5 years (2019-2023)
#   3c. All 4 regions present in regional performance
#   3d. All 3 categories present in category performance
#   3e. Loss products correctly identified
#   3f. Discount buckets cover full range
#   3g. Executive KPIs have all 5 years
#   3h. Gold totals match Silver totals

CAT3 = "Category 3: Dashboard Data Verification"

# 3a. All 6 Gold Tables Exist & Non-Empty
print("\n  3a. All 6 Gold Tables Exist & Non-Empty")
for key, df in df_gold.items():
    if df is not None:
        count = df.count()
        add_result(
            check_name = f"Gold Table: {key}",
            category   = CAT3,
            status     = "PASS" if count > 0 else "FAIL",
            expected   = "Table exists with rows",
            actual     = f"{count:,} rows",
            layer      = "GOLD"
        )
    else:
        add_result(
            check_name = f"Gold Table: {key}",
            category   = CAT3,
            status     = "FAIL",
            expected   = "Table exists with rows",
            actual     = "Table missing",
            layer      = "GOLD"
        )

# 3b. Sales Trend Covers All 5 Years
print("\n  3b. Sales Trend Covers All 5 Years")
if df_gold.get("sales_trend") is not None:
    years = [
        row["Order_Year"]
        for row in df_gold["sales_trend"]
        .select("Order_Year")
        .distinct()
        .collect()
    ]
    expected_years  = {2019, 2020, 2021, 2022, 2023}
    actual_years    = set(years)
    missing_years   = expected_years - actual_years

    add_result(
        check_name = "Sales Trend Years Coverage",
        category   = CAT3,
        status     = "PASS" if not missing_years else "FAIL",
        expected   = "Years 2019-2023 all present",
        actual     = f"Found: {sorted(actual_years)} | "
                     f"Missing: {missing_years or 'None'}",
        layer      = "GOLD"
    )

# 3c. All 4 Regions Present
print("\n  3c. All 4 Regions Present in Regional Performance")
if df_gold.get("regional_performance") is not None:
    regions = [
        row["Region"]
        for row in df_gold["regional_performance"]
        .select("Region")
        .distinct()
        .collect()
    ]
    expected_regions = {"East", "West", "Central", "South"}
    actual_regions   = set(regions)
    missing_regions  = expected_regions - actual_regions

    add_result(
        check_name = "All 4 Regions Present",
        category   = CAT3,
        status     = "PASS" if not missing_regions else "FAIL",
        expected   = "East, West, Central, South",
        actual     = f"Found: {sorted(actual_regions)}",
        layer      = "GOLD"
    )

# 3d. All 3 Categories Present
print("\n  3d. All 3 Categories in Category Performance")
if df_gold.get("category_performance") is not None:
    categories = [
        row["Category"]
        for row in df_gold["category_performance"]
        .select("Category")
        .distinct()
        .collect()
    ]
    expected_cats = {
        "Furniture",
        "Office Supplies",
        "Technology"
    }
    actual_cats   = set(categories)
    missing_cats  = expected_cats - actual_cats

    add_result(
        check_name = "All 3 Categories Present",
        category   = CAT3,
        status     = "PASS" if not missing_cats else "FAIL",
        expected   = "Furniture, Office Supplies, Technology",
        actual     = f"Found: {sorted(actual_cats)}",
        layer      = "GOLD"
    )

# 3e. Loss Products Correctly Identified
print("\n  3e. Loss Products Correctly Identified")
if df_gold.get("loss_making_products") is not None:
    loss_count = df_gold["loss_making_products"].count()

    # All products in loss table should have
    # negative Total_Loss
    invalid_loss = df_gold["loss_making_products"].filter(
        F.col("Total_Loss") >= 0
    ).count()

    add_result(
        check_name = "Loss Products Have Negative Profit",
        category   = CAT3,
        status     = "PASS" if invalid_loss == 0 else "FAIL",
        expected   = "All Total_Loss < 0",
        actual     = f"{loss_count:,} products | "
                     f"{invalid_loss:,} invalid",
        layer      = "GOLD"
    )

# 3f. Discount Buckets Cover Full Range
print("\n  3f. Discount Buckets Cover Full Range")
if df_gold.get("discount_impact") is not None:
    buckets = [
        row["Discount_Bucket"]
        for row in df_gold["discount_impact"]
        .select("Discount_Bucket")
        .distinct()
        .collect()
    ]
    expected_buckets = {
        "1. No Discount", "2. 1-10%",
        "3. 11-20%", "4. 21-30%",
        "5. 31-40%", "6. 41-50%", "7. 51%+"
    }
    actual_buckets   = set(buckets)
    missing_buckets  = expected_buckets - actual_buckets

    add_result(
        check_name = "Discount Buckets Complete",
        category   = CAT3,
        status     = "PASS" if not missing_buckets else "WARN",
        expected   = f"{len(expected_buckets)} buckets",
        actual     = f"Found {len(actual_buckets)} | "
                     f"Missing: {missing_buckets or 'None'}",
        layer      = "GOLD"
    )

# 3g. Executive KPIs Have All 5 Years
print("\n  3g. Executive KPIs Have All 5 Years")
if df_gold.get("executive_kpis") is not None:
    kpi_years = [
        row["Order_Year"]
        for row in df_gold["executive_kpis"]
        .select("Order_Year")
        .distinct()
        .collect()
    ]
    missing_kpi_years = {
        2019, 2020, 2021, 2022, 2023
    } - set(kpi_years)

    add_result(
        check_name = "KPIs Cover All 5 Years",
        category   = CAT3,
        status     = "PASS" if not missing_kpi_years
                     else "FAIL",
        expected   = "5 years: 2019-2023",
        actual     = f"Found: {sorted(kpi_years)} | "
                     f"Missing: {missing_kpi_years or 'None'}",
        layer      = "GOLD"
    )

# 3h. Gold Sales Total Matches Silver
print("\n  3h. Gold Sales Total Matches Silver")
if df_gold.get("executive_kpis") is not None:
    gold_total_sales = df_gold["executive_kpis"] \
        .agg(F.sum("Total_Sales")) \
        .collect()[0][0] or 0

    variance = abs(
        gold_total_sales - silver_sales
    ) / abs(silver_sales) * 100 \
        if silver_sales != 0 else 0

    add_result(
        check_name = "Gold vs Silver Sales Match",
        category   = CAT3,
        status     = "PASS" if variance <= MAX_VARIANCE_PCT
                     else "FAIL",
        expected   = f"Variance <= {MAX_VARIANCE_PCT}%",
        actual     = f"Silver=${silver_sales:,.2f} | "
                     f"Gold=${gold_total_sales:,.2f} | "
                     f"Variance={variance:.2f}%",
        layer      = "GOLD"
    )


# COMMAND ----------

# Save Validation Report as Delta Table
# superstore_gold.validation_report

df_report = spark.createDataFrame(validation_results)

(
    df_report
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(VALIDATION_TABLE)
)

print(f"Saved to : {VALIDATION_TABLE}")
print(f"Total    : {df_report.count()} checks")

# Display report sorted by status
display(
    spark.sql(f"""
        SELECT
            category,
            check_name,
            status,
            expected,
            actual,
            layer
        FROM {VALIDATION_TABLE}
        ORDER BY
            CASE status
                WHEN 'FAIL' THEN 1
                WHEN 'WARN' THEN 2
                WHEN 'PASS' THEN 3
            END,
            category,
            check_name
    """)
)
