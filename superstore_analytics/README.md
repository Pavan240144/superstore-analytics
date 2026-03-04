# SuperStore Analytics — End-to-End Data Engineering Pipeline

A retail analytics solution built on **Azure Databricks** using the **Medallion Architecture** (Bronze → Silver → Gold), orchestrated with **Apache Airflow**, and visualized through a **Databricks SQL Dashboard**.

---

## 📋 Table of Contents

- [Project Overview]
- [Architecture]
- [Tech Stack]
- [Project Structure]
- [Notebooks Deep Dive]
- [Data Flow]
- [Airflow DAG]
- [Performance Optimizations]
- [Dashboard]
- [Setup & Installation]

---

## 📌 Project Overview

This project builds a complete retail analytics pipeline for SuperStore sales data — ingesting raw CSV data from Kaggle, processing it through a multi-layer Delta Lake architecture, and surfacing business insights through interactive dashboards.


## 🏗️ Architecture

```
Kaggle API (vivek468/superstore-dataset-final)
          │
          ▼
┌─────────────────────────────────────────────────────┐
│              DATABRICKS FILE SYSTEM (DBFS)          │
│         dbfs:/FileStore/superstore/raw/             │
│  ┌──────────────┐ ┌───────────┐ ┌───────────────┐  │
│  │  Orders CSV  │ │Returns CSV│ │Salesperson CSV│  │
│  │  (×5 years)  │ │           │ │               │  │
│  └──────────────┘ └───────────┘ └───────────────┘  │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│         🟤 BRONZE LAYER — Raw Ingestion             │
│         superstore_bronze.orders_raw                │
│  • Schema validation on all source files            │
│  • 3-way JOIN (orders + returns + salesperson)      │
│  • Metadata: _ingestion_timestamp, _batch_id,       │
│    _source_layer, _record_hash (MD5)                │
│  • Partitioned by Source_Year                       │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│         ⚪ SILVER LAYER — Cleaned Data              │
│         superstore_silver.orders_cleaned            │
│  • Duplicate removal on [Order ID, Product ID,      │
│    Source_Year]                                     │
│  • Null handling (drop mandatory / fill optional)   │
│  • Text standardization → Title Case                │
│  • Date parsing (M/d/yyyy) + numeric casting        │
│  • 7 derived columns engineered                     │
│  • ≥95% row retention enforced                      │
│  • Partitioned by Order_Year                        │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│         🟡 GOLD LAYER — Business Analytics          │
│                                                     │
│  sales_trend          category_performance          │
│  regional_performance loss_making_products          │
│  discount_impact      executive_kpis                │
│                                                     │
│  + validation_report           │
└─────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────┐
│                Databricks SQL Dashboard        │
└─────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Cloud Platform | Microsoft Azure |
| Data Platform | Azure Databricks |
| Storage | DBFS (Databricks File System) |
| Table Format | Delta Lake |
| Processing | Apache Spark (PySpark) |
| Language | Python 3.x |
| Orchestration | Apache Airflow 2.8+ |
| Airflow Operator | DatabricksSubmitRunOperator |
| Visualization | Databricks SQL Dashboard (Lakeview) |
| Data Source | Kaggle API |
| Cluster Runtime | Databricks Runtime 13.3 LTS |

---

## 📁 Project Structure

```
superstore_analytics/
│
├── databricks_notebooks/
│   ├── 00_setup_and_download.py     # Environment + Kaggle ingestion
│   ├── 01_data_generator.py         # Multi-year data generation
│   ├── 02_bronze_ingestion.py       # Raw → Bronze Delta
│   ├── 03_silver_transformation.py  # Bronze → Silver Delta
│   ├── 04_gold_analytics.py         # Silver → 6 Gold Delta tables
│   └── 05_validation.py             # data validation
│
├── airflow/
│   └── dags/
│       └── superstore_dag.py        # Airflow DAG
│
└── README.md
```

---

## 📓 Notebooks Deep Dive

### Notebook 00 — Setup & Download

- Installs kaggle library on cluster
- Sets Kaggle API credentials
- Downloads SuperStore dataset from Kaggle
- Creates DBFS folder structure
- Verifies download integrity

---

### Notebook 01 — Data Generator

- Reads raw SuperStore data from DBFS
- Splits into 3 source files (orders, returns, salesperson)
- Generates 5 yearly files (2019–2023) with date shifts
- Injects realistic data quality issues
- Saves all files back to DBFS raw layer

---

### Notebook 02 — Bronze Ingestion

- Loads all 5 yearly CSV files from DBFS
- Validates schema on each file
- Joins 3 sources (orders + returns + salesperson)
- Adds Bronze metadata columns
- Writes to Delta Table: superstore_bronze.orders_raw
- Partitioned by Source_Year

---

### Notebook 03 — Silver Transformation

- Reads Bronze Delta Table
- Removes duplicates at scale
- Handles null values
- Standardizes text fields
- Parses and validates dates
- Casts numeric types correctly
- Validates row retention rate
- Writes to Delta Table: superstore_silver.orders_cleaned

---

### Notebook 04 — Gold Analytics

- Reads Silver Delta Table
- Builds 6 business-ready Gold tables:
  1. sales_trend           → Monthly & yearly trends
  2. category_performance  → Category & sub-category
  3. regional_performance  → Region & state level
  4. loss_making_products  → Products losing money
  5. discount_impact       → Discount vs profitability
  6. executive_kpis        → High-level yearly KPIs

---

### Notebook 05 — Validation

**Three Validation Categories:**
- Category 1 → Data Accuracy & Consistency after ETL
- Category 2 → Sales & Profit Calculation Validation  
- Category 3 → Dashboard Data Verification

---

## 🔄 Data Flow

```
Kaggle Dataset (~10,000 rows, 1 year)
        │
        ▼  Notebook 00
DBFS Raw Parquet
        │
        ▼  Notebook 01
8 DBFS CSV Folders (~50,450 rows, 5 years)
  superstore_2019/ → superstore_2023/   (~10,090 rows each)
  superstore_orders/
  superstore_returns/
  superstore_salesperson/
        │
        ▼  Notebook 02
superstore_bronze.orders_raw
  ~50,450 rows | joined | 4 metadata cols | partitioned by Source_Year
        │
        ▼  Notebook 03
superstore_silver.orders_cleaned
  ~49,500 rows | clean | 7 derived cols | partitioned by Order_Year
        │
        ▼  Notebook 04
6 Gold Delta Tables + 6 CSV exports
  sales_trend          
  category_performance 
  regional_performance 
  loss_making_products 
  discount_impact      
  executive_kpis       
        │
        ▼  Notebook 05
validation_report 
```

---

## ⚡ Airflow DAG

**DAG ID:** `superstore_analytics_pipeline`
**Schedule:** Daily at 06:00 AM
**Max Active Runs:** 1

**Task Graph:**
```

task_00_setup  (timeout: 30 min)
        │
        ▼
task_01_generator  (timeout: 30 min)
        │
        ▼
task_02_bronze  (timeout: 60 min)
        │
        ▼
task_03_silver  (timeout: 60 min)
        │
        ▼
task_04_gold  (timeout: 60 min)
        │
        ▼
task_05_validation  (timeout: 20 min)

*Another Way*

        │
        ▼
Run Databrics job (Using DatabricksRunNowOperator) 
```
---

## 🚀 Performance Optimizations

`df_silver.cache()` in Notebook 04 — saves 5 redundant full-table disk reads across the 6 Gold table computations.

---

## 📊 Dashboard

Built using **Databricks SQL Lakeview Dashboard** reading directly from Gold Delta tables.

**Widgets:**
1. **KPI Cards** — Total Sales, Total Profit, Avg Margin %, YoY Growth %
2. **Monthly Sales & Profit Trend** — Line chart (2019–2023)
3. **Category & Sub-Category Performance** — Grouped bar chart
4. **Regional Sales vs Target** — Bar chart with target status coloring
5. **Salesperson Performance** — Bar chart by region
6. **Discount Impact on Profitability** — Bar chart (margin % per bucket)
7. **Top 20 Loss Making Products** — Sortable table with severity flags

**Filters:** Year, Region, Category (cross-filter across all datasets)

---

## 🔧 Setup & Installation

### Prerequisites

- Azure Databricks workspace with Unity Catalog
- Apache Airflow 2.8+ with Databricks provider
- Kaggle account with API key
- Python 3.11

### Step 1 — Clone Repository

```bash
git clone https://github.com/your-username/superstore-analytics.git
cd superstore-analytics
```

### Step 2 — Upload Notebooks to Databricks

```
Databricks Workspace
  → Import
  → Upload all 6 .py files to:
    /Users/your-email/superstore_analytics/
```

### Step 3 — Install Airflow

```bash
python3 -m venv airflow_venv
source airflow_venv/bin/activate
pip install apache-airflow==2.8.0
pip install apache-airflow-providers-databricks==6.0.0
airflow db init
```

### Step 4 — Add Databricks Connection

```bash
airflow connections add databricks_default \
    --conn-type    databricks \
    --conn-host    https://your-workspace.azuredatabricks.net \
    --conn-password your-personal-access-token
```

### Step 5 — Deploy DAG

```bash
cp airflow/dags/superstore_dag.py ~/airflow/dags/
```

### Step 6 — Start Airflow & Trigger

```bash
# Terminal 1
airflow standalone

```
Open `http://localhost:8080` → Find `superstore_analytics_pipeline` → Toggle ON → Trigger ▶️

---


## 📄 License

This project is developed as part of a Data Engineering Training program.

---

