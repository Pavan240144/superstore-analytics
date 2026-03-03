# SuperStore Analytics — Main Airflow DAG


from airflow                    import DAG
from airflow.providers.databricks.operators.databricks \
    import DatabricksSubmitRunOperator
# from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import sys
import os


# Databricks Config
DATABRICKS_CONN_ID    = "databricks_default"
DATABRICKS_CLUSTER_ID = "0301-115553-e5t7ewt7" 

# Notebook paths in Databricks Workspace
NOTEBOOKS_BASE = "/Users/pawankumar@purpletalk.onmicrosoft.com/superstore_analytics"
NOTEBOOKS = {
    "setup"      : f"{NOTEBOOKS_BASE}/00_setup_and_download",
    "generator"  : f"{NOTEBOOKS_BASE}/01_data_generator",
    "bronze"     : f"{NOTEBOOKS_BASE}/02_bronze_ingestion",
    "silver"     : f"{NOTEBOOKS_BASE}/03_silver_transformation",
    "gold"       : f"{NOTEBOOKS_BASE}/04_gold_analytics",
    "validation" : f"{NOTEBOOKS_BASE}/05_validation",
}

# DAG Schedule Config 
DAG_SCHEDULE        = "0 6 * * *"  # Daily 6:00 AM
DAG_START_DATE_YEAR = 2024
DAG_START_DATE_MON  = 1
DAG_START_DATE_DAY  = 1

# Task Config
TASK_TIMEOUT_SECONDS = 3600         # 1 hour per task
TASK_RETRIES         = 2            # Retry twice on fail
TASK_RETRY_DELAY     = timedelta(minutes=5)


# SHARED_CLUSTER_CONFIG = {
#     "cluster_name"        : "superstore-pipeline-cluster",
#     "spark_version"       : "13.3.x-scala2.12",
#     "driver_node_type_id" : "Standard_DS4_v2",
#     "node_type_id"        : "Standard_DS3_v2",
#     "autoscale"           : {
#         "min_workers" : 1,
#         "max_workers" : 2,
#     },
#     "spark_conf"          : {
#         "spark.databricks.delta.preview.enabled"
#             : "true",
#         "spark.databricks.delta.optimizeWrite.enabled"
#             : "true",
#         "spark.databricks.delta.autoCompact.enabled"
#             : "true",
#         "spark.sql.shuffle.partitions"
#             : "8",
#         "spark.sql.adaptive.enabled"
#             : "true",
#         "spark.sql.autoBroadcastJoinThreshold"
#             : "52428800",
#         "spark.sql.session.timeZone"
#             : "UTC",
#     },
#     "cluster_log_conf"    : {
#         "dbfs" : {
#             "destination" :
#                 "dbfs:/FileStore/superstore/logs/cluster"
#         }
#     },
# }

PIPELINE_NAME    = "SuperStore Analytics Pipeline"


def build_notebook_task(
    notebook_key  : str,
    timeout       : int = TASK_TIMEOUT_SECONDS,
    params        : dict = None,
) -> dict:

    notebook_task = {
        "notebook_path" : NOTEBOOKS[notebook_key],
    }

    if params:
        notebook_task["base_parameters"] = params

    return {
        "notebook_task"   : notebook_task,
        # "existing_cluster_id"     : SHARED_CLUSTER_CONFIG,
        "existing_cluster_id" : DATABRICKS_CLUSTER_ID,
        "timeout_seconds" : timeout,
        "run_name"        : (
            f"superstore_{notebook_key}_"
            "{{ ds_nodash }}"       
        ),
    }


# DAG Default Arguments
default_args = {
    "owner"               : "purpltetalk",
    "depends_on_past"     : False,
    "retries"             : TASK_RETRIES,
    "retry_delay"         : TASK_RETRY_DELAY,
}


# DAG Definition
with DAG(
    dag_id              = "superstore_analytics_pipeline",
    default_args        = default_args,
    description         = (
        "SuperStore End-to-End Analytics Pipeline"
    ),
    schedule   = DAG_SCHEDULE,
    start_date          = datetime(
        DAG_START_DATE_YEAR,
        DAG_START_DATE_MON,
        DAG_START_DATE_DAY
    ),
    catchup             = False,
    tags                = [
        "superstore",
        "databricks",
        "medallion",
    ],
) as dag:


    task_00_setup = DatabricksSubmitRunOperator(
        task_id              = "task_00_setup",
        json                 = build_notebook_task(
            notebook_key = "setup",
            timeout      = 1800,     
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=30),
    )

    task_01_generator = DatabricksSubmitRunOperator(
        task_id              = "task_01_generator",
        json                 = build_notebook_task(
            notebook_key = "generator",
            timeout      = 1800,     # 30 mins
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=30),
    )

    task_02_bronze = DatabricksSubmitRunOperator(
        task_id              = "task_02_bronze",
        json                 = build_notebook_task(
            notebook_key = "bronze",
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=30),
    )

    task_03_silver = DatabricksSubmitRunOperator(
        task_id              = "task_03_silver",
        json                 = build_notebook_task(
            notebook_key = "silver",
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=30),
    )

    task_04_gold = DatabricksSubmitRunOperator(
        task_id              = "task_04_gold",
        json                 = build_notebook_task(
            notebook_key = "gold",
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=30),
    )

    task_05_validation = DatabricksSubmitRunOperator(
        task_id              = "task_05_validation",
        json                 = build_notebook_task(
            notebook_key = "validation",
        ),
        databricks_conn_id   = DATABRICKS_CONN_ID,
        sla                  = timedelta(minutes=20),
    )

    # run_databricks_job = DatabricksRunNowOperator(
    #     task_id="run_job",
    #     databricks_conn_id="databricks_default",
    #     job_id=274584208361663
    # )

    (
        task_00_setup
        >> task_01_generator
        >> task_02_bronze
        >> task_03_silver
        >> task_04_gold
        >> task_05_validation
    )