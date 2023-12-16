from task_fetch_and_save_data import *
from schema import schema
from datetime import datetime

# from airflow.decorators import dag, task
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.baseoperator import chain
from airflow.operators.bash_operator import BashOperator

from astro import sql as aql
from astro.files import File 
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from cosmos import ExecutionConfig, DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode

from cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG

# current_date = '20231212'
path_to_local_home = "/usr/local/airflow"
dataset_file = f"secondhand-boats-{current_date}.parquet"

PROJECT_ID = "de-zoomcamp-401109"
BIGQUERY_DATASET = "secondhand_boats"
RAW_DATASET = "raw_data"
STAGING_TBL_TABLE = 'stg_boats_tbl'
BUCKET = "secondhand-boats"

RAW_TABLE = 'raw_boats'
EXTERNAL_TABLE = 'external_boats_tbl'
DBT_EXECUTABLE_PATH = '/usr/local/airflow/dbt_env/bin/dbt'

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)
# @dag(
#   dag_id="daily_ingestion_gcs_dag",
#     schedule="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=["daily-veneet-tori.fi"],
# )
with DAG(
    dag_id="daily_ingestion_gcs_dag",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["daily-veneet-tori.fi"],
) as dag:
# def ingest_and_transform_data():

  def execute_query(task_id, query, conn = 'gcp'):
    return BigQueryInsertJobOperator(
      task_id = task_id,
        gcp_conn_id = conn,
        configuration = {
          "query": {
            "query": query,
            "useLegacySql": False
          }
        })

  task_fetch_and_save_to_local = PythonOperator(
    task_id = "fetch_and_save_to_local_task",
    python_callable = fetch_all_and_save,
    op_kwargs = {
      "path": path_to_local_home,
      "number_of_pages": 3
    }
  )

  task_local_to_gcs = LocalFilesystemToGCSOperator(
    task_id = "local_to_gcs_task",
    src = f"{path_to_local_home}/data/{dataset_file}",
    gcp_conn_id = "gcp",
    bucket = BUCKET,
    dst = dataset_file
  )

  create_or_replace_external_table_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}` {schema}
    OPTIONS (
      format ="PARQUET",
      uris = ['gs://{BUCKET}/{dataset_file}']
      );

    """

  task_create_external_table = execute_query(
    task_id = "create_external_table_task", 
    query=create_or_replace_external_table_query, 
    )

  create_staging_table_from_ext_table_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{RAW_DATASET}.{STAGING_TBL_TABLE}` AS
    SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}`
    """

  task_create_staging_table = execute_query(
    task_id="create_staging_table_task", 
    query = create_staging_table_from_ext_table_query)

  merge_new_rows_to_raw_table_query = f"""
    MERGE `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}` T
    USING `{PROJECT_ID}.{RAW_DATASET}.{STAGING_TBL_TABLE}` S
    ON T.list_id = S.list_id
    WHEN NOT MATCHED THEN
    INSERT ROW
    """

  task_merge_staging_to_raw_table = execute_query(
    task_id = "task_merge_staging_to_raw_table",
    query = merge_new_rows_to_raw_table_query,
  )
  data_modelling = DbtTaskGroup(
    group_id = 'data_modelling',
    project_config = DBT_PROJECT_CONFIG,
    profile_config = DBT_CONFIG,
    execution_config=execution_config,
    render_config = RenderConfig(
      load_method = LoadMode.DBT_LS,
      select = ['path:models']
  )
  )
  # DbtDag(
  #   dag_id = 'data_modelling_task',
  #   project_config = DBT_PROJECT_CONFIG,
  #   profile_config = DBT_CONFIG,
  #   execution_config = execution_config,
  #   schedule_interval="@daily",
  #   start_date=days_ago(1),
  #   default_args = {"retries": 1}
  # )
  # BashOperator(
  #   task_id='data_modelling',
  #   bash_command="dbt deps && dbt run ",
  # )
  # 
  #   )
  
  # task_fetch_and_save_to_local >> task_local_to_gcs  >> task_create_external_table >> task_create_staging_table >> task_merge_staging_to_raw_table >> data_modelling
  chain(task_fetch_and_save_to_local , task_local_to_gcs  , task_create_external_table , task_create_staging_table , task_merge_staging_to_raw_table, data_modelling )
  

# ingest_and_transform_data()
