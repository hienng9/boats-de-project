import os
from task_fetch_and_save_data import *
from schema import schema
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models.baseoperator import chain
from airflow.operators.bash_operator import BashOperator

path_to_local_home = os.environ['AIRFLOW_HOME']
dbt_path = os.path.join(path_to_local_home, 'dbt')
dataset_file = f"secondhand-boats-{current_date}.parquet"

PROJECT_ID = os.environ['PROJECT_ID']
BIGQUERY_DATASET = "secondhand_boats"
RAW_DATASET = "raw_data"
STAGING_TBL_TABLE = 'stg_boats_tbl'
BUCKET = "secondhand-boats"

RAW_TABLE = 'raw_boats'
EXTERNAL_TABLE = 'external_boats_tbl'
DBT_EXECUTABLE_PATH = os.environ['DBT_EXECUTABLE_PATH']

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="daily_ingestion_gcs_dag",
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["daily-veneet-tori.fi"],
) as dag:
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

  # Task 1: Scrape the first 3 pages of the website to get the latest listings.
  # Save to local disk
  task_fetch_and_save_to_local = PythonOperator(
    task_id = "fetch_and_save_to_local_task",
    python_callable = fetch_all_and_save,
    op_kwargs = {
      "path": path_to_local_home,
      "number_of_pages": 3
    }
  )

  # Task 2: Upload the file to Google cloud storage with the credentials provided in airflow.
  task_local_to_gcs = LocalFilesystemToGCSOperator(
    task_id = "local_to_gcs_task",
    src = f"{path_to_local_home}/data/{dataset_file}",
    gcp_conn_id = "gcp",
    bucket = BUCKET,
    dst = dataset_file
  )

  # Query for create or replace the external table in bigquery with the provide schema.
  # The purpose for providing schema is to make sure bigquery load the parquet file into with correct types.
  create_or_replace_external_table_query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}` {schema}
    OPTIONS (
      format ="PARQUET",
      uris = ['gs://{BUCKET}/{dataset_file}']
      );

    """
  # Task 3: Create or replace the staging table to be loaded in the main table.
  task_create_external_table = execute_query(
    task_id = "create_external_table_task", 
    query=create_or_replace_external_table_query, 
    )

  # Query for create the staging table to be loaded.
  create_staging_table_from_ext_table_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{RAW_DATASET}.{STAGING_TBL_TABLE}` AS
    SELECT * FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{EXTERNAL_TABLE}`
    """
  # Task 4 for create or replace staging table.
  task_create_staging_table = execute_query(
    task_id="create_staging_table_task", 
    query = create_staging_table_from_ext_table_query)

  # Query for MERGE the staging table to raw_boats with the logic 
  # if the list_id of the newly scraping data does not exist in previous table, that row will be loaded into the table.
  merge_new_rows_to_raw_table_query = f"""
    MERGE `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}` T
    USING `{PROJECT_ID}.{RAW_DATASET}.{STAGING_TBL_TABLE}` S
    ON T.list_id = S.list_id
    WHEN NOT MATCHED THEN
    INSERT ROW
    """
  # Task 5: Execute the Merge query.
  task_merge_staging_to_raw_table = execute_query(
    task_id = "task_merge_staging_to_raw_table",
    query = merge_new_rows_to_raw_table_query,
  )

  # Task 6: run dbt run to do the data modelling.
  data_modelling = BashOperator(
    task_id='data_modelling',
    bash_command= f"cd {path_to_local_home} && source dbt_env/bin/activate && cd dbt && dbt deps && dbt run"
  )
 
  # Determine the dependencies between tasks
  chain(
    task_fetch_and_save_to_local , 
    task_local_to_gcs  , 
    task_create_external_table , 
    task_create_staging_table , 
    task_merge_staging_to_raw_table ,
    data_modelling)