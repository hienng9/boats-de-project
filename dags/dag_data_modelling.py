# from cosmos_config import DBT_CONFIG, DBT_PROJECT_CONFIG
# from cosmos import DbtDag

# from airflow.utils.dates import days_ago

# data_modelling = DbtDag(
#   dag_id = "data_modelling_dag",
#   profile_config = DBT_CONFIG,
#   project_config = DBT_PROJECT_CONFIG,
#   operator_args={ 
#           "install_deps": True,  # install any necessary dependencies before running any dbt command 
#           "full_refresh": True,  # used only in dbt commands that support this flag 
#           "schema": "public",
#           "dbt_executable_path":"/usr/local/airflow/dbt_env/bin/dbt"
#      }, 
#   # dbt_args = {
    
#   # },
#   schedule_interval=None, 
#   start_date = days_ago(1), 
#   catchup = False,
#   default_args={"retries": 1}
# )

# data_modelling
