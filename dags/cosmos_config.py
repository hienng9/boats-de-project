# from cosmos import ProfileConfig, ProjectConfig
# from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
# from pathlib import Path 

# DBT_PROJECT_PATH = '/usr/local/airflow/dags'


# DBT_CONFIG = ProfileConfig(
#   profile_name = 'boats',
#   target_name = 'dev',
#   # profile_mapping = GoogleCloudServiceAccountFileProfileMapping(
#   #   conn_id = 'gcp',
#   #   profile_args={"schema": "dbt_hnguyen"}
#   # )
#   profiles_yml_filepath= Path(f'{DBT_PROJECT_PATH}/profiles.yml')
# )

# DBT_PROJECT_CONFIG = ProjectConfig(DBT_PROJECT_PATH)