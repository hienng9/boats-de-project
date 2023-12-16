FROM quay.io/astronomer/astro-runtime:9.6.0
USER $AIRFLOW_UID

WORKDIR /usr/local/airflow
COPY dbt-requirements.txt ./
# install dbt into a virtual environment
RUN python -m venv dbt_env && source dbt_env/bin/activate && \
    pip install --upgrade pip && pip install --no-cache-dir -r dbt-requirements.txt && deactivate