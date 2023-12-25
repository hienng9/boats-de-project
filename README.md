# Secondhand boats market on Autot.tori.fi
# End-to-end data engineering project

## Architectures
- Cloud services from Google: Compute Engine, BigQuery, and Cloud Storage
- Docker images
- Orchestration: Airflow
- Data modelling: dbt
<img src="https://github.com/hienng9/boats-de-project/tree/master/images/architecture.png" height="400px" />

## Daily DAG
<img src="https://github.com/hienng9/boats-de-project/tree/master/images/airflow.png" height="400px" />
1. Task - Python Operator: Scrape data from autot.tori.fi, transform to tabular data and save to local as parquet files

2. Task - LocalFilesystemToGCSOperator: Upload the file to Google cloud storage
<img src="https://github.com/hienng9/boats-de-project/tree/master/images/GCS.png" height="400px" />

3. Task - BigQueryInsertJobOperator: Create an external table from files in Google Cloud Storage

4. Task - BigQueryInsertJobOperator: Create a staging table from external table
<img src="https://github.com/hienng9/boats-de-project/tree/master/images/BigQuery.png" height="400px" />

5. Task - BigQueryInsertJobOperator: Merge newly scraped data and insert only new rows into existing table

6. Task - BashOperator: Run data modelling using dbt.

## Data modelling - Dimensional modelling.

<img src="https://github.com/hienng9/boats-de-project/tree/master/images/Data modelling.png" height="400px" />

## Credits
Thanks to [dezoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) for the valuable knowledge and a youtube tutorial from [DataWithMarc](https://www.youtube.com/watch?v=DzxtCxi4YaA&t=1110s)
