# etl-online-retail-project

data source: https://www.kaggle.com/datasets/tunguz/online-retail

airflow tasks test <def_name> <task_id> 2025-01-01

reference:
https://astro-sdk-python.readthedocs.io/en/stable/
https://registry.astronomer.io/providers/apache-airflow-providers-google/versions/19.0.0/modules/LocalFilesystemToGCSOperator


source dbt_venv/bin/activate
cd include/dbt
dbt deps
dbt run --profiles-dir /usr/local/airflow/include/dbt/
