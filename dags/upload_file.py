from airflow.sdk import dag
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from airflow.models.baseoperator import chain

@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)
def upload_file():
    bucket_name = os.getenv('BUCKET_NAME')
    project_id =  os.getenv('PROJECT_ID')
    raw_table_invoices = 'raw_invoices'
    gcp_conn_id = 'gcp'
    bronze_dataset = 'bronze_retail'
    silver_dataset = 'silver_retail'
    gold_dataset = 'gold_retail'

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src=['/usr/local/airflow/include/dataset/online_retail.csv', '/usr/local/airflow/include/dataset/country_codes.csv'],
        dst='raw/',
        bucket=bucket_name,
        gcp_conn_id=gcp_conn_id,
        mime_type='text/csv'
    )

    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_bronze_dataset',
        dataset_id=bronze_dataset,
        gcp_conn_id=gcp_conn_id
    )

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_silver_dataset',
        dataset_id=silver_dataset,
        gcp_conn_id=gcp_conn_id
    )

    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_gold_dataset',
        dataset_id=gold_dataset,
        gcp_conn_id=gcp_conn_id
    )

    chain(
        upload_csv_to_gcs,
        create_bronze_dataset,
        create_silver_dataset,
        create_gold_dataset
    )
    
upload_file()