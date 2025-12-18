from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import os
from dotenv import load_dotenv

load_dotenv()

@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)

def retail():

    bucket_name = os.getenv('BUCKET_NAME')
    project_id =  os.getenv('PROJECT_ID')
    raw_table_name = 'raw_invoices'
    
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/usr/local/airflow/include/dataset/online_retail.csv',
        dst='raw/online_retail.csv',
        bucket=bucket_name,
        gcp_conn_id='gcp',
        mime_type='text/csv'
    )

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',
        dataset_id='retail_dataset',
        gcp_conn_id='gcp'
    )

    gcs_to_raw = GCSToBigQueryOperator(
        task_id='gcs_to_raw',
        bucket=bucket_name,
        source_objects=['raw/online_retail.csv'],
        destination_project_dataset_table=f'{project_id}.retail_dataset.{raw_table_name}',
        source_format='CSV',
        schema_fields=[
            {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},  # Changed to STRING
            {'name': 'UnitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',  # or 'WRITE_APPEND'
        skip_leading_rows=1,  # if CSV has headers
        gcp_conn_id='gcp'
    )
    upload_csv_to_gcs >> create_retail_dataset >> gcs_to_raw
retail()