
from airflow.sdk import dag
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.models.baseoperator import chain

from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig, ExecutionConfig

from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

def load_sql(filename):
    """Load SQL file from include/dataset directory"""
    sql_path = Path(__file__).parent.parent / 'include' / 'dataset' / filename
    return sql_path.read_text()

country_sql = load_sql('country.sql')

@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)

def retail():

    bucket_name = os.getenv('BUCKET_NAME')
    project_id =  os.getenv('PROJECT_ID')
    raw_table_invoices = 'raw_invoices'
    raw_table_country = 'raw_country'
    gcp_conn_id = 'gcp'
    bronze_dataset = 'bronze_retail'
    

    gcs_to_raw_invoice = GCSToBigQueryOperator(
        task_id='gcs_to_raw_invoice',
        bucket=bucket_name,
        source_objects=['raw/online_retail.csv'],
        destination_project_dataset_table=f'{project_id}.{bronze_dataset}.{raw_table_invoices}',
        source_format='CSV',
        schema_fields=[
            {'name': 'InvoiceNo', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'StockCode', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Quantity', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'InvoiceDate', 'type': 'STRING', 'mode': 'NULLABLE'},  # Changed to STRING
            {'name': 'UnitPrice', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'CustomerID', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE',  # or 'WRITE_APPEND'
        skip_leading_rows=1,  # if CSV has headers
        gcp_conn_id=gcp_conn_id
    )
    
    gcs_to_raw_country = GCSToBigQueryOperator(
        task_id='gcs_to_raw_country',
        bucket=bucket_name,
        source_objects=['raw/country_codes.csv'],
        destination_project_dataset_table=f'{project_id}.{bronze_dataset}.{raw_table_country}',
        source_format='CSV',
        schema_fields=[
            {'name': 'Country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Alpha-2_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Alpha-3_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Numeric', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE',  # or 'WRITE_APPEND'
        skip_leading_rows=1,  # if CSV has headers
        gcp_conn_id=gcp_conn_id
    )

    dbt_brz_to_slv = DbtTaskGroup(
        group_id="dbt_bronze_to_silver",
        project_config=ProjectConfig(
            dbt_project_path='/usr/local/airflow/include/dbt',
        ),
        profile_config=ProfileConfig(
            profile_name="retail",
            target_name="dev",
            profiles_yml_filepath=Path("/usr/local/airflow/include/dbt/profiles.yml"),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=Path("/usr/local/bin/dbt"),
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/staging']
        )
    )

    dbt_slv_to_gld = DbtTaskGroup(
        group_id="dbt_silver_to_gold",
        project_config=ProjectConfig(
            dbt_project_path='/usr/local/airflow/include/dbt',
        ),
        profile_config=ProfileConfig(
            profile_name="retail",
            target_name="prod",
            profiles_yml_filepath=Path("/usr/local/airflow/include/dbt/profiles.yml"),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=Path("/usr/local/bin/dbt"),
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/marts']
        )
    )

    chain(
        gcs_to_raw_invoice,
        gcs_to_raw_country,
        dbt_brz_to_slv,
        dbt_slv_to_gld
    )

retail()