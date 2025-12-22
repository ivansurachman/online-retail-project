from airflow.sdk import dag, task
from datetime import datetime

from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProfileConfig, ProjectConfig, RenderConfig, ExecutionConfig

from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()


@dag(
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
    tags=['dbt']
)
def dbt_test():

    project_id =  os.getenv('PROJECT_ID')

    dbt_tg = DbtTaskGroup(
        group_id="dbt_transformations",
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
            select=['path:models/staging', 'path:models/marts']
        )
    )

dbt_test()