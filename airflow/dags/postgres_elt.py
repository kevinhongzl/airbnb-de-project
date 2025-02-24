import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow import DAG

sys.path.append("/opt/airflow/")
from extract.airflow_schedule import get_schedule
from extract.download_files import download_files_if_not_exist
from load.ingestion_utils import (
    ingest_listings_data,
    ingest_neighbourhoods_data,
    ingest_reviews_data,
)

root = "/opt/airflow"
data_source = f"{root}/data/"
extract = f"{root}/extract"
load = f"{root}/load"
conn_airflow = "postgresql://airflow:airflow@postgres:5432/airflow"
conn_airbnb = "postgresql://airflow:airflow@postgres:5432/airbnb"

# Cosmos Settings
transform = Path("/opt/airflow/dbt/postgres")  # the dir you mount your dbt project
dbt_executable = Path("/home/airflow/.local/bin/dbt")  # the dir of dbt binaries in the container
venv_execution_config = ExecutionConfig(
    dbt_executable_path=str(dbt_executable),
)
db_connection = ProfileConfig(
    profile_name="dbt_pg",  # the configuration of your dbt profile
    target_name="dev",
    # set a connection to the postgres in airflow
    # you can create one from Admin > connection in Airflow webui
    # or using an env variable (The approach I took, see AIRFLOW_CONN_AIRFLOW2POSTGRES)
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="airflow2postgres",
        profile_args={"schema": "public"},
    ),
)


with DAG(
    "postgres-elt",
    description="Create stage tables",
    catchup=True,
    start_date=datetime(2023, 9, 1),
    schedule=get_schedule(),
    end_date=datetime(2024, 10, 31),
    max_active_runs=1,
    concurrency=10,
    tags=["ELT", "on premise"],
) as dag:
    date = "{{ ds }}"

    @task(task_id="download_files")
    def download_files(ds=None, **kwargs):
        download_files_if_not_exist(data_source, ds)

    @task.bash(task_id="create_database")
    def create_database():
        from sqlalchemy import create_engine, text  # fmt: skip
        engine = create_engine(conn_airflow)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1 FROM pg_database WHERE datname = 'airbnb'"))
            airbnb_db_exist = len(list(result)) == 1
        if airbnb_db_exist:
            return "echo 'DATABASE airbnb EXISTS.'"
        else:
            return f"psql {conn_airflow} -c 'CREATE DATABASE airbnb'"

    db = [download_files() >> create_database()]

    with TaskGroup(group_id="create_staging_tables") as create_staging_tables:

        @task.bash
        def create_stg_listings():
            with open(f"{load}/create_stg_listings.sql", "r") as f:
                print("The sql query:\n", f.read())
            return f"psql {conn_airbnb} -f {load}/create_stg_listings.sql"

        @task.bash
        def create_stg_reviews():
            with open(f"{load}/create_stg_reviews.sql", "r") as f:
                print("The sql query:\n", f.read())
            return f"psql {conn_airbnb} -f {load}/create_stg_reviews.sql"

        @task.bash
        def create_stg_neighbourhoods():
            with open(f"{load}/create_stg_neighbourhoods.sql", "r") as f:
                print("The sql query:\n", f.read())
            return f"psql {conn_airbnb} -f {load}/create_stg_neighbourhoods.sql"

        cs1 = db >> create_stg_listings()
        cs2 = db >> create_stg_reviews()
        cs3 = db >> create_stg_neighbourhoods()

    with TaskGroup(group_id="load_staging_tables") as load_staging_tables:

        @task
        def load_stg_listings(ds, **kwargs):
            from sqlalchemy import create_engine  # fmt: skip
            engine = create_engine(conn_airbnb)
            file = data_source + f"/{ds}/listings.csv.gz"
            ingest_listings_data(file, engine)

        @task
        def load_stg_reviews(ds, **kwargs):
            from sqlalchemy import create_engine  # fmt: skip
            engine = create_engine(conn_airbnb)
            file = data_source + f"/{ds}/reviews.csv.gz"
            ingest_reviews_data(file, engine)

        @task
        def load_stg_neighbourhoods(ds, **kwargs):
            from sqlalchemy import create_engine  # fmt: skip
            engine = create_engine(conn_airbnb)
            file = data_source + f"/{ds}/neighbourhoods.geojson"
            ingest_neighbourhoods_data(file, engine)

        ls1 = cs1 >> load_stg_listings()
        ls2 = cs2 >> load_stg_reviews()
        ls3 = cs3 >> load_stg_neighbourhoods()

    transform_into_dimension_and_fact_tables = DbtTaskGroup(
        group_id="transform_into_dimension_and_fact_tables",
        project_config=ProjectConfig(transform),
        profile_config=db_connection,
        execution_config=venv_execution_config,
    )

    update_dashboard = EmptyOperator(task_id="update_dashboard")

    [ls1, ls2, ls3] >> transform_into_dimension_and_fact_tables >> update_dashboard
