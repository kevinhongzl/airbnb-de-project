import os
import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig

from airflow import DAG

# custom utils
sys.path.append("/opt/airflow/")
from extract.airflow_schedule import get_schedule
from extract.download_files import download_files_if_not_exist
from load.bigquery_utils import create_and_load, upload_blob

# parameters
root = "/opt/airflow"
terraform = f"{root}/terraform"
data_source = f"{root}/data"
extract = f"{root}/extract"
load = f"{root}/load"
transform = f"{root}/dbt/gcp"  # the dir you mount your dbt project
dbt_executable = Path("/home/airflow/.local/bin/dbt")  # the dir of dbt binaries in the container
venv_execution_config = ExecutionConfig(dbt_executable_path=str(dbt_executable))
db_connection = ProfileConfig(
    profile_name="dbt_gcp",  # the configuration of your dbt profile
    target_name="dev",
    profiles_yml_filepath=f"{transform}/profiles.yml",
)
test_periods = 6

with DAG(
    "gcp-elt",
    description="An ELT data piepline using Google Cloud Platform (GCP)",
    catchup=True,
    start_date=datetime(2022, 8, 1),
    schedule=get_schedule(-test_periods),
    end_date=datetime(2024, 12, 31),
    max_active_runs=1,
    max_active_tasks=10,
    tags=["ELT", "cloud"],
) as dag:

    # [START operators and tasks]
    @task(task_id="download_files")
    def download_files_to_local(ds=None, **kwargs):
        download_files_if_not_exist(data_source, ds)

    with TaskGroup(group_id="initialize_infras") as initialize_infras:

        @task.bash
        def terraform_init():
            return f"terraform -chdir={terraform} init"

        @task.bash
        def terraform_validate():
            return f"terraform -chdir={terraform} validate"

        @task.bash
        def terraform_apply():
            return f"terraform -chdir={terraform} apply -auto-approve"

        terraform_init() >> terraform_validate() >> terraform_apply()

    with TaskGroup(group_id="load_files_to_data_lake") as load_files_to_data_lake:

        @task
        def convert_listings_csv_into_parquet(ds, **kwargs):
            import pandas as pd

            data = pd.read_csv(f"{data_source}/{ds}/listings.csv.gz")
            data.to_parquet(f"{data_source}/{ds}/listings.parquet")

        @task
        def convert_reviews_csv_into_parquet(ds, **kwargs):
            import pandas as pd

            data = pd.read_csv(f"{data_source}/{ds}/reviews.csv.gz")
            data.to_parquet(f"{data_source}/{ds}/reviews.parquet")

        @task
        def convert_neighbourhoods_geojson_into_json(ds, **kwargs):
            import json

            with open(f"{data_source}/{ds}/neighbourhoods.geojson", "r", encoding="utf8") as f:
                data = json.load(f)
            with open(f"{data_source}/{ds}/neighbourhoods.json", "w") as f:
                json_text = ""
                for nbh in data["features"]:
                    json_text += json.dumps(nbh, ensure_ascii=False) + "\n"
                f.write(json_text)

        @task(task_id="upload_files_to_gcs")
        def upload_files_to_gcs(ds=None, **kwargs):
            bucket_name = os.environ["TF_VAR_BUCKET_NAME"]
            filenames = [
                "listings.parquet",
                "reviews.parquet",
                "neighbourhoods.json",
            ]
            for file in filenames:
                upload_blob(bucket_name, data_source, ds, file)

        c1 = convert_listings_csv_into_parquet()
        c2 = convert_reviews_csv_into_parquet()
        c3 = convert_neighbourhoods_geojson_into_json()
        [c1, c2, c3] >> upload_files_to_gcs()

    with TaskGroup(group_id="create_raw_tables_in_data_warehouse") as create_raw_tables:

        @task
        def create_and_load_raw_listings(ds, **kwargs):
            from google.cloud import bigquery

            project = os.environ["TF_VAR_PROJECT"]
            dataset_id = os.environ["TF_VAR_DATASET_ID"]
            table_name = "raw_listings"
            bucket = os.environ["TF_VAR_BUCKET_NAME"]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                clustering_fields=["neighbourhood_cleansed", "host_id"],
            )
            create_and_load(
                project, dataset_id, table_name, bucket, ds, "listings.parquet", job_config
            )

        @task
        def create_and_load_raw_reviews(ds, **kwargs):
            from google.cloud import bigquery

            project = os.environ["TF_VAR_PROJECT"]
            dataset_id = os.environ["TF_VAR_DATASET_ID"]
            table_name = "raw_reviews"
            bucket = os.environ["TF_VAR_BUCKET_NAME"]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE",
                clustering_fields=["reviewer_id", "listing_id"],
            )
            create_and_load(
                project, dataset_id, table_name, bucket, ds, "reviews.parquet", job_config
            )

        @task
        def create_and_load_raw_neighbourhoods(ds, **kwargs):
            from google.cloud import bigquery

            project = os.environ["TF_VAR_PROJECT"]
            dataset_id = os.environ["TF_VAR_DATASET_ID"]
            table_name = "raw_neighbourhoods"
            bucket = os.environ["TF_VAR_BUCKET_NAME"]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                json_extension="GEOJSON",
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
            )
            create_and_load(
                project, dataset_id, table_name, bucket, ds, "neighbourhoods.json", job_config
            )

        cl1 = create_and_load_raw_listings()
        cl2 = create_and_load_raw_reviews()
        cl3 = create_and_load_raw_neighbourhoods()
        [cl1, cl2, cl3]

    data_warehouse = DbtTaskGroup(
        group_id="transform_into_staging_dimension_fact_tables",
        project_config=ProjectConfig(transform),
        profile_config=db_connection,
        execution_config=venv_execution_config,
    )

    update_dashboard = EmptyOperator(task_id="update_dashboard")
    # [END operators and tasks]

    # [START pipeline]
    (
        download_files_to_local()
        >> initialize_infras
        >> load_files_to_data_lake
        >> create_raw_tables
        >> data_warehouse
        >> update_dashboard
    )
    # [END pipeline]
