from airflow.decorators import task

from airflow import DAG

root = "/opt/airflow"
terraform = f"{root}/terraform"

with DAG(
    "gcp-tear-down",
    description="Remove the GCP resources deployed during the run of this data pipeline",
    schedule=None,
    tags=["cloud"],
):

    @task.bash
    def terraform_destroy():
        return f"terraform -chdir={terraform} destroy -auto-approve"

    terraform_destroy()
