FROM apache/airflow:2.10.4-python3.10
USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags
RUN apt-get update && apt-get install -y wget

# Install terraform
RUN apt-get install -y gnupg software-properties-common
RUN wget -O- https://apt.releases.hashicorp.com/gpg | gpg --dearmor | tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
RUN gpg --no-default-keyring --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg --fingerprint
RUN echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | tee /etc/apt/sources.list.d/hashicorp.list
RUN apt update
RUN apt-get -y install terraform

# Make sure the airflow user is set before running commands
USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

# USER ${AIRFLOW_UID}