def upload_blob(bucket_name, data_source, ds, file):
    from google.cloud import storage

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    source_file_name = f"{data_source}/{ds}/{file}"
    destination_blob_name = f"{ds}/{file}"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name, if_generation_match=None)
    # Set if_generation_match=None to enable replacing existing blob
    # See: https://stackoverflow.com/questions/75547631/
    # overwrite-single-file-in-a-google-cloud-storage-bucket-via-python-code
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


def create_and_load(project, dataset_id, table_name, bucket, ds, filename, job_config):
    from google.cloud import bigquery

    bq_client = bigquery.Client()
    table_id = f"{project}.{dataset_id}.{table_name}"
    uri = f"gs://{bucket}/{ds}/{filename}"

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    dest_table = bq_client.get_table(table_id)
    print("Loaded {} rows.".format(dest_table.num_rows))
