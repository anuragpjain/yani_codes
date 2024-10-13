from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import time
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import google.auth
from googleapiclient.discovery import build
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.operators.functions import CloudFunctionInvokeFunctionOperator  # Updated import
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# Define constants
DATASET_NAME = "tz_pdm_20220401"
TABLE = "orng_activity_equipment_f"
BUCKET_NAME = "als_mb52"
BUCKET_FILE = "orng_table/orng_activity_equipment_*.csv"

# Define default_args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

gcs_bucket_name = 'europe-west1-service-delive-ba7c5405-bucket'
sql_file_path = 'data/orng_activity_equipment_f_curated_insert.sql'

# Define the DAG
with DAG(
        dag_id="bigquery_to_gcs_example",
        default_args=default_args,
        description="Export BigQuery table to GCS and invoke Cloud Function",
        schedule_interval=None,  # Change schedule interval as needed
        start_date=days_ago(1),
        catchup=False,
) as dag:
    
    def load_sql_from_gcs(bucket_name, file_name):
        gcs_hook = GCSHook()
        return gcs_hook.download(bucket_name, file_name)
        
    def list_gcs_files(bucket_name, prefix):
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        file_list = [blob.name for blob in blobs]
        return file_list
        
    def import_csv_to_cloud_sql(project_id, instance_id, database, bucket_name, uris, table):
        credentials, project = google.auth.default()
        print(project)
        
        service = build('sqladmin', 'v1beta4', credentials=credentials)

        for uri in uris:
            while is_operation_in_progress(service, project_id, instance_id):
                time.sleep(8)

            body = {
                'importContext': {
                    'kind': 'sql#importContext',
                    'fileType': 'CSV',
                    'uri': f"gs://{bucket_name}/{uri}",
                    'database': database,
                    'csvImportOptions': {'table': table}
                }
            }

            request = service.instances().import_(project=project_id, instance=instance_id, body=body)
            response = request.execute()
            operation_id = response['name']
            wait_for_operation_to_complete(service, project_id, instance_id, operation_id)

    def is_operation_in_progress(service, project_id, instance_id):
        request = service.operations().list(project=project_id, instance=instance_id)
        response = request.execute()
        for operation in response.get('items', []):
            if operation['status'] in ['PENDING', 'RUNNING']:
                return True
        return False

    def wait_for_operation_to_complete(service, project_id, instance_id, operation_id):
        while True:
            request = service.operations().get(project=project_id, operation=operation_id)
            response = request.execute()
            if response.get('status') == 'DONE':
                break
            time.sleep(8)

    # Task to export BigQuery table to GCS
    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="poc_bigquery_to_gcs",
        source_project_dataset_table=f"{DATASET_NAME}.{TABLE}",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/{BUCKET_FILE}"],
        compression="GZIP",
        export_format="CSV",
    )

    import_to_cloud_sql_task = PythonOperator(
        task_id='import_to_cloud_sql',
        python_callable=import_csv_to_cloud_sql,
        op_kwargs={
            'project_id': 'slb-it-op-dev',
            'instance_id': 'poc-bq-to-sql',
            'database': 'bq_to_cloudsql',
            'bucket_name': 'als_mb52',
            'uris': list_gcs_files("als_mb52", 'orng_table/'),
            'table': 'orng_activity_equipment_f'
        }
    )

    # New task to invoke Cloud Function
    invoke_cloud_function = CloudFunctionInvokeFunctionOperator(
        task_id='invoke_cloud_function',
        location='europe-west1',  # Replace with your Cloud Function location
        function_id='postgresssql',  # Replace with your Cloud Function name
        project_id='project_id',  # Replace with your GCP project ID
        input_data={}  
    )

    # Set task dependencies
    bigquery_to_gcs >> import_to_cloud_sql_task >> invoke_cloud_function
