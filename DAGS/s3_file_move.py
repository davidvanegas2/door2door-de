from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
import logging

glue_job_name = 'door2door_notebook'

# Define the source and destination buckets and prefix
source_bucket = 'de-tech-assessment-2022'
source_prefix = 'data/'
destination_bucket = 'door2door-de'
destination_prefix = 'data/'
bucket_glue = 'aws-glue-assets-719386081370-us-east-1'


def move_files(ds, **kwargs):
    logging.info('Starting move_files task...')
    print(f"HELLO LOKA WORLD ---------------------------------")

    # Get the previous day's date
    execution_date = datetime.strptime(ds, '%Y-%m-%d')
    prev_day = execution_date - timedelta(days=1)
    date_str = prev_day.strftime('%Y-%m-%d')
    date_str = datetime.strptime('2019-06-01', '%Y-%m-%d').strftime('%Y-%m-%d')

    # Define the source and destination keys
    source_key = f"{source_prefix}{date_str}*"

    print(f'-- The date_str is {date_str}')
    print(f'-- The source key is {source_key}')

    # Copy the data from the source bucket to the destination bucket
    s3_hook = S3Hook()
    keys = s3_hook.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    for key in keys:
        print(f'-- The key is {key}')
        destination_key = f"{destination_prefix}{date_str}/{key.split('/')[-1]}"
        if key.startswith(source_prefix + date_str):
            s3_hook.copy_object(
                source_bucket_name=source_bucket,
                source_bucket_key=key,
                dest_bucket_name=destination_bucket,
                dest_bucket_key=destination_key,
            )
            print(f"----- Successfully copied {key} to {destination_key}")

    print('----- move_files task complete.')


default_args = {
    'owner': 'David Vanegas',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 9),
    'email': ['davidfvanegas@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'door2door_pipeline',
    default_args=default_args,
    description='Copy data from source S3 bucket to target S3 bucket on a daily basis',
    schedule_interval='@daily',
)

move_files_operator = PythonOperator(
    task_id='move_files',
    provide_context=True,
    python_callable=move_files,
    dag=dag
)

# Define the GlueOperator to run the AWS Glue job
process_data = GlueJobOperator(
    task_id='process_data',
    job_name=glue_job_name,
    region_name='us-east-1',
    script_location=f's3://{bucket_glue}/scripts/door2door_notebook.py',
    script_args={"--day": "2019-06-01"},
    create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 5, "WorkerType": "G.1X"},
    s3_bucket=bucket_glue,
    iam_role_name='role_glue_s3',
    # depends_on_past=True,
    dag=dag
)

run_command = BashOperator(
    task_id='run_command',
    bash_command='echo "Hello, Airflow!"',
    dag=dag
)

run_command >> move_files_operator >> process_data