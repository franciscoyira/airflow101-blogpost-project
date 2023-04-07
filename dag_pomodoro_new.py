from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import boto3
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 11),
    'email_on_failure': 'francisco.yira@outlook.com',
    'email_on_retry': 'francisco.yira@outlook.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# Instantiate the DAG
dag = DAG(
    dag_id='pivot_and_upsert_new_data',
    default_args=default_args,
    # this means the DAG will run every day at the same time as start date
    schedule=timedelta(days=1)
)

# Defines custom poke that checks for files modified after the last dag run
def custom_poke(self, context):
    # Get the last DAG run time from the context
    last_dag_run = context.get('dag_run').execution_date
    # Get the S3 client from the connection
    s3 = self.get_hook().get_conn()
    # List the objects in the bucket with the prefix
    response = s3.list_objects_v2(
        Bucket=self.bucket_name,
        Prefix=self.bucket_key
    )
    # Loop through the objects and check their last modified time
    for obj in response.get('Contents', []):
        if obj['LastModified'] > last_dag_run:
            # Found a file that was modified after the last DAG run
            return True
    # No file was modified after the last DAG run
    return False

# Create the sensor with the custom poke method
t0 = S3KeySensor(
    task_id='check_modified_files_last_dag_run',
    # OK, it appears on the documentation
    bucket_key='pomodoro-sessions/*',
    # Also OK
    bucket_name='pomodoro-sessions',
    # OK
    wildcard_match=True,
    # OK in the documentation
    # but I don't know if it's correctly configured
    aws_conn_id='aws_default',
    # Warning: all of the following are not in the documentation
    dag=dag,
    poke_interval=60, # Check every 60 seconds
    timeout=200,
    soft_fail=True,
    poke_method=custom_poke # Use the custom function
)


def copy_latest_file_from_s3():
    # Create a boto3 session to interact with S3/AWS
    session = boto3.Session()
    s3 = session.client('s3')
    
    # Get the list of objects in the source bucket
    objects = s3.list_objects_v2(Bucket='pomodoro-sessions')['Contents']
    # this returns a dict (within a list) with the names and properties of
    # the objects in our bucket

    #  Find the latest modified object
    latest_object = max(objects, key=lambda x: x['LastModified'])
    
    # Copy the object to the destination bucket
    s3.copy_object(
        CopySource={'Bucket': 'pomodoro-sessions',
        'Key': latest_object['Key']},
        Bucket='processed-pomodoro-sessions',
        Key=latest_object['Key']
        )

t1 = PythonOperator(
        task_id='copy_latest_file_from_s3',
        python_callable=copy_latest_file_from_s3,
        dag=dag)