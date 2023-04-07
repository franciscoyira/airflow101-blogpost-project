from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 11),
    'email_on_failure': 'francisco.yira@outlook.com',
    'email_on_retry': 'francisco.yira@outlook.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

# Instantiate the DAG
dag = DAG(
    dag_id='copy_latest_file_from_s3',
    default_args=default_args,
    # this means the DAG will run every day at the same time as start date
    schedule="@weekly"
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