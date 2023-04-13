from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from s3fs import S3FileSystem
from pandas import read_csv, concat
import boto3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 6),
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

def read_new_files_from_bucket():
    # Create a boto3 session to interact with S3/AWS
    session = boto3.Session()
    s3 = session.client('s3')
    
    # Get the list of objects in the source bucket
    objects = s3.list_objects_v2(Bucket='pomodoro-sessions')['Contents']
    # this returns a dict (within a list) with the names and properties of
    # the objects in our bucket

    # It's [-2] because [-1] is the current dag run
    last_dag_run = DagRun.find(dag_id='copy_latest_file_from_s3')[-2]
    last_dag_run_date = last_dag_run.execution_date
    print('Last dag run date: ', last_dag_run_date)

    # For the objects that are more recent than the last dag run date,
    # reads them (using read_csv) and combines in them in a pandas dataframe
    dfs = []
    for obj in objects:
        print('Last modified object: ', obj['LastModified'])
        if obj['LastModified'] > last_dag_run_date:
            dfs.append(read_csv('s3://pomodoro-sessions/' + obj['Key']))
    
    if len(dfs) > 0:
        df_combined = concat(dfs, axis=0)

        # S3 key for the file that informs the date
        date_str = datetime.now().strftime('%Y-%m-%d')
        key = f'processed_{date_str}.csv'

        # write df_combined as CSV on the S3 bucket
        s3.put_object(
            Body=df_combined.to_csv(),
            Bucket='processed-pomodoro-sessions',
            Key=key)

t1 = PythonOperator(
        task_id='copy_latest_file_from_s3',
        python_callable=read_new_files_from_bucket,
        dag=dag)