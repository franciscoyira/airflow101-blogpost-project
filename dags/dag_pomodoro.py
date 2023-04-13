# standard library imports
from datetime import datetime, timedelta

# related third party imports
import boto3
from pandas import read_csv, concat, pivot_table, to_datetime
from s3fs import S3FileSystem

# local application/library specific imports
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

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

def read_combines_new_files_from_s3(**kwargs):
    # Create a boto3 session to interact with S3/AWS
    session = boto3.Session()
    s3 = session.client('s3')
    
    # Get the list of objects in the source bucket
    objects = s3.list_objects_v2(Bucket='pomodoro-sessions')['Contents']
    # this returns a dict (within a list) with the names and properties of
    # the objects in our bucket

    # Get the date of the last dag run
    # It's [-2] because [-1] is the current dag run
    last_dag_run = DagRun.find(dag_id='copy_latest_file_from_s3')[-2]
    last_dag_run_date = last_dag_run.execution_date
    print('Last dag run date: ', last_dag_run_date)

    # For the objects that are more recent than the last dag run date,
    # reads them (using read_csv) and combines in them in a pandas dataframe
    dfs = []
    df_combined = None
    for obj in objects:
        print('Last modified object: ', obj['LastModified'])
        if obj['LastModified'] > last_dag_run_date:
            dfs.append(read_csv('s3://pomodoro-sessions/' + obj['Key']))
    
    if len(dfs) > 0:
        df_combined = concat(dfs, axis=0)

    return df_combined

def branch_function(**kwargs):
  ti = kwargs['ti']
  # get the dataframe from xcom
  df = ti.xcom_pull(task_ids='read_combines_new_files_from_s3')
  # check if the dataframe is None
  if df is None:
    # return the task_id of the end task to skip the downstream tasks
    return 'end_task'
  else:
    # return the task_id of the next task to execute
    return 'pivoting_df'


def pivoting_df(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='read_combines_new_files_from_s3')

    df = df.drop('End date', axis=1)

    # Converting the column 'Start date' to date
    df['Start date'] = df['Start date'].str[:10]

    df['Start date'] = to_datetime(
        df['Start date'],
        format='%Y-%m-%d').dt.date

    df_pivoted = pivot_table(
       data=df,
       values="Duration (in minutes)",
       index="Start date",
       columns="Project",
       fill_value= 0
    )

    return df_pivoted

def write_to_final_destination(**kwargs):
    ti = kwargs['ti']
    # Create a boto3 session to interact with S3/AWS
    session = boto3.Session()
    s3 = session.client('s3')
   
    # S3 key for the file that informs the date
    date_str = datetime.now().strftime('%Y-%m-%d')
    key = f'processed_{date_str}.csv'

    # write df_combined as CSV on the S3 bucket
    df = ti.xcom_pull(task_ids='pivoting_df')
    s3.put_object(
       Body=df.to_csv(),
       Bucket='processed-pomodoro-sessions',
       Key=key)

read_combines_new_files_from_s3 = PythonOperator(
        task_id='copy_latest_file_from_s3',
        python_callable=read_combines_new_files_from_s3,
        dag=dag)

branch_task = BranchPythonOperator(
  task_id='branch_task',
  python_callable=branch_function,
  provide_context=True,
  dag=dag
)

pivoting_df = PythonOperator(
  task_id='pivoting_df',
  python_callable=pivoting_df,
  provide_context=True,
  dag=dag
)

write_to_final_destination = PythonOperator(
    task_id='write_to_final_destination',
    python_callable=write_to_final_destination,
    provide_context=True,
    dag=dag
)

# define the end_task as a DummyOperator
end_task = DummyOperator(
  task_id='end_task',
  dag=dag
)

# set the dependencies
read_combines_new_files_from_s3 >> branch_task
branch_task >> pivoting_df >> write_to_final_destination
branch_task >> end_task
