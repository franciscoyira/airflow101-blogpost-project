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
from airflow.hooks.postgres_hook import PostgresHook

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
    dag_id='etl-pomodoro',
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

    # Rename columns
    df_pivoted = df_pivoted.rename(
       columns={'Learning': 'learning_minutes',
                'Work': 'work_minutes'}
       )

    return df_pivoted

# Define the function that performs the upsert
def upsert_df_to_rds(**kwargs):
    # Get the dataframe from the previous task using xcom
    df = kwargs['ti'].xcom_pull(task_ids='pivoting_df')
    
    # Get the RDS connection using PostgresHook
    rds_hook = PostgresHook(postgres_conn_id='database-airflow-blogpost')
    
    # Get the table name and schema from the parameters
    table = kwargs['params']['table']
    schema = kwargs['params']['schema']
    
    # Create a temporary table with the same structure as the target table
    rds_hook.run(f"CREATE TEMPORARY TABLE tmp_{table} (LIKE {schema}.{table});")
    
    # Insert the dataframe into the temporary table using to_sql method
    df.to_sql(
       f"tmp_{table}",
       rds_hook.get_sqlalchemy_engine(),
       schema=schema,
       if_exists='replace',
       index=True,
       index_label='date')
    
    # Perform the upsert by merging the temporary table with the target table on the date column
    rds_hook.run(f"""
        INSERT INTO {schema}.{table}
        SELECT * FROM tmp_{table}
        ON CONFLICT (date) DO UPDATE SET
        work_minutes = EXCLUDED.work_minutes,
        learning_minutes = EXCLUDED.learning_minutes;
    """)
    
    # Drop the temporary table
    rds_hook.run(f"DROP TABLE tmp_{table};")

# Define the task that performs the upsert using PythonOperator
upsert_task = PythonOperator(
    task_id='upsert_df_to_rds',
    python_callable=upsert_df_to_rds,
    params={
        'table': 'pomodoro_day_catg',
        'schema': 'public'
    },
    provide_context=True,
    dag=dag,
)

read_combines_new_files_from_s3 = PythonOperator(
        task_id='read_combines_new_files_from_s3',
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

# define the end_task as a DummyOperator
end_task = DummyOperator(
  task_id='end_task',
  dag=dag
)

# set the dependencies
read_combines_new_files_from_s3 >> branch_task
branch_task >> pivoting_df >> upsert_task
branch_task >> end_task
