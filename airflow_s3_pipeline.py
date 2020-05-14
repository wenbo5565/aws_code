########################################
# use airflow to set up a pipeline
# task 1: create a python dataframe use pandas
# task 2: send the dataframe to a s3 bucket on AWS
########################################


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.S3_hook import S3Hook
import datetime
import boto3
import pandas as pd
from io import StringIO
import logging

#####################
# set AWS parameters
#####################
# AWS parameters are now saved in 'Variable' and 'Connection' in Airflow
#aws_access_key_id = ''
#aws_access_key = ''
#bucket = ''

######################
# create a data frame
######################
def create_df(**kwargs):
    df = pd.DataFrame({'sequence': range(1, kwargs['sequence_end'])})
    return df
   
def save_to_s3(**kwargs):
    hook = S3Hook(aws_conn_id = 'aws_credential_wenbo') # connect to s3 use saved credentials
    bucket_name = Variable.get('s3_bucket_wenbo') # get a bucket name from a saved variable
    csv_buffer = StringIO()
    df = kwargs['task_instance'].xcom_pull(task_ids = 'create_df') # take parameters from upstream task
    df.to_csv(csv_buffer)
    s3 = hook.get_bucket(bucket_name) # get a s3 bucket
    s3.put_object(Body = csv_buffer.getvalue(), Key = 'air-flow-test.csv') # put the object to s3
    
dag = DAG(
          'create_df_and_dump_to_s3',
           start_date = datetime.datetime.now(),
           schedule_interval = '@daily')

create_df = PythonOperator(
        task_id = 'create_df',
        python_callable = create_df,
        op_kwargs = {'sequence_end': 10},
        dag = dag
)

save_to_s3 = PythonOperator(
        task_id = 'save_to_s3',
        provide_context = True,
        python_callable = save_to_s3,
        dag = dag
)

create_df >> save_to_s3 # set task dependency
