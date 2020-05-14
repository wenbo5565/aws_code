from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hook.S3_hook import S3HOOK

import datetime
import boto3
import pandas as pd
from io import StringIO

#####################
# set AWS parameters
#####################
aws_access_key_id = ''
aws_access_key = ''
bucket = ''

######################
# create a data frame
######################
def create_data_frame():
    df = pd.DataFrame({'sequence': range(1,10)})
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    
    bucket = bucket
    s3 = boto3.resource('s3', region_name = 'us-west-2', 
                    aws_access_key_id = aws_access_key_id,
                    aws_secret_access_key = aws_access_key)
    
    s3.Object(bucket, 'airflow_test_from_udacity.csv').put(Body = csv_buffer.getvalue())


dag = DAG(
          'create_df_and_dump_to_s3',
           start_date = datetime.datetime.now())

create_df_dump_to_s3 = PythonOperator(
        task_id = 'create_df_and_dump_to_s3',
        python_callable = create_data_frame,
        dag = dag
)
        
