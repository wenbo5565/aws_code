"""
1. import JSON file from directories on S3 buckets.

2. use PySpark (DataFrame and Spark SQL) to join and transform data
   into desired format.

3. output to desired data to S3 directory as Parquet.

4. the files in the Parquet directory works as a data lake

5. The script can be run in the master node of a EMR Spark/Hive cluster
   with command 'spark-submit --master yarn this_file_name.py'
"""

from pyspark.sql import SparkSession
from datetime import datetime


log_data_folder = 's3://udacity-dend/log_data/*/*/*.json'
data_output = 's3://wenbo-udacity-eng/data_lake_output'


spark = SparkSession.builder.appName('spark_datalake').getOrCreate()

"""
Load and Transform log Data
"""
df_log = spark.read.json(log_data_folder)
df_log_song = df_log.filter(df_log.page == 'NextSong') # get log related to song plays
df_log_song.createOrReplaceTempView('user_table_DF')
users_table = spark.sql("""
                       select userID as user_id,
                              firstName as first_name,
                              LastName as last_name,
                              gender,
                              level
                       from user_table_DF
                       order by last_name
                      """) # extract distinct users and related columns
now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
users_table_output = data_output + '/users_table_parquet' + '_' + now

users_table.write.parquet(users_table_output)