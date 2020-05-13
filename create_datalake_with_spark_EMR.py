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
from pyspark.sql.functions import monotonically_increasing_id
from datetime import datetime


log_data_folder = 's3://udacity-dend/log_data/*/*/*.json'
song_data_folder = 's3://udacity-dend/song_data/*/*/*/*.json'
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

"""
Load and Transform song_data
"""
df_song = spark.read.json(song_data_folder)
df_song.createOrReplaceTempView('songs_table_DF')
songs_table = spark.sql("""
                        select song_id,
                               title,
                               artist_id,
                               year,
                               duration
                        from songs_table_DF
                        order by song_id
                        """)

now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
songs_table_path = data_output + "/songs_table" + "_" + now
songs_table.write.parquet(songs_table_path)

"""
create song plays table
"""
df_log_song_combine = df_log_song.join(df_song, (df_log_song.artist == df_song.artist_name) & (df_log_song.song == df_song.title))
df_log_song_combine = df_log_song_combine.withColumn('songplay_id', monotonically_increasing_id())

df_log_song_combine.createOrReplaceTempView('song_play_table_df')
song_play_df = spark.sql("""
          select songplay_id as songplay_id,
                 userId as user_id,
                 song_id as song_id,
                 location as location,
                 userAgent as user_agent
          from song_play_table_df
          order by user_id
          """
    )
now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
song_play_table_path = data_output + '/song_play_table' + '_' + now
song_play_df.write.parquet(song_play_table_path)