import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, monotonically_increasing_id



#config = configparser.ConfigParser()
#config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']='AKIAZI5LRGSM2HNQBPF6'
os.environ['AWS_SECRET_ACCESS_KEY']='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'


def create_spark_session():
      
    """
    This function creates a spark session
   
    """
    
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
    This function creates the artist and song parquet S3 files based on the 
    arguments:
    -spark = current spark session
    -input_data: the S3 source bucket
    -output_data: the S3 destination bucket
   
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_df.select(\
                'song_id', 'title', 'artist_id','year', 'duration').dropDuplicates()
    
    print('Step 1 / 11 - Done selecting the Songs data') 
    # write songs table to parquet files partitioned by year and artist
 
    songs_table.write.partitionBy('year', 'artist_id') \
                     .parquet(os.path.join(output_data, \
                     'songs/songs.parquet'), 'overwrite')

    print('Step 2 / 11 - Done Writing the parquet Songs data')
    
    # extract columns to create artists table
    

    artists_table = song_df.select['artist_id', 'artist_name', 
                            'artist_location','artist_latitude',
                            'artist_longitude'].drop_duplicates()
    
    artist_table.withColumnRenamed('artist_name', 'name') \
                .withColumnRenamed('artist_location', 'location') \
                .withColumnRenamed('artist_latitude', 'latitude') \
                .withColumnRenamed('artist_longitude', 'longitude')
    
    print('Step 3 / 11 - Done selecting the Artist data') 
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, \
                               'artists.parquet'), 'overwrite')
    
    print('Step 4 / 11 - Done Writing the parquet Artist data')

def process_log_data(spark, input_data, output_data):
    
    """
    This function creates the users, time and songplay 
    parquet S3 files based on the arguments:
    -spark = current spark session
    -input_data: the S3 source bucket
    -output_data: the S3 destination bucket
   
    """
    
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    users_table = log_df.select('userId', 'firstName', 'lastName',\
                            'gender', 'level').dropDuplicates()
    
    print('Step 5 / 11 - Done selecting the Users data') 
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    
    print('Step 6 / 11 - Done Writing the parquet Users data')

    # create timestamp column from original timestamp column
    udf_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', udf_timestamp(log_df.ts))
    
    # create datetime column from original timestamp column
    udf_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    log_df = log_df.withColumn('datetime', udf_datetime(log_df.ts))
    
    # extract columns to create time table
    time_table = log_df.select('datetime') \
                       .withColumn('start_time', log_df.datetime) \
                       .withColumn('hour', hour('datetime')) \
                       .withColumn('day', dayofmonth('datetime')) \
                       .withColumn('week', weekofyear('datetime')) \
                       .withColumn('month', month('datetime')) \
                       .withColumn('year', year('datetime')) \
                       .withColumn('weekday', dayofweek('datetime')) \
                       .dropDuplicates()
    
    print('Step 7 / 11 - Done selecting and converting the time data') 
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month') \
                    .parquet(os.path.join(output_data,'time/time.parquet'),\
                    'overwrite')

    print('Step 8 / 11 - Done Writing the parquet Time data')
    
    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    song_df = spark.read.json(song_data)
    song_df = song_df.select('song_id','artist_id', 'artist_name').dropDuplicates()
    
    # extract columns from joined song and log datasets to create songplays table 
  
    song_log_df = log_df.join(song_df, col('log_df.artist') \
                == col('song_df.artist_name'), 'inner')
    
    print('Step 9 / 11 - Done Joining song and log data')

    songplays_table = song_log_df.select(
            monotonically_increasing_id().alias('songplay_id'),
            col('log_df.datetime').alias('start_time'),
            col('log_df.userId').alias('user_id'),
            col('log_df.level').alias('level'),
            col('song_df.song_id').alias('song_id'),
            col('song_df.artist_id').alias('artist_id'),
            col('log_df.sessionId').alias('session_id'),
            col('log_df.location').alias('location'), 
            col('log_df.userAgent').alias('user_agent'),
            year('log_df.datetime').alias('year'),
            month('log_df.datetime').alias('month'))
        
    print('Step 10 / 11 - Done selecting the Songplay data') 
    
    # write songplays table to parquet files partitioned by year and month


    songplays_table.write.partitionBy('year', 'month')\
                    .parquet(os.path.join(output_data,\
                    'songplays/songplays.parquet'),'overwrite')
    
    print('Step 11 / 11 - Done Writing the parquet Songplay data')

def main():
    
        
    """
    
    - Creates a Spark session.
    
    - Executes the copy of the song data into the data lake as parquet files.
    
    - Executes the copy of the log data into the data lake as parquet files.
    

    """
    
    print('CREATING SESSION')
    spark = create_spark_session()
    input_data = "s3n://ancasource/"
    output_data = "s3n://ancadest/"
    
    print('SESSION CREATED')
    print('STARTED ETL')
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    
    print('ETL DONE!')


if __name__ == "__main__":
    main()

    
