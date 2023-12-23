import configparser
from datetime import datetime
import os

# used for datetime type operations
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id  
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

# changed to use "config.get" due to errors
os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Description:
        Creates the spark session
    Arguments:
        None
    Returns:
        spark - spark session object
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description:
        Processes song related data
    Arguments:
        spark - spark session object
        input_data - S3 path where the input data is located
        output_data - s3 path to store the data after processing 
    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(f'{output_data}songs.pq', mode='overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'name', 'location', 'latitude', 'longitude']
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(f'{output_data}artists.pq', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Description:
        Processes log related data
    Arguments:
        spark - spark session object
        input_data - S3 path where the input data is located
        output_data - s3 path to store the data after processing 
    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file 
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.drop_duplicates(subset=['userId'])

    # write users table to parquet files
    users_table.parquet(f'{output_data}users.pq', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda timestamp: timestamp / 1000.0, TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda datatime: datetime.fromtimestamp(datatime), DateType())
    df = df.withColumn('start_time', get_datetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.withColumn("hour", hour(col("start_time"))).withcolumn("day", dayofmonth(col("start_time"))) \
        .withcolumn("week", weekofyear(col("start_time"))).withcolumn("month", month(col("start_time"))) \
        .withcolumn("year", year(col("start_time"))).withcolumn("weekday", date_format(col("datetime"), 'E'))
    
    time_table = time_table.drop_duplicates(subset=['start_time'])
         
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(f'{output_data}time.pq', mode='overwrite')

    # read in song data to use for songplays table
    song_path = os.path.join(output_data, 'songs/*/*/*')
    song_df = spark.read.parquet(song_path)

    # extract columns from joined song and log datasets to create songplays table 
    # adds to the df the needed columns    
    df = df['start_time','userId', 'level', 'song', 'sessionId', 'location', 'userAgent']

    #joins with the songs dataset
    log_songs = df.join(song_df, df.song == song_df.title)

    songplays_table = log_songs.select(
        monotonically_increasing_id().alias('songplay_id'),
        col('start_time'),
        year('start_time').alias('year'),
        month('start_time').alias('month'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(f'{output_data}songplays.pq', mode='overwrite')


def main():
    """
    Description:
        Responsible for the calling the functions and is the entry point for the etl.py script
    Arguments:
        spark - spark session object
        input_data - S3 path where the input data is located
        output_data - s3 path to store the data after processing 
    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
