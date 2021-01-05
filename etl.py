import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Description:
    This functions creates a spark session to be used in our aws enviroment

    Arguments:
    None

    Returns:
    spark: session to be used.
    '''

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Description:
    This functions processes the song data to be read, extracted, and write

    Arguments:
    spark: Spark session in use
    input_data: The file directory where the input_data is at
    output_data: Where to write the files at

    Returns:
    None
    '''
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # read song data file
    df_songData = spark.read.json(song_data)
    df_songData.show()

    # extract columns to create songs table
    songs_table = df_songData.select('song_id', 'title', 'artist_id', 'year', 'duration')

        # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'song')

    # extract columns to create artists table
    artists_table = df_songData.select('artist_id', 'name', 'location', 'lattitude', 'longitud')

    # write artists table to parquet files
    artists_table.mode('overwrite').parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    '''
    Description:
    This functions processes the log data to be read, extracted, and write

    Arguments:
    spark: Spark session in use
    input_data: The file directory where the input_data is at
    output_data: Where to write the files at

    Returns:
    None
    '''
    # get filepath to log data file
    log_data = output_data + "log_data/*/*/*.json"

    # read log data file
    df_logData = spark.read.json(log_data)

    # filter by actions for song plays
    df_logData = df_logData.filter(df_logData.page == 'NextSong')

    # extract columns for users table
    artists_table = df_logData.select('user_id', 'first_name', 'last_name', 'gender', 'level' ,'ts').dropDuplicates()

    # write users table to parquet files
    artists_table.mode('overwrite').parquet(output_data, 'artists')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda time: datetime.fromtimestamp(time/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_logData = df_logData.withColumn('timestamp', get_timestamp(df_logData.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda date: datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d'))
    df_logData = df_logData.withColumn('datetime', get_datetime(df_logData.ts))

    # extract columns to create time table
    time_table = df_logData.select('timestamp', 'hour(timestamp) as hour', 'day(timestamp) as day',
                            'week(timestamp) as week', 'month(timestamp) as month',
                            'year(timestamp) as year', 'weekday(timestamp) as weekday')

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df_logData.join(song_df, df_logData.artist == song_df.artist_name).select('songplay_id', 'ts',
                        'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('songplay_id', 'user_id').parquet(output_data + 'songplays')


def main():
    '''
    Description:
    Main function where the data is passed intot the functions and the session is created.

    Arguments:
    None

    Returns:
    None
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacityemrtest/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()