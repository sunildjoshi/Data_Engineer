import configparser
from datetime import datetime
import os
import boto3
import botocore
from pyspark.sql.types import TimestampType,DateType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: This function creates spark session  
    Arguments:
        None 
    Returns:
        spark session object 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def get_all_files(folder,bucket,s3):
    """
    Description: This function list all json files present at S3 bucket subfolder  
    Arguments:
        s3: the S3 client object. 
        bucket: S3 bucket name.
        folder: folder under the S3 bucket where json files are to be searched
    Returns:
        None 
    """
    all_files = []
    for key in s3.list_objects(
        Bucket=bucket,Prefix=folder)['Contents']:
        if key['Key'].endswith('.json'):
             all_files.append('s3a://{}/{}'.format(bucket,key['Key']))
    return all_files


def process_song_data(spark, s3, input_data, output_data):
    """
    Description: This function loads songs data from S3 process songs and artist table and saves them in parquet format  
    Arguments:
        spark: the spark session object. 
        conn: the s3 client object.
        input_data: bucket where song data is present
        output_data: path where artist and song table are required to be saved
    Returns:
        None 
    """
    # Foldername to song data file
    song_data = 'song_data'
    
    # get all files matching json extension from directory
    all_files=get_all_files(song_data, input_data, s3)
    
    # reading song data file
    df = spark.read.json(all_files)
    df.createOrReplaceTempView("temp")
    
    # extracting columns to create songs table
    songs_table = spark.sql('''SELECT song_id, title, artist_id, year, duration 
                               FROM (SELECT song_id, title, artist_id, 
                               cast(year as integer) as year,
                               cast(duration as float) as duration,
                               row_number () OVER
                               (PARTITION BY song_id ORDER BY year DESC) AS rownum 
                               FROM temp) groups where rownum = 1''')
    
    # writing songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(output_data+"songs.parquet")
    
    # extracting columns to create artists table
    artists_table = spark.sql('''SELECT artist_id, name, location, latitude, longitude 
                                 FROM (SELECT artist_id, year,
                                 artist_name as name, 
                                 artist_location as location,
                                 cast(artist_latitude as float) as latitude, 
                                 cast(artist_longitude as float) as longitude, 
                                 row_number () OVER
                                 (PARTITION BY artist_id ORDER BY year DESC) AS rownum 
                                 FROM temp) groups where rownum = 1''')
    
    # writing artists table to parquet files
    artists_table.write.partitionBy("artist_id").mode('overwrite').parquet(output_data+"artist.parquet")
    

def process_log_data(spark, s3,input_data, output_data):
    """
    Description: This function loads songs data from S3 process songs and artist table and saves them in parquet format  
    Arguments:
        spark: the spark session object. 
        conn: the s3 client object.
        input_data: bucket where song data is present
        output_data: path where artist and song table are required to be saved
    Returns:
        None 
    """
    # Foldername at to log data file
    log_data = 'log_data'
    
    # geting all files matching extension from directory
    all_files=get_all_files(log_data, input_data, s3)
    
    # reading log data file
    df = spark.read.json(all_files)
    
    # filter by actions for song plays
    df = df.filter((col('page')=='NextSong')&(col("ts").isNotNull())&
                   (col("userId").isNotNull())&(col("sessionId").isNotNull()))
    users_table = df.select('userId','firstName','lastName','gender','level','ts').withColumn(
        'userId',df.userId.cast('integer'))
    users_table.createOrReplaceTempView("users")
    users_table=spark.sql('''SELECT userId as user_id, firstName as first_name,lastName last_name, gender, level 
                       FROM (
                           SELECT userId, firstName, lastName, gender, level,ts,
                           ROW_NUMBER () OVER 
                           (PARTITION BY userId ORDER BY ts DESC) AS rownum
                           FROM users) groups
                       WHERE groups.rownum = 1''')
    
    # writing users table to parquet files
    users_table.write.partitionBy("user_id").mode('overwrite').parquet(output_data+"users.parquet")
    
    # creating timestamp column from original timestamp column
    get_timestamp = udf(lambda ts : datetime.fromtimestamp(ts / 1e3),TimestampType())
    df = df.withColumn('start_time',get_timestamp('ts'))
    
    # creating datetime column from original timestamp column
    get_datetime = udf(lambda ts : datetime.date(ts),DateType())
    df = df.withColumn('date',get_datetime('start_time'))
    df = df.withColumn('year', year('date')).withColumn('month',  month('date'))
    
    # extracting columns to create time table
    time_table = df.select('start_time','date','month','year').distinct()
    time_table=time_table.withColumn('day', dayofmonth('date')).withColumn(
                                    'hour', hour('date')).withColumn(
                                    'week', weekofyear('date')).withColumn(
                                    'weekday',date_format('date', "EEEE")).drop('date')
    df=df.drop('date')
    
    # writing time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"time.parquet")
    
    # reading in song data to use for songplays table
    song_df = spark.read.parquet(output_data+"songs.parquet")
    song_df.createOrReplaceTempView("songs")
    df.createOrReplaceTempView('songplays')
    song_df = spark.sql('''SELECT title, song_id, artist_id FROM (
                           SELECT title, song_id, artist_id, row_number () OVER
                           (PARTITION BY title ORDER BY song_id DESC) AS rownum 
                           FROM songs) groups where rownum = 1''')
    song_df.createOrReplaceTempView("songs")
    
    # extracting columns from joined song and log datasets to create songplays table 
    songplays_table=spark.sql('''SELECT start_time, year, month, level, location, song_id, artist_id, 
                                 userId as user_id, sessionId as session_id, userAgent as user_agent
                                 FROM songplays LEFT JOIN songs ON (song=title)''')
  
    # writing songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(output_data+"songplays.parquet")

    
def main():
    """
    Description: This function creates spark session and load Song and Log data  
    Arguments:
        None
    Returns:
        None 
    """
    spark = create_spark_session()
    output_data="/home/workspace/data/"
    input_data = "udacity-dend"
    s3 = boto3.client('s3', config=botocore.client.Config(signature_version=botocore.UNSIGNED))  
    process_song_data(spark,s3, input_data, output_data)    
    process_log_data(spark,s3, input_data, output_data)


if __name__ == "__main__":
    main()
