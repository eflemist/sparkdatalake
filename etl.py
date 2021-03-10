import configparser
#import findspark
#findspark.init()

#from datetime import datetime
import datetime
import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import pandas as pd


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS","AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS","AWS_SECRET_ACCESS_KEY")


def create_spark_session():

    spark = SparkSession \
        .builder \
        .appName("Spark DataLake Standalone - Python") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ loads the events data from s3, performs data wrangling and writes output back to S3 as parquet files
        data files outputed contain song and artist details 
    """
    # get filepath to song data file
    #song_data = input_data + "song_data\*\*\*"
    song_data = input_data + "song_data/*/*/*"
    
    # read song data file
    df = spark.read.format("json").load(song_data)
    
    # create sql table from data
    df.createOrReplaceTempView("songs_data_tbl")

    # extract columns to create songs table
    songs_table = spark.sql("""SELECT sdt.song_id,
                                      sdt.title,
                                      sdt.artist_id,
                                      sdt.year,
                                      sdt.duration
                                 FROM songs_data_tbl sdt     
                             """)
    
    songs_table = songs_table.distinct()
    
    #print(songs_table.show())
    # write songs table to parquet files partitioned by year and artist
    song_tbl_file = output_data + "song_table.parquet"
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(song_tbl_file)

    # extract columns to create artists table
    artists_table = spark.sql("""SELECT sdt.artist_id,
                                      sdt.artist_name,
                                      sdt.artist_location,
                                      sdt.artist_latitude,
                                      sdt.artist_longitude
                                 FROM songs_data_tbl sdt     
                             """)
    
    artists_table = artists_table.distinct()
    
    # write artists table to parquet files
    artist_tbl_file = output_data + "artist_table.parquet"
    artists_table.write.mode('overwrite').parquet(artist_tbl_file)


def process_log_data(spark, input_data, output_data):
    """ loads the events data from s3, performs data wrangling and writes output back to S3 as parquet files
        data files outputed contain user, time and songplay details 
    """
    # get filepath to log data file
    #log_data = input_data + "log-data\*"
    log_data = input_data + "log_data/*/*"

    # read log data file
    df = spark.read.format("json").load(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    # create sql table from data
    df.createOrReplaceTempView("logs_data_tbl")


    # extract columns for users table    
    users_table = spark.sql("""SELECT DISTINCT ldt.userId,
                                      ldt.firstName,
                                      ldt.lastName,
                                      ldt.gender,
                                      ldt.level
                                 FROM logs_data_tbl ldt     
                             """)
    
    # write users table to parquet files
    user_tbl_file = output_data + "users_table.parquet"
    users_table.write.mode('overwrite').parquet(user_tbl_file)
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn("date", get_datetime(df.ts)) 
    df_dtime_pd = df.select(["ts","date"]).toPandas()
    df_dtime_pd['ts'] = pd.to_datetime(df_dtime_pd['ts'], unit='ms')
    df_dtime_pd['year'] = df_dtime_pd['ts'].dt.year
    df_dtime_pd['month'] = df_dtime_pd['ts'].dt.month
    df_dtime_pd['week'] = df_dtime_pd['ts'].dt.week 
    df_dtime_pd['weekday'] = df_dtime_pd['ts'].dt.weekday
    df_dtime_pd['day'] = df_dtime_pd['ts'].dt.day
    df_dtime_pd['hour'] = df_dtime_pd['ts'].dt.hour

    # create spark dataframe from pandas df
    df_dtime_sp = spark.createDataFrame(df_dtime_pd)
    
    # create sql table from spark dataframe
    df_dtime_sp.createOrReplaceTempView("times_data_tbl")
    
    # extract columns to create time table    
    times_table = spark.sql("""SELECT tdt.date as start_time,
                                      tdt.year,
                                      tdt.month,
                                      tdt.week,
                                      tdt.weekday,
                                      tdt.day,
                                      tdt.hour
                                 FROM times_data_tbl tdt     
                             """)
    
    times_table = times_table.distinct()
    
    # write time table to parquet files partitioned by year and month
    time_tbl_file = output_data + "times_table.parquet"
    times_table.write.partitionBy("year","month").mode('overwrite').parquet(time_tbl_file)
    
    # read in song data to use for songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    
    songplay_df = df['date','userId','level','song','artist','sessionId','location','userAgent']
    songplay_rdd_df = songplay_df.rdd.zipWithIndex()
    songplay_df = songplay_rdd_df.toDF()

    songplay_df = songplay_df.withColumn('start_time', songplay_df['_1'].getItem("date"))
    songplay_df = songplay_df.withColumn('user_id', songplay_df['_1'].getItem("userId"))   
    songplay_df = songplay_df.withColumn('level', songplay_df['_1'].getItem("level"))    
    songplay_df = songplay_df.withColumn('song', songplay_df['_1'].getItem("song"))
    songplay_df = songplay_df.withColumn('artist', songplay_df['_1'].getItem("artist"))
    songplay_df = songplay_df.withColumn('session_id', songplay_df['_1'].getItem("sessionId"))
    songplay_df = songplay_df.withColumn('location', songplay_df['_1'].getItem("location"))    
    songplay_df = songplay_df.withColumn('user_agent', songplay_df['_1'].getItem("userAgent"))
    
    final_song_df = songplay_df['_2','start_time','user_id','level','song','artist','session_id','location','user_agent']
    final_song_df = final_song_df.withColumnRenamed("_2", "songplay_id")
    
    # create sql table from spark dataframe
    final_song_df.createOrReplaceTempView("songplays_data_tbl")
                                 
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT spdt.songplay_id,
                                      spdt.start_time,
                                      spdt.user_id,
                                      spdt.level,
                                      sdt.song_id,
                                      sdt.artist_id,
                                      spdt.session_id,
                                      spdt.location,
                                      spdt.user_agent,
                                      tdt.year,
                                      tdt.month 
                                 FROM songplays_data_tbl spdt
                                 JOIN songs_data_tbl sdt
                                   ON spdt.artist = sdt.artist_name and spdt.song = sdt.title
                                 JOIN times_data_tbl tdt
                                   ON spdt.start_time = tdt.date    
                             """)
 
    songplays_table = songplays_table.distinct()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_tbl_file = output_data + "songplays_table.parquet"
    songplays_table.write.partitionBy("year","month").mode('overwrite').parquet(songplays_tbl_file)

def main():
    spark = create_spark_session()
    
#    input_data = "data\\"
#    output_data = "output\\"

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://eflemist-dend/analytic/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
    

