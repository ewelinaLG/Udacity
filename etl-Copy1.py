import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, to_timestamp, dayofweek
# ELG
from zipfile import ZipFile
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    #song_data =  input_data + '/song_data/*/*/*/*.json'
    song_data_medium = input_data + '/song_data/A/A/*/*.json'
    #1 song_data_few = input_data + '/song_data/A/A/A/*.json'
    # read song data file
    #1 df = spark.read.json(song_data_few)
    df = spark.read.json(song_data_medium)
    df.printSchema()
    #print(df.take(2))
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    # songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + '/song')
    # songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data, '/song/songs.parquet'))
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data, 'song/'))
    # songplays_table.write.partitionBy(['year', 'month']).parquet(output_data + "songplays_table.parquet")
    #songs_table.createOrReplaceTempView("song")
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + '/artist')
    #songs_table.write.mode("overwrite").parquet(output_data + '/song')
    #songs_table.write.mode("overwrite").parquet(os.path.join(output_data, '/song/songs.parquet'))
    # songs_table.write.mode("overwrite").parquet(output_data + '/song')
    print("Artists done")
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + '/log_data/2018/11/2018-11-12-events.json'
    #print(log_data)
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df["page"] == "NextSong")
    # extract columns for users table    
    artists_table = df.dropDuplicates()
    user_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    # user_table = user_table.drop_duplicates(subset=['userId'])
    user_table.write.mode("overwrite").parquet(output_data + '/user')
    # create timestamp column from original timestamp column
    df = df.withColumn('ts_str', from_unixtime(df.ts / 1000, "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn("start_time", to_timestamp(df.ts_str, "yyyy-MM-dd HH:mm:ss"))
    df = df.withColumn('ts_to_timestamp', to_timestamp(from_unixtime(df.ts / 1000, "yyyy-MM-dd HH:mm:ss")))
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn('datetime', from_unixtime(df.ts / 1000).cast('date'))
    #print(df.printSchema())
    #print(df.head(1))
    # Adding required columns
    df = df.withColumn('year', year(df['start_time']))
    df = df.withColumn('month', month(df['start_time']))
    df = df.withColumn('day', dayofmonth(df['start_time']))
    df = df.withColumn('week', weekofyear(df['start_time']))
    df = df.withColumn('hour', hour(df['start_time']))
    df = df.withColumn('weekday', dayofweek(df['start_time']))
    
    # extract columns to create time table
    time_table = df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + '/time')
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, '/song/songs.parquet'))
    
    #song_df = spark.read.parquet('./output/song/*.parquet')
    # song_df = spark.read.parquet('./output/song/*.parquet')
    # songs_table = spark.read.parquet('./output/song/*.parquet')
    artist_df = spark.read.parquet('./output/artist/*.parquet')
     #time_df = spark.read.parquet('./output/time/*.parquet')
    log_df = df
    
    # extract columns from joined song and log datasets to create songplays table 
     # log: song
    # song: title
    # song_df.createOrReplaceTempView("song")
    log_df.createOrReplaceTempView("log_df")
    artist_df.createOrReplaceTempView("artist")
    time_table.createOrReplaceTempView("time_table")
    # print(song_df.filter(song_df["title"]=="Drop of Rain").show())
    # print(log_df.filter(log_df["song"]=="Drop of Rain").select('song').show())
    # songplays_table = spark.sql('''SELECT DISTINCT l.start_time, l.userId, l.level, s.song_id, a.artist_id, l.sessionId AS sesssion_id, l.location, l.userAgent as user_agent 
    # FROM log AS l
    # LEFT JOIN song AS s
    # ON s.title == l.song AND s.duration == l.length
    # LEFT JOIN artist as a
    # ON l.artist == a.artist_name
    # ''')
    #songplays_table = spark.sql("SELECT e.start_time,\
     #           e.userId, e.level, s.song_id,\
      #          s.artist_id, e.sessionId, e.userAgent, \
       #         t.year, t.month \
        #  FROM log_df e \
         # JOIN song s ON e.song = s.title AND e.length = s.duration \
          #JOIN artist a ON e.artist = a.artist_name AND a.artist_id = s.artist_id \
          #JOIN time_table t ON e.start_time = t.start_time ")
    
    songplays_table = df.join(artist_df,df.artist == artist_df.artist_name).\
                        join(song_df,song_df.title == df.song).\
                        join(time_table,df.start_time == time_table.start_time).\
    select(df.start_time,
             df.userId.alias("user_id"),
             df.level,
             song_df.song_id,
             song_df.artist_id,
             df.sessionId.alias('session_id'),
             col("artist_location").alias("location"),
             df.userAgent.alias("user_agent"),
             time_table.year,
             time_table.month)
    #print(songplays_table.show())
    # write songplays table to parquet files partitioned by year and month
    # songplays_table.write.mode("overwrite").parquet(output_data + '/songplay')
    # songplays_table.write.partitionBy(['year', 'month']).parquet(output_data + "songplays_table.parquet")
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + '/songplay')
    log_df.write.mode("overwrite").parquet(output_data + '/log')
    # songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + "songplays_table.parquet")
    #songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + '/song')
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "./output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
if __name__ == "__main__":
    main()