from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, hour
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def create_spark_session():
    spark = SparkSession.builder.appName("Assignment 1").getOrCreate()
    logging.info("Spark session created")
    return spark

def create_rdd(spark, filepath):
    rdd = spark.sparkContext.textFile(filepath)
    logging.info("Creating RDD")
    return rdd

def count_lines(log_rdd):
    lines_count = log_rdd.count()
    logging.info("Number of lines: %s" % lines_count)
    return lines_count

def read_text_file(spark, filepath):
    df = spark.read.text(filepath)
    logging.info("Reading text file")
    return df

def create_dataframe(df):
    df_parsed = df.withColumn("log_level", split(col("value"), ",").getItem(0)) \
        .withColumn("timestamp", split(col("value"), ",").getItem(1)) \
        .withColumn("downloader_id", split(col("value"), ",").getItem(2)).drop(col("value"))
    df_downloader = df_parsed.withColumn("downloader", split(col("downloader_id"), "--").getItem(0)) \
        .withColumn("repository_client", split(col("downloader_id"), "--").getItem(1)).drop("downloader_id")
    df_torrent = df_downloader.withColumn("repository_clients", split(col("repository_client"), ":").getItem(0)) \
        .withColumn("commit_message", split(col("repository_client"), ":").getItem(1)).drop("repository_client")
    logging.info("Creating a DataFrame")
    return df_torrent

def count_warning_messages(df_torrent):
    warning_messages_count = df_torrent.filter(col("log_level") == "WARN").count()
    logging.info("Number of warning messages: %s" % warning_messages_count)
    return warning_messages_count

def count_api_clients(df_torrent):
    api_repositories_count = df_torrent.filter(col("repository_clients").like('%api_client%')).count()
    logging.info("Repositories processed only by API clients: %s" % api_repositories_count)
    return api_repositories_count

def most_http_requests(df_torrent):
    df_http_requests = df_torrent.filter(col("commit_message").like('%https%'))
    df_most_http_requests = df_http_requests.groupBy("repository_clients").agg({"commit_message": "count"}).orderBy("count(commit_message)", ascending=False).limit(1)
    logging.info("Most HTTP requests: %s" % df_most_http_requests.collect()[0])

def most_failed_requests(df_torrent):
    df_failed_requests = df_torrent.filter(col("commit_message").like('%Failed%'))
    df_most_failed_requests = df_failed_requests.groupBy("repository_clients").agg({"commit_message": "count"}).orderBy("count(commit_message)", ascending=False).limit(1)
    logging.info("Most failed requests: %s" % df_most_failed_requests.collect()[0])

def most_active_hours(df_torrent):
    df_active_hours = df_torrent.withColumn("hour", hour("timestamp"))
    df_most_active_hours = df_active_hours.groupBy("hour").agg({"hour": "count"}).orderBy("count(hour)", ascending=False).limit(1)
    logging.info("Most active hours: %s" % df_most_active_hours.collect()[0])

def most_active_repositories(df_torrent):
    df_active_repositories = df_torrent.filter(col("repository_clients") == " ghtorrent.rb")
    if not df_active_repositories.isEmpty():
        row = df_active_repositories.first()
        logging.info("Most active repository: %s" % row)


