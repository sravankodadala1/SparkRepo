from src.Assignment2.utils import *

spark = create_spark_session()

filepath = "C:/Users/sravan/Downloads/ghtorrent-logs.txt"
rdd = create_rdd(spark, filepath)

lines_count = count_lines(rdd)

df = read_text_file(spark, filepath)

df_torrent = create_dataframe(df)

warning_messages_count = count_warning_messages(df_torrent)

api_repositories_count = count_api_clients(df_torrent)

most_http_requests(df_torrent)

most_failed_requests(df_torrent)
most_active_hours(df_torrent)
most_active_repositories(df_torrent)
