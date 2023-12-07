import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.assignment_2.utils import *

class TestSparkAssignment(unittest.TestCase):
    def setUp(self): 
        self.spark = spark_session()
        self.filepath = "C:/Users/sravan/Downloads/ghtorrent-logs.txt"
        self.rdd = creating_rdd(self.spark, self.filepath)
        self.df = reading_file(self.spark, self.filepath)
        self.df_torrent = create_dataframe_from_rdd(self.df)

    def tearDown(self):
        self.spark.stop()

    def test_creating_rdd(self):
        actual_rdd = self.rdd.collect()
        expected_rdd = ['DEBUG,2017-03-24T13:56:38+00:00,ghtorrent-46 -- ghtorrent.rb: Repo agco/harvesterjs exists',
                        'WARN,2017-03-23T11:04:43+00:00,ghtorrent-32 -- ght_data_retrieval.rb: Error processing event. Type: IssueCommentEvent, ID: 5531718847, Time: 64 ms',
                        'DEBUG,2017-03-23T10:44:44+00:00,ghtorrent-14 -- api_client.rb: Sleeping for 931 seconds',
                        'DEBUG,2017-03-23T09:36:14+00:00,ghtorrent-42 -- api_client.rb: Sleeping for 1441 seconds']
        self.assertEqual(actual_rdd, expected_rdd)

    def test_num_of_lines(self):
        num_lines = num_of_lines(self.rdd)
        expected_output = 4
        self.assertEqual(num_lines, expected_output)

    def test_create_dataframe_from_rdd(self):
        actual_df = self.df_torrent.collect()
        expected_df = [
            ('DEBUG', '2017-03-24T13:56:38+00:00', 'ghtorrent-46 ', ' ghtorrent.rb', ' Repo agco/harvesterjs exists'),
            ('WARN', '2017-03-23T11:04:43+00:00', 'ghtorrent-32 ', ' ght_data_retrieval.rb',
             ' Error processing event. Type'),
            ('DEBUG', '2017-03-23T10:44:44+00:00', 'ghtorrent-14 ', ' api_client.rb', ' Sleeping for 931 seconds'),
            ('DEBUG', '2017-03-23T09:36:14+00:00', 'ghtorrent-42 ', ' api_client.rb', ' Sleeping for 1441 seconds')
        ]
        self.assertEqual(actual_df, expected_df)

    def test_warn_messages(self):
        actual_warn_count = warn_messages(self.df_torrent)
        expected_warn_count = 1
        self.assertEqual(actual_warn_count, expected_warn_count)

    def test_api_clients(self):
        actual_api_clients = api_clients(self.df_torrent)
        expected_api_clients = 2
        self.assertEqual(actual_api_clients, expected_api_clients)

    def test_most_http_requests(self):
        actual_most_http_requests = most_http_requests(self.df_torrent)
        expected_most_req_df = []
        self.assertEqual(actual_most_http_requests.collect(), expected_most_req_df)

    def test_most_failed_requests(self):
        actual_most_failed_requests = most_failed_requests(self.df_torrent)
        expected_most_failed_df = []
        self.assertEqual(actual_most_failed_requests.collect(), expected_most_failed_df)

    def test_most_active_hours(self):
        actual_most_active_hours = most_active_hours(self.df_torrent)
        expected_active_hours = [(16,)]
        expected_active_schema = StructType([StructField('hour', IntegerType(), True)])
        expected_df_more_active_hours = self.spark.createDataFrame(data=expected_active_hours, schema=expected_active_schema)
        self.assertEqual(actual_most_active_hours.collect(), expected_df_more_active_hours.collect())

    def test_most_active_repositories(self):
        actual_most_active_repositories = most_active_repositories(self.df_torrent)
        expected_activerepo_df = self.spark.createDataFrame(data=expected_activerepo_data, schema=expected_activerepo_schema)
        self.assertEqual(actual_most_active_repositories.collect(), expected_activerepo_df.collect())

if __name__ == '__main__':
    unittest.main()
