import unittest 
from pyspark.sql import SparkSession
from src.Assignmnet2.util import users_data, transc_data, join_dataframe, loc_unique, prod_user, total_spending_user

class TestSparkFunctions(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestSparkFunctions").getOrCreate()

    def tearDown(self):
        self.spark.stop()

    def create_test_df(self, data, columns):
        return self.spark.createDataFrame(data, columns)

    def test_users_data(self):
        test_user_data = [("1", "user1@example.com", "English", "Location1"),
                          ("2", "user2@example.com", "Spanish", "Location2")]
        expected_df = self.create_test_df(test_user_data, ["user_id", "emailid", "nativelanguage", "location"])

        result_df = users_data(self.spark, "C:/Users/sravan/Downloads/user.csv", header=True, inferSchema=True)

        self.assertTrue(expected_df.collect() == result_df.collect())

    def test_transc_data(self):
        test_trans_data = [("101", "A", "1", 10, "Product A"),
                           ("102", "B", "2", 20, "Product B")]
        expected_df = self.create_test_df(test_trans_data,
                                          ["transaction_id", "product_id", "userid", "price", "product_description"])
        result_df = transc_data(self.spark, "C:/Users/sravan/Downloads/transaction.csv", header=True, inferSchema=True)
        self.assertTrue(expected_df.collect() == result_df.collect())

    def test_join_dataframe(self):
        test_user_data = [("1", "user1@example.com", "English", "Location1"),
                          ("2", "user2@example.com", "Spanish", "Location2")]
        test_trans_data = [("101", "A", "1", 10, "Product A"),
                           ("102", "B", "2", 20, "Product B")]

        user_df = self.create_test_df(test_user_data, ["user_id", "emailid", "nativelanguage", "location"])
        trans_df = self.create_test_df(test_trans_data,
                                       ["transaction_id", "product_id", "userid", "price", "product_description"])
        expected_df = join_dataframe(user_df, trans_df, "user_id", "userid")
        result_df = join_dataframe(user_df, trans_df, "user_id", "userid")
        self.assertTrue(expected_df.collect() == result_df.collect())



if __name__ == "__main__":
    unittest.main()
