from pyspark.sql import SparkSession
from utils import join_dataframe, loc_unique, prod_user, total_spending_user
def Spark_Session():
    spark = SparkSession.builder.appName("Spark_assignment_1").getOrCreate()
    return spark
def users_data(spark, user_csv_path, header=True, inferSchema=True):
    user_df = spark.read.csv(user_csv_path, header=header, inferSchema=inferSchema)
    return user_df

def transc_data(spark, transaction_csv_path, header=True, inferSchema=True):
    transaction_df = spark.read.csv(transaction_csv_path, header=header, inferSchema=inferSchema)
    return transaction_df

if __name__ == "__main__":
    spark = Spark_Session()

    user_df = users_data(spark, "C:/Users/sravan/Downloads/user.csv")
    user_df.show()

    trans_df = transc_data(spark, "C:/Users/sravan/Downloads/transaction.csv")
    trans_df.show()

    join_df = join_dataframe(user_df, trans_df, "user_id", "userid")
    join_df.show()

    df_unique_loc_ = loc_unique(join_df, "product_description", "location")
    df_unique_loc_.show()

    user_product = prod_user(join_df, "userid", "product_id")
    user_product.show()

    exp_user = total_spending_user(join_df, "userid", "product_id", "price")
    exp_user.show()
