from pyspark.sql.functions import countDistinct, collect_set, sum, col

def join_dataframe(user_df, transaction_df, user_id_col, transaction_user_id_col):
    join_df = transaction_df.join(user_df, user_df[user_id_col] == transaction_df[transaction_user_id_col], "inner")
    return join_df

def loc_unique(join_df, product_description_col, location_col):
    unique_locations = join_df.groupBy(product_description_col, location_col).agg(
        countDistinct(location_col).alias("unique_locations"))
    return unique_locations

def prod_user(join_df, user_id_col, product_id_col):
    product_user = join_df.groupBy(user_id_col).agg(collect_set(product_id_col).alias("bought_products"))
    return product_user

def total_spending_user(join_df, user_id_col, product_id_col, price_col):
    total_spending = join_df.groupBy(user_id_col, product_id_col).agg(sum(price_col).alias("total_spending"))
    return total_spending
