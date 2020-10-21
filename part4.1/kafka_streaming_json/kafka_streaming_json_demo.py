from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import time

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema = StructType() \
        .add("order_id", StringType()) \
        .add("order_product_name", StringType()) \
        .add("order_card_type", StringType()) \
        .add("order_amount", StringType()) \
        .add("order_datetime", StringType()) \
        .add("order_country_name", StringType()) \
        .add("order_city_name", StringType()) \
        .add("order_ecommerce_website_name", StringType())

    # {'order_id': 1, 'order_product_name': 'Laptop', 'order_card_type': 'MasterCard',
    # 'order_amount': 38.48, 'order_datetime': '2020-10-21 10:59:10', 'order_country_name': 'Italy',
    # 'order_city_name': 'Rome', 'order_ecommerce_website_name': 'www.flipkart.com'}
    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")

    # Simple aggregate - find total_order_amount by grouping country, city
    orders_df4 = orders_df3.groupBy("order_country_name", "order_city_name") \
        .agg({'order_amount': 'sum'}) \
        .select("order_country_name", "order_city_name", col("sum(order_amount)") \
        .alias("total_order_amount"))

    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()

    # Write final result into console for debugging purpose
    orders_agg_write_stream = orders_df4 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")