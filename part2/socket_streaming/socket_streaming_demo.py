from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    spark = SparkSession \
            .builder \
            .appName("Socket Streaming Demo") \
            .master("local[*]") \
            .getOrCreate()

    stream_df = spark \
                .readStream \
                .format("socket") \
                .option("host", "localhost") \
                .option("port", "1100") \
                .load()

    print(stream_df.isStreaming)
    stream_df.printSchema()

    write_query = stream_df \
    .writeStream \
    .format("console") \
    .start()

    write_query.awaitTermination()

    print("Application Completed.")