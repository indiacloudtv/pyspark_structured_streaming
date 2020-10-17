from pyspark.sql import SparkSession
from pyspark.sql.functions import *

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

    stream_words_df = stream_df \
                        .select(explode(split("value", ' ')).alias("word"))

    stream_word_count_df = stream_words_df \
                            .groupBy("word").count()

    write_query = stream_word_count_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="2 second") \
    .start()

    write_query.awaitTermination()

    print("Application Completed.")