from pyspark.sql import SparkSession
from pyspark.sql.types import Row

if __name__ == '__main__':
    spark = SparkSession \
            .builder \
            .appName("Hello World PySpark Application") \
            .master("local[*]") \
            .getOrCreate()

    df_rows_list = [Row(id=1, name="Sivan", city="Chennai"), Row(id=2, name="Arun", city="Hyderabad")]

    df = spark.createDataFrame(df_rows_list)

    df.show(10, False)

    spark.stop()