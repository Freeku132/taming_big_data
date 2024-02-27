from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("Total-spent-by-customer").getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

movies_df = spark.read.csv("../data/ml-100k/u.data", sep="\t", schema=schema)

top_movies = movies_df.groupBy("movie_id").agg(func.count("user_id").alias("count")).orderBy(func.col("count").desc())
top_movies.show()

spark.stop()
