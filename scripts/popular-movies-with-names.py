from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def loadMovieNames():
    movie_names = {}

    with codecs.open("../data/ml-100k/u.item", "r", "ISO-8859-1", errors='ignore') as f:
        for line in f:
            fields = line.strip().split("|")
            movie_names[int(fields[0])] = fields[1]
        return movie_names


def lookupName(movie_id):
    return name_dict.value[movie_id]


spark = SparkSession.builder.appName("Total-spent-by-customer").getOrCreate()

name_dict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", LongType(), True)
])

movies_df = spark.read.csv("../data/ml-100k/u.data", sep="\t", schema=schema)

top_movies = movies_df.groupBy("movie_id").agg(func.count("user_id").alias("count")).orderBy(func.col("count").desc())
top_movies.show()

lookupNameUDF = func.udf(lookupName)

movies_with_names = top_movies.withColumn("movie_title", lookupNameUDF(func.col('movie_id')))
movies_with_names.show(10, False)

spark.stop()