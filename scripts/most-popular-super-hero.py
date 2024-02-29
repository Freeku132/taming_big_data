from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

spark = SparkSession.builder.appName("Super hero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.csv("../data/Marvel-names", schema=schema, sep=' ')

lines = spark.read.text('../data/Marvel-graph')

connections = (lines.withColumn('id', func.split(func.col('value'), ' ')[0])
               .withColumn("connections", func.size(func.split(func.col('value'), ' ')) - 1)
               .groupby("id").agg(func.sum('connections').alias('connections')))

most_popular = connections.orderBy(func.col('connections').desc()).first()

most_popular_name = names.filter(func.col('id') == most_popular[0]).select("name").first()

print(most_popular_name.name)

spark.stop()
