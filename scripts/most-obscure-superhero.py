from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Obscure hero").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

names = spark.read.csv("../data/Marvel-names", schema=schema, sep=' ')

lines = spark.read.text('../data/Marvel-graph')

connections = (lines.withColumn('id', func.split(func.col('value'), ' ')[0])
               .withColumn("connections", func.size(func.split(func.col('value'), ' ')) - 1)
               .groupby("id").agg(func.sum('connections').alias('connections')))

min_val = connections.select(func.min('connections').alias('min_value')).first()

filtered_connections = connections.filter(connections.connections == min_val['min_value'])

most_obscure = filtered_connections.join(names, 'id')

most_obscure.show(most_obscure.count())
