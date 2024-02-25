from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, round

spark = SparkSession.builder.appName("Firends by age").getOrCreate()

data = spark.read.csv('../data/fakefriends-header.csv', header='true', inferSchema='true')

data.select('age', 'friends').groupby('age').agg(round(mean('friends'), 2).alias('avg_friends')).orderBy('age').show()

spark.stop()
