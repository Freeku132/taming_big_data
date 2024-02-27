from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Min and max temperatures").getOrCreate()

schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

df = spark.read.csv('../data/1800.csv', schema=schema)

df.printSchema()

minTemps = df.filter(df.measure_type == 'TMIN')
maxTemps = df.filter(df.measure_type == 'TMAX')

minTemps = minTemps.groupby('station_id').agg(func.min('temperature').alias('lowest_temp'))
minTemps.orderBy('lowest_temp').show()

maxTemps = maxTemps.groupby('station_id').agg(func.max('temperature').alias('highest_temp'))
maxTemps.orderBy('highest_temp').show()

minTempsF = minTemps.withColumn("lowest_temp", func.round(func.col('lowest_temp') * 0.1 * (9.0 / 5.0) + 32.0, 2))
minTempsF.orderBy('lowest_temp').show()

spark.stop()