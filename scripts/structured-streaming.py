from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("Structured Streaming").getOrCreate()

# Watch the logs directory for new log data
accessLines = spark.readStream.text("../logs")


contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(|S+)\s(\S+)\s*(S*)\"'
timeExp = r'\[(\d{2})/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'

logsDF = accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                            regexp_extract('value', timeExp, 1).alias('timestamp'),
                            regexp_extract('value', generalExp, 1).alias('method'),
                            regexp_extract('value', generalExp, 2).alias('method'),
                            regexp_extract('value', generalExp, 3).alias('method'),
                            regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                            regexp_extract('value', contentSizeExp, 1).cast('integer').alias('contentSize'))

# Keep a running count of every access by status code
statusCountsDF = logsDF.groupBy(logsDF.status).count()

# Kick off streaming query, dumping result to console
query = (statusCountsDF.writeStream .outputMode("complete").format("console").queryName("counts").start())

# Run forever until terminated
query.awaitTermination()

spark.stop()