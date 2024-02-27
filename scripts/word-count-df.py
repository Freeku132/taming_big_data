from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Word count").getOrCreate()

df = spark.read.text('../data/Book.txt')
words = df.select(func.explode(func.split(df.value, '\\W+')).alias('word'))
words.filter(words.word != "")

lowercase_words = words.select(func.lower(words.word).alias('word'))

wordCounts = lowercase_words.groupBy('word').count()
wordCounts.orderBy("count", ascending=False).show(10)
wordCounts.orderBy(wordCounts['count'].desc()).show(10)
wordCounts.sort(wordCounts['count'].desc()).show(10)
# .show(df.count()), it show all rows of df
wordCounts.sort(func.desc('count')).show(wordCounts.count())

spark.stop()
