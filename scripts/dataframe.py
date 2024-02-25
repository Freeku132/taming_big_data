from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Frame Exercise").getOrCreate()

people = spark.read.option('header', 'true').option("inferSchema", "true").csv('../data/fakefriends-header.csv')
people_2 = spark.read.csv('../data/fakefriends-header.csv', header='true', inferSchema='true')

people.printSchema()
people_2.printSchema()

people.select('name').show(10)

people.filter(people.age < 21).show()

people.groupby("age").count().show()

people.select(people.name, people.age + 10). show()

spark.stop()

