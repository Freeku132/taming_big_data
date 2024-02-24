from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("Teenager counter").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("UTF-8")), age=int(fields[2]), numFriends=int(fields[3]))


lines = spark.sparkContext.textFile("../data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# Select by row SQL from spark.sql using registered people table
# DataFrame[ID: bigint, name: string, age: bigint, numFriends: bigint]
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# Can be use like rdd
for teen in teenagers.collect():
    print(teen)

# Using spark.sql functions on created DF instead of sql queries
schemaPeople.groupby("age").count().orderBy("age").show()

spark.stop()
