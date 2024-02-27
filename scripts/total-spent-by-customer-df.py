from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Total-spent-by-customer").getOrCreate()

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("item_id", StringType(), True),
    StructField('order_value', FloatType(), True)
])

df = spark.read.csv('../data/customer-orders.csv', schema=schema)

df.printSchema()
df.show()

total_spent_customer = df.groupBy("customer_id").agg(func.sum('order_value').alias('total_spent')).orderBy(func.col('total_spent').desc())
total_spent_customer.show()

spark.stop()