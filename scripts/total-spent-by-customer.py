from pyspark import SparkConf, SparkContext
from operator import add

conf = SparkConf().setMaster("local").setAppName("Total spent by customer")
sc = SparkContext(conf=conf)


# Return tuple with id and order value
def parseLine(line):
    fields = line.split(',')
    customer_id = fields[0]
    order_value = float(fields[2])
    return customer_id, order_value


file = sc.textFile('../data/customer-orders.csv')

parsedFile = file.map(parseLine)

# Group by key and sum value
# customer_sum = parsedFile.groupByKey().mapValues(sum)
# customer_sum = parsedFile.reduceByKey(lambda x, y: x + y)
customer_sum = parsedFile.reduceByKey(add)
sorted_rdd = customer_sum.map(lambda x: (x[1], x[0])).sortByKey()
# print(customer_sum.collect())

for sum_value, customer_id in sorted_rdd.collect():
    print(customer_id, round(sum_value, 2))
