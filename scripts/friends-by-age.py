from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf=conf)


# Return new line (age, num_friends) type tuple
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


lines = sc.textFile("../data/fakefriends.csv")
# Get new rdd with new lines tuple(age, num_friends)
rdd = lines.map(parseLine)

# # Get new rdd with (age, (num_friends, 1)
# # 1 is needed to count number of persons with same age
# totalByAge = rdd.mapValues(lambda num_friends: (num_friends, 1))
#
# # Get new rdd with reduce by Key
# # It returns new rdd with (age, (sum(num_friends), (sum (person with specific age))
# reducedByAge = totalByAge.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#
# # Map with Values and return new rdd with (age, avg(num_friends)) avg = num_friends/count(persons)
# averagesByAge = reducedByAge.mapValues(lambda x: x[0]/x[1])

totalByAge = rdd.mapValues(lambda age: (age, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

averagesByAge = totalByAge.mapValues(lambda x: x[0]/x[1])

results = averagesByAge.collect()

for result in results:
    print(result)
