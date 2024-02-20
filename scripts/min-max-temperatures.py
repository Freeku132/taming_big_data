from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


# Convert temp to F
# Return new tuple
def parseLine(line):
    fields = line.split(',')
    entry_type = fields[2]
    station_id = fields[0]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return station_id, entry_type, temperature


# Read file
file = sc.textFile('../data/1800.csv')
# Parse
parsedFile = file.map(parseLine)

# Filter by entry_type
# Return lines with entry_type "TMIN"
minTemps = parsedFile.filter(lambda x: "TMIN" in x[1])

# Remove entry type, return new tuple with station_id and temp
stations = minTemps.map(lambda x: (x[0], x[2]))

# Reduce by station_id and
# Filter by compare temp
filteredStations = stations.reduceByKey(lambda x, y: min(x, y))

# Collect result
results = filteredStations.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

# Add section with max temp

# Filter by entry_type
# Return lines with entry_type "TMAX"
maxTemps = parsedFile.filter(lambda x: "TMAX" in x[1])

# Remove entry type
maxTempStations = maxTemps.map(lambda x: (x[0], x[2]))

# Get max temp for unique stations_id
filteredMaxTemp = maxTempStations.reduceByKey(lambda x, y: max(x, y))

# Collect result
maxTempResult = filteredMaxTemp.collect()

for result in maxTempResult:
    print(result[0] + "\t{:.2f}F".format(result[1]))


