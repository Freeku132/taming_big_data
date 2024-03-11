from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}

    with codecs.open('../data/ml-100k/u.item', 'r', 'ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("ALS Example").getOrCreate()

movieSchema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)
])

names = loadMovieNames()

rating = spark.read.csv('../data/ml-100k/u.data', sep='\t', schema=movieSchema)

print('Training recommendation model...')

als = ALS(maxIter=5, regParam=0.01, userCol='user_id', itemCol='movie_id', ratingCol='rating')

model = als.fit(rating)

user_id = int(sys.argv[1])
userSchema = StructType([StructField('user_id', IntegerType(), True)])
users = spark.createDataFrame([[user_id]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print(recommendations)

for userRecs in recommendations:
    recs = userRecs[1]
    for rec in recs:
        print(names[rec.movie_id])