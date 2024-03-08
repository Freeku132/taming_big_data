from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
import sys


def computeCosineSimilarity(data):
    # Compute xx, xy, and yy columns
    pairScores = data.withColumn('xx', func.col('rating1') * func.col('rating1')) \
        .withColumn('yy', func.col('rating2') * func.col('rating2')) \
        .withColumn('xy', func.col('rating1') * func.col('rating2'))

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores.groupBy(['movie1', 'movie2']) \
        .agg(func.sum(func.col('xy')).alias('numerator'),
             (func.sqrt(func.sum(func.col('xx'))) * func.sqrt(func.sum(func.col('yy')))).alias('denominator'),
             func.count(func.col('xy')).alias('numPairs'))

    result = calculateSimilarity.withColumn('score', func.when(func.col("denominator") != 0, func.col("numerator") / func.col("denominator"))
                                            .otherwise(0)).select("movie1", "movie2", "score", "numPairs")

    return result


def getMovieName(movieNames, movie_id):
    result = movieNames.filter(func.col('movie_id') == movie_id).select("movie_title").first()

    return result[0]


spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

movieNamesSchema = StructType([
    StructField('movie_id', IntegerType(), True),
    StructField('movie_title', StringType(), True)
])

movieSchema = StructType([
    StructField('user_id', IntegerType(), True),
    StructField('movie_id', IntegerType(), True),
    StructField('rating', IntegerType(), True),
    StructField('timestamp', LongType(), True)
])

movieNames = (spark.read
              .option('sep', '|')
              .option('charset', 'ISO-8859-1')
              .schema(movieNamesSchema)
              .csv('../data/ml-100k/u.item', ))

movies = spark.read.csv('../data/ml-100k/u.data', schema=movieSchema, sep='\t')

ratings = movies.select('user_id', 'movie_id', 'rating')

moviePairs = ratings.alias('ratings1') \
    .join(ratings.alias('ratings2'),
          (func.col('ratings1.user_id') == func.col('ratings2.user_id')) &
          (func.col('ratings1.movie_id') < func.col('ratings2.movie_id'))
          ) \
    .select(func.col('ratings1.movie_id').alias('movie1'),
            func.col('ratings2.movie_id').alias('movie2'),
            func.col('ratings1.rating').alias('rating1'),
            func.col('ratings2.rating').alias('rating2')
            )

moviePairSimilarities = computeCosineSimilarity(moviePairs).cache()

moviePairSimilarities.show()

if len(sys.argv) > 1:
    scoreThreshold = 0.97
    coOccurrenceThreshold = 50.0

    movie_id = int(sys.argv[1])

    filteredResults = moviePairSimilarities.filter( \
        ((func.col("movie1") == movie_id) | (func.col("movie2") == movie_id)) & \
        (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurrenceThreshold))

    results = filteredResults.sort(func.col('score').desc()).take(10)

    for result in results:
        similarMovieID = result.movie1
        if similarMovieID == movie_id:
            similarMovieID = result.movie2

            print(getMovieName(movieNames, similarMovieID), result.score, result.numPairs)