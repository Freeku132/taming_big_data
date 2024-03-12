from __future__ import print_function
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Linear Regression").getOrCreate()

    inputLines = spark.sparkContext.textFile("../data/regression.txt")

    data = inputLines.map(lambda line: line.split(",")).map(lambda x: (float(x[0]), Vectors.dense([float(x[1])])))

    colNames = ["label", 'features']
    df = data.toDF(colNames)

    trainingData, testData = df.randomSplit([0.8, 0.2])

    lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    model = lir.fit(trainingData)

    fullPredictions = model.transform(testData).cache()

    predictions = fullPredictions.select('prediction').rdd.map(lambda x: x[0])
    labels = fullPredictions.select('label').rdd.map(lambda x: x[0])

    predictionAndLabel = predictions.zip(labels).collect()

    for prediction in predictionAndLabel:
        print(prediction)

    spark.stop()
