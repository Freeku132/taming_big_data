from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Decision Trees").getOrCreate()

    data = spark.read.csv("../data/realestate.csv", header=True, inferSchema=True)

    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]).setOutputCol("features")

    df = assembler.transform(data).select("PriceOfUnitArea", "features")

    trainingData, testData = df.randomSplit([0.8, 0.2])

    dtr = DecisionTreeRegressor(featuresCol="features").setFeaturesCol("features").setLabelCol("PriceOfUnitArea")

    model = dtr.fit(trainingData)

    fullPredictions = model.transform(testData).cache()

    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    predictionAndLabel = predictions.zip(labels).collect()

    for prediction in predictionAndLabel:
        print(prediction)

    spark.stop()
