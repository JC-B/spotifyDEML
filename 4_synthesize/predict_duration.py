from datetime import datetime
import json
from pyspark import SparkConf, SparkContext
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

## Constants
APP_NAME = "Batch ML Predict Genre"


def getVals(line):
    af = json.loads(line.strip())
    acousticness = af.get('acousticness')
    danceability = af.get('danceability')
    energy = af.get('energy')
    instrumentalness = af.get('instrumentalness')
    liveness = af.get('liveness')
    loudness = af.get("loudness")
    duration = af.get("duration_ms")

    return (acousticness, danceability, energy, instrumentalness, liveness, loudness, duration)

def main():

    # 1. Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    spark = SparkSession(sc)

    text_file = sc.textFile("s3a://spotifybuck/albumfeatures/2017/*/*/*/*/*")

    #3. Transform data
    af = (text_file
        .map(getVals)
        )

    #4. Create a DataFrame out of this using the toDF method and cache it
    afdf = af.toDF(['acousticness', 'danceability', 'energy', 'instrumentalness', 'liveness',
                    'loudness', 'duration']).cache()

    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer = \
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(afdf)

    #5. Create a train/test split with 70% of data in training set and 30% of data in test set
    afdf_train, afdf_test = afdf.randomSplit([0.7, 0.3], seed=123)

    # Train a RandomForest model.
    rf = RandomForestRegressor(featuresCol="indexedFeatures")

    # Chain indexer and forest in a Pipeline
    pipeline = Pipeline(stages=[featureIndexer, rf])

    # Train model.  This also runs the indexer.
    model = pipeline.fit(afdf_train)

    # Make predictions.
    predictions = model.transform(afdf_test)

    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

    rfModel = model.stages[1]
    print(rfModel)  # summary only

    #Step 3: Building our Pipelines

    rfModel.save('s3a://spotifybuck/model-export'+datetime.now().strftime('%Y%m%d%H%M'))
    pipeline.save('s3a://spotifybuck/pipeline-export'+datetime.now().strftime('%Y%m%d%H%M'))

    sc.stop()

if __name__ == '__main__':
    main()
