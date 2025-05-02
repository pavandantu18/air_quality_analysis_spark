from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimePredictionIntegration") \
    .getOrCreate()

# Load the trained model
model_path = "/workspaces/air_quality_analysis_spark/Task4/output/optimized_linear_regression_model"
model = LinearRegressionModel.load(model_path)

# Define the schema for incoming streaming data
schema = "pm25_lag1 DOUBLE, temperature DOUBLE, rate_of_change DOUBLE, humidity DOUBLE"

# Read streaming data from a socket source
streaming_data = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .selectExpr(f"from_csv(value, '{schema}') as data") \
    .select("data.*")

# Apply the model to make predictions
predictions = model.transform(streaming_data)

# Write predictions to the console
query = predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()