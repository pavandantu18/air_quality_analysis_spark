from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TrainAndEvaluateModel") \
    .getOrCreate()

# Load the prepared training and test datasets
train_data_path = "/workspaces/air_quality_analysis_spark/Task4/output/train_data.parquet"
test_data_path = "/workspaces/air_quality_analysis_spark/Task4/output/test_data.parquet"
train_data = spark.read.parquet(train_data_path)
test_data = spark.read.parquet(test_data_path)

# Initialize the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="pm25")

# Train the model
model = lr.fit(train_data)

# Evaluate the model on the test dataset
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

# Print evaluation metrics
print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# Save the trained model
model_output_path = "/workspaces/air_quality_analysis_spark/Task4/output/linear_regression_model"
model.save(model_output_path)

print("Model training and evaluation completed. Model saved successfully.")