from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HyperparameterTuning") \
    .getOrCreate()

# Load the prepared training and test datasets
train_data_path = "/workspaces/air_quality_analysis_spark/Task4/output/train_data.parquet"
test_data_path = "/workspaces/air_quality_analysis_spark/Task4/output/test_data.parquet"
train_data = spark.read.parquet(train_data_path)
test_data = spark.read.parquet(test_data_path)

# Initialize the Linear Regression model
lr = LinearRegression(featuresCol="features", labelCol="pm25")

# Create a parameter grid for hyperparameter tuning
param_grid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# Define a cross-validator
evaluator = RegressionEvaluator(labelCol="pm25", predictionCol="prediction", metricName="rmse")
crossval = CrossValidator(estimator=lr, \
                          estimatorParamMaps=param_grid, \
                          evaluator=evaluator, \
                          numFolds=3)

# Perform cross-validation to find the best model
cv_model = crossval.fit(train_data)

# Evaluate the best model on the test dataset
best_model = cv_model.bestModel
predictions = best_model.transform(test_data)
rmse = evaluator.evaluate(predictions)

# Print the best hyperparameters and evaluation metrics
print(f"Best Model Parameters: regParam={best_model._java_obj.getRegParam()}, elasticNetParam={best_model._java_obj.getElasticNetParam()}")
print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# Save the best model
model_output_path = "/workspaces/air_quality_analysis_spark/Task4/output/optimized_linear_regression_model"
best_model.save(model_output_path)

print("Hyperparameter tuning completed. Best model saved successfully.")