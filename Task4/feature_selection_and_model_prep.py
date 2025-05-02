from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

# Initialize Spark session
spark = SparkSession.builder \
    .appName("FeatureSelectionAndModelPrep") \
    .getOrCreate()

# Load the enhanced data from Task 2
input_path = "/workspaces/air_quality_analysis_spark/output_task2/enhanced_data/task2_enhanced_data.parquet"
data = spark.read.parquet(input_path)

# Select relevant features for modeling
selected_columns = ["pm25", "temperature", "relativehumidity", "lat", "lon"]
data = data.select(*selected_columns)

# Handle null values by imputing them with the mean
for column in ["temperature", "relativehumidity", "lat", "lon"]:
    data = data.fillna({column: data.select(column).agg({column: 'mean'}).collect()[0][0]})

# Split data into training and test sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Prepare features for MLlib
feature_columns = ["temperature", "relativehumidity", "lat", "lon"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
train_data = assembler.transform(train_data).select("features", "pm25")
test_data = assembler.transform(test_data).select("features", "pm25")

# Save the prepared datasets
train_output_path = "/workspaces/air_quality_analysis_spark/Task4/output/train_data.parquet"
test_output_path = "/workspaces/air_quality_analysis_spark/Task4/output/test_data.parquet"
train_data.write.parquet(train_output_path, mode="overwrite")
test_data.write.parquet(test_output_path, mode="overwrite")

print("Feature selection and data preparation completed. Training and test datasets saved.")