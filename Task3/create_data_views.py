from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CreateDataViews") \
    .getOrCreate()

# Load the cleaned/aggregated data
input_path = "/workspaces/air_quality_analysis_spark/output_task2/cleaned_data/task2_cleaned_data.parquet"
data = spark.read.parquet(input_path)

# Register the DataFrame as a temporary SQL view
data.createOrReplaceTempView("air_quality_data")

# Save the DataFrame as a Parquet file for persistence
output_path = "/workspaces/air_quality_analysis_spark/Task3/output/air_quality_data.parquet"
data.write.parquet(output_path, mode="overwrite")

print("Data saved as a Parquet file at", output_path)

# Optionally partition the data by date or region for efficient queries
# Example: Partitioning by location
partitioned_data = data.repartition("location")
partitioned_data.createOrReplaceTempView("partitioned_air_quality_data")

print("Temporary views 'air_quality_data' and 'partitioned_air_quality_data' created successfully.")