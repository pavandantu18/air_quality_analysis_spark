from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TrendAnalysis") \
    .getOrCreate()

# Load the data from the Parquet file
input_path = "/workspaces/air_quality_analysis_spark/Task3/output/air_quality_data.parquet"
data = spark.read.parquet(input_path)
data.createOrReplaceTempView("air_quality_data")

# Query: Trend analysis using window functions
query = """
SELECT 
    location AS region, 
    datetime, 
    pm25, 
    LAG(pm25) OVER (PARTITION BY location ORDER BY datetime) AS prev_pm25,
    LEAD(pm25) OVER (PARTITION BY location ORDER BY datetime) AS next_pm25,
    pm25 - LAG(pm25) OVER (PARTITION BY location ORDER BY datetime) AS rate_of_change
FROM air_quality_data
"""
result = spark.sql(query)
result.show()

# Save the results to an output file
output_path = "/workspaces/air_quality_analysis_spark/Task3/output/trend_analysis.csv"
result.write.csv(output_path, header=True, mode="overwrite")

print("Trend analysis completed and results saved successfully.")