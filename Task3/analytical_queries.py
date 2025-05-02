from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AnalyticalQueries") \
    .getOrCreate()

# Load the data from the Parquet file
input_path = "/workspaces/air_quality_analysis_spark/Task3/output/air_quality_data.parquet"
data = spark.read.parquet(input_path)
data.createOrReplaceTempView("air_quality_data")

# Query 1: Find locations with the highest average PM2.5 in the last 24 hours
query1 = """
SELECT location AS region, AVG(pm25) AS avg_pm25
FROM air_quality_data
WHERE datetime >= current_timestamp() - INTERVAL 1 DAY
GROUP BY location
ORDER BY avg_pm25 DESC
LIMIT 10
"""
result1 = spark.sql(query1)
result1.show()

# Query 2: Identify peak pollution intervals
query2 = """
SELECT location AS region, datetime, pm25
FROM air_quality_data
WHERE pm25 = (SELECT MAX(pm25) FROM air_quality_data)
"""
result2 = spark.sql(query2)
result2.show()

# Save the results to output files
output_path_query1 = "/workspaces/air_quality_analysis_spark/Task3/output/highest_avg_pm25.csv"
output_path_query2 = "/workspaces/air_quality_analysis_spark/Task3/output/peak_pollution_intervals.csv"

result1.write.csv(output_path_query1, header=True, mode="overwrite")
result2.write.csv(output_path_query2, header=True, mode="overwrite")

print("Analytical queries executed and results saved successfully.")