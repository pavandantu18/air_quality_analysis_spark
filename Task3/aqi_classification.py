from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AQIClassification") \
    .getOrCreate()

# Define a UDF to classify AQI based on PM2.5 value
def classify_aqi(pm25):
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 150:
        return "Unhealthy for Sensitive Groups"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"

# Register the UDF
classify_aqi_udf = udf(classify_aqi, StringType())

# Load the data from the Parquet file
input_path = "/workspaces/air_quality_analysis_spark/Task3/output/air_quality_data.parquet"
data = spark.read.parquet(input_path)
data.createOrReplaceTempView("air_quality_data")

# Add AQI classification column
data_with_aqi = data.withColumn("AQI_Category", classify_aqi_udf(data["pm25"]))

# Save the results to an output file
output_path = "/workspaces/air_quality_analysis_spark/Task3/output/aqi_classification.csv"
data_with_aqi.write.csv(output_path, header=True, mode="overwrite")

print("AQI classification completed and results saved successfully.")