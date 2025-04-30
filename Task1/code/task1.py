# stream_to_batches.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_csv, col
import os

BATCH_PATH      = "/workspaces/air_quality_analysis_spark/Task1/output/batch/"
CHECKPOINT_PATH = "/workspaces/air_quality_analysis_spark/Task1/output/checkpoint/"

os.makedirs(BATCH_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

spark = SparkSession.builder \
    .appName("StreamToBatches") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Read the raw socket
raw = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# 2) Define CSV schema
schema_ddl = """
  location_id STRING,
  sensors_id  STRING,
  location    STRING,
  datetime    STRING,
  lat         STRING,
  lon         STRING,
  parameter   STRING,
  units       STRING,
  value       STRING
"""

# 3) Parse & drop unwanted columns
parsed = raw.select(
    from_csv(col("value"), schema_ddl,
             {"sep": ",", "header": "false", "quote": '"'}
    ).alias("c")
).select(
    "c.*"
)

# debug_query = parsed.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start()

# debug_query.awaitTermination()

# 4) Write each parsed row out as CSV (append)
query = parsed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", BATCH_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .option("header", "true") \
    .start()

print("▶️  Streaming raw into batches. Ctrl+C to stop.")
query.awaitTermination()
