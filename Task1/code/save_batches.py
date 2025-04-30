import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, regexp_replace, trim, col
from pyspark.sql.types import DoubleType

# ───── CONFIG ─────────────────────────────────────────────────────────────────
BATCH_PATH = "/workspaces/air_quality_analysis_spark/Task1/output/batch/"
OUTPUT_DIR = "/workspaces/air_quality_analysis_spark/Task1/output/combined_data_singlefile/"

# ensure batches exist
if not os.path.isdir(BATCH_PATH) or not os.listdir(BATCH_PATH):
    print(f"No CSVs in {BATCH_PATH}. Run the streaming job first.")
    sys.exit(1)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ───── SPARK SETUP ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("MergeBatchesClean").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ───── 1) Load the “long” CSVs (note the wildcard!) ────────────────────────────
raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(BATCH_PATH, "*.csv"))

# ───── 2) Clean stray brackets/quotes & cast numerics ─────────────────────────
clean = (
    raw
    .withColumn("datetime",
        trim(regexp_replace(col("datetime"),  r"[\[\]']", ""))
    )
    .withColumn("location",
        trim(regexp_replace(col("location"),  r"[\[\]']", ""))
    )
    .withColumn("parameter",
        trim(regexp_replace(col("parameter"), r"[\[\]']", ""))
    )
    .withColumn("lat",
        regexp_replace(col("lat"), r"[\[\]']", "").cast(DoubleType())
    )
    .withColumn("lon",
        regexp_replace(col("lon"), r"[\[\]']", "").cast(DoubleType())
    )
    .withColumn("value",
        regexp_replace(col("value"), r"[\[\]']", "").cast(DoubleType())
    )
)

# ───── 3) Pivot into wide form ─────────────────────────────────────────────────
PARAMS = ["pm10", "pm25", "pm1", "relativehumidity", "temperature", "um003"]

wide_df = (
    clean
    .groupBy("datetime", "location", "lat", "lon")
    .pivot("parameter", PARAMS)
    .agg(first("value"))
    .orderBy("datetime")
)

# ───── 4) Preview ─────────────────────────────────────────────────────────────
wide_df.show(5, truncate=False)

# ───── 5) Write final CSV ─────────────────────────────────────────────────────
wide_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(OUTPUT_DIR)

print(f"✅ Wide CSV (clean, no stray brackets) written to {OUTPUT_DIR}")
spark.stop()
