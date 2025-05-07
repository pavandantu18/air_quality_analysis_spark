
# Air Quality Analysis Using Spark

## 📌 Project Overview

This project builds a modular, near-real-time air quality analysis pipeline using PySpark. It ingests sensor data via a TCP server, merges pollution and weather metrics, applies data cleaning and feature engineering, performs SQL-based trend analysis, trains predictive models with Spark MLlib, and visualizes results on an interactive dashboard. Outputs from each stage are stored independently (CSV/Parquet/PostgreSQL) to support parallel development and reproducibility.

---

## 🧩 Section 1: Data Ingestion and Initial Pre-Processing

### ✅ Objectives

- Simulate live data streaming from a TCP server.
- Parse datetime and detect schema correctness.
- Merge PM2.5, temperature, and humidity data by timestamp and region.
- Enrich with external weather data (temperature and humidity).
- Validate the final dataset quality.

---

## 🛠️ Project Structure

```
ingestion/
│
├── ingestion_task1.py                # Spark job to stream and clean data
├── merge_and_sort.py                 # Spark job to merge sensor metrics into unified records
├── tcp_log_file_streaming_server.py  # Simulated TCP server sending log data
├── test_reading_client.py            # Testing client for TCP connection
├── locations_metadata.csv            # Optional metadata for location mapping
├── download_from_s3.py               # Script to fetch files from S3
```

---

## 🚀 Getting Started

### 1. Requirements

- Python 3.8+
- Apache Spark 3.x (Structured Streaming)
- PySpark
- Docker (optional, for TCP server testing)
- Git

### 2. Installation

```bash
cd air_quality_analysis_spark
pip install -r requirements.txt
```

---

## ⚙️ Execution Steps

### Step 1: Download Files from S3

```bash
python ingestion/download_from_s3.py
```

### Step 2: Run the Simulated TCP Server

```bash
python ingestion/tcp_log_file_streaming_server.py
```

### Step 3: Ingest and Preprocess Streamed Data

```bash
spark-submit ingestion/ingestion_task1.py
```

### Step 4: Merge and Sort Metrics

```bash
spark-submit ingestion/merge_and_sort.py
```

---

## 📊 Output

The output of this stage is a **cleaned and enriched DataFrame** written to:
- Console (for debugging), and/or
- Local Parquet/CSV directory (e.g., `/ingestion/data/pending/final_task1`)

---

#  Section 2: Data Aggregation, Transformation & Trend Feature Engineering

## ✅ Objectives

- Handle outliers and missing data in pollution and weather sensor readings.
- Apply Z-score normalization to key numerical features.
- Perform daily and hourly aggregations to analyze time-based trends.
- Create rolling averages, lag features, and rate-of-change indicators.
- Save cleaned and feature-enhanced datasets for SQL and ML use.

---

## 🔧 Data Preprocessing Steps

### 1. Load Cleaned Output from Section 1
```python
import pandas as pd

# Load enriched and cleaned dataset (merged PM2.5, temperature, humidity)
df = pd.read_csv("/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task1/part-00000-*.csv", parse_dates=["timestamp"])
```

---

### 2. Handle Outliers
```python
import numpy as np

# Remove or cap implausible values
df = df[df["pm2_5"] < 1000]
df["temperature"] = np.where(df["temperature"] > 60, np.nan, df["temperature"])
df["humidity"] = np.where((df["humidity"] > 100) | (df["humidity"] < 0), np.nan, df["humidity"])
```

---

### 3. Impute Missing Values (Median)
```python
df["pm2_5"].fillna(df["pm2_5"].median(), inplace=True)
df["temperature"].fillna(df["temperature"].median(), inplace=True)
df["humidity"].fillna(df["humidity"].median(), inplace=True)
```

---

### 4. Normalize Key Features (Z-score)
```python
for col in ["pm2_5", "temperature", "humidity"]:
    df[f"{col}_zscore"] = (df[col] - df[col].mean()) / df[col].std()
```

---

### 5. Time-Based Aggregations
```python
# Extract date and hour for groupings
df["date"] = df["timestamp"].dt.date
df["hour"] = df["timestamp"].dt.hour

# Daily Aggregates
daily_avg = df.groupby(["date", "location"]).agg({
    "pm2_5": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()
daily_avg.to_csv("/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task2/daily_aggregates.csv", index=False)

# Hourly Aggregates
hourly_avg = df.groupby(["date", "hour", "location"]).agg({
    "pm2_5": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()
hourly_avg.to_csv("/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task2/hourly_aggregates.csv", index=False)
```

---

### 6. Rolling Averages, Lag Features, and Rate-of-Change
```python
# Sort for window operations
df.sort_values(by=["location", "timestamp"], inplace=True)

# Create rolling average (3-hour window), lag, and rate-of-change for PM2.5
df["pm2_5_rolling_avg_3"] = df.groupby("location")["pm2_5"].transform(lambda x: x.rolling(3, min_periods=1).mean())
df["pm2_5_lag_1"] = df.groupby("location")["pm2_5"].shift(1)
df["pm2_5_rate_of_change"] = df["pm2_5"] - df["pm2_5_lag_1"]
```

---

### 📂 Save Output
```python
# Final enriched dataset
output_path = "/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task2/task2_feature_enhanced.csv"
df.to_csv(output_path, index=False)
```

---

## 🎯 Outcome of Section 2

- Outliers capped and missing values imputed
- Features normalized with Z-score
- Time-based aggregations stored for trend analysis
- Rolling and lagged metrics computed for ML models
- Final dataset ready for SQL exploration and modeling in Section 3

Files Generated:
- `task2_feature_enhanced.csv`
- `daily_aggregates.csv`
- `hourly_aggregates.csv`




## 📊 Section 3: Spark SQL Exploration & Correlation Analysis

### ✅ Objectives

- Register cleaned feature-enhanced air quality data as a **temporary SQL view**.
- Develop **complex analytical queries** to identify regions with the highest PM2.5 levels.
- Perform **trend analysis** using **SQL window functions** (`ROW_NUMBER()`, `LAG()`, `LEAD()`).
- Implement a **UDF-based Air Quality Index (AQI) classification** to assess pollution risk levels.
- Save all outputs into organized CSV files.

---

## 🧩 Queries and Operations

### 1. Top Locations by Highest Average PM2.5

Using a CTE and MAX aggregation to find regions with the highest average:

```python
WITH avg_pm25_by_location AS (
    SELECT location, ROUND(AVG(pm2_5),2) AS avg_pm25
    FROM air_quality
    WHERE date = '{latest_date}'
    GROUP BY location
)
SELECT location, avg_pm25
FROM avg_pm25_by_location
WHERE avg_pm25 = (SELECT MAX(avg_pm25) FROM avg_pm25_by_location)
```

Saved Output: `/outputs/section3/top_locations_pm25.csv`

---

### 2. Peak Pollution Time Intervals

Ordering PM2.5 readings in descending order:

```python
SELECT timestamp, location, pm2_5
FROM air_quality
WHERE pm2_5 IS NOT NULL
ORDER BY pm2_5 DESC
```

Saved Output: `/outputs/section3/peak_pollution_times.csv`

---

### 3. Trend Analysis Using Window Functions

Calculating trends using LAG, LEAD, and ROW_NUMBER:

```python
window_spec = Window.partitionBy("location").orderBy("timestamp")

trend_df = df.withColumn("row_num", row_number().over(window_spec))              .withColumn("prev_pm2_5", lag(col("pm2_5")).over(window_spec))              .withColumn("next_pm2_5", lead(col("pm2_5")).over(window_spec))              .withColumn("pm2_5_change_prev", col("pm2_5") - col("prev_pm2_5"))              .withColumn("pm2_5_change_next", col("next_pm2_5") - col("pm2_5"))              .withColumn("trend", when(col("pm2_5_change_next") > 0, "Increasing")
                                  .when(col("pm2_5_change_next") < 0, "Decreasing")
                                  .otherwise("Stable"))
```

Saved Output: `/outputs/section3/trend_analysis_pm25.csv`

---

### 4. Air Quality Index (AQI) Classification

Custom UDF for classifying air quality:

```python
def classify_aqi(pm2_5_value):
    if pm2_5_value is None:
        return "Unknown"
    elif pm2_5_value <= 12:
        return "Good"
    elif pm2_5_value <= 35.4:
        return "Moderate"
    else:
        return "Unhealthy"

aqi_udf = udf(classify_aqi, StringType())

aqi_classified_df = df.withColumn("AQI_Category", aqi_udf(col("pm2_5")))
```

Saved Output: `/outputs/section3/aqi_classification.csv`

---

## Section 4:

Section 4 focuses on building, training, and evaluating a predictive model using Spark MLlib to forecast Air Quality Index (AQI) categories based on sensor readings (temperature, humidity, and PM2.5 trends).

## Steps Performed;

1. Load Feature-Enhanced Dataset:
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Air Quality ML Modeling").getOrCreate()

# Load the dataset generated in Task 2
df = spark.read.option("header", "true").option("inferSchema", "true").csv("task2_feature_enhanced.csv")
```

2. Create AQI Category Label

```python
from pyspark.sql.functions import when

# Define AQI categories based on PM2.5 values
df = df.withColumn("AQI_Category",
    when(df.pm2_5 <= 12, "Good")
    .when(df.pm2_5 <= 35.4, "Moderate")
    .otherwise("Unhealthy")
)
```

3. Feature Selection and Label Preparation
```python
from pyspark.ml.feature import StringIndexer, VectorAssembler

# Index AQI categories into numeric labels
indexer = StringIndexer(inputCol="AQI_Category", outputCol="label")
df = indexer.fit(df).transform(df)

# Assemble features
assembler = VectorAssembler(
    inputCols=["temperature", "humidity", "pm2_5_lag_1", "pm2_5_rate_of_change"],
    outputCol="features",
    handleInvalid="skip"
)

final_df = assembler.transform(df)
```

4. Train-Test Split:
# Split data
```python
train_data, test_data = final_df.randomSplit([0.7, 0.3], seed=42)
```
5. Train Random Forest Classifier
```python
from pyspark.ml.classification import RandomForestClassifier
```

# Initialize and train the model
```python
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)
model = rf.fit(train_data)
```

6. Evaluate Model Performance
```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Predictions
predictions = model.transform(test_data)

# Evaluators
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

# Results
accuracy = evaluator_acc.evaluate(predictions)
f1_score = evaluator_f1.evaluate(predictions)

print(f"\u2705 Model Evaluation Results:")
print(f" - Accuracy: {accuracy:.4f}")
print(f" - F1 Score: {f1_score:.4f}")
```

Result Achieved:

Accuracy: 96.26%

F1 Score: 96.12%


## Final Output Saved
# Save important fields (timestamp, location, true label, predicted label)
```python
predictions.select("timestamp", "location", "label", "prediction") \
    .write.mode("overwrite").option("header", "true") \
    .csv("../outputs/section4/final_predictions")
```


# Section 5: Real-Time Scoring & Sink to Postgres

## Objective
Apply the trained PM2.5 model in a live Spark Structured Streaming pipeline:  
1. Ingest from TCP  
2. Enrich features & compute AQI  
3. Score with RandomForest model  
4. Write predictions into Postgres

## Pre-requisites
- Section 4 has produced and saved:
  - `models/pm25_featurizer` (the feature-engineering PipelineModel)
  - `models/best_pm25_model` (the trained RF PipelineModel)
- PostgreSQL running and reachable; JDBC URL in env var `AIRQ_JDBC`

# 1. Start the TCP streaming simulator (from Section 1)
python ingestion/tcp_log_file_streaming_server.py

# 2. In a second shell, set your JDBC connection string
#    (replace host, port, db, user, password as needed)
export AIRQ_JDBC="jdbc:postgresql://localhost:5432/postgres?user=postgres&password=airq"

# 3. Submit the Section 5 pipeline to Spark
spark-submit \
  --master local[*] \
  Section5/pipeline_section5.py


## Pipeline Script
Path: `Section5/pipeline_section5.py`

```python
from pyspark.ml import PipelineModel
from pyspark.sql.functions import current_timestamp, first
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

# load featurizer & RF model
featurizer = PipelineModel.load("models/pm25_featurizer")
rf_model    = PipelineModel.load("models/best_pm25_model")

FEATURE_COLS = [
  "temperature","humidity",
  "pm25_lag1","temperature_lag1","humidity_lag1",
  "pm25_rate_change","temperature_rate_change","humidity_rate_change",
  "rolling_pm25_avg"
]
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")

def foreach_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty(): return

    # pivot raw parameters → columns
    pivoted = (batch_df
      .groupBy("location_id","latitude","longitude","event_time")
      .pivot("parameter", ["pm25","temperature","humidity"])
      .agg(first("value"))
    )

    # feature-engineer + AQI, assemble, score, timestamp
    feat      = featurizer.transform(pivoted)
    scored    = (rf_model.transform(feat)
                   .withColumn("ingest_time", current_timestamp())
                )

    # write only known columns
    (scored.select(
        "location_id","latitude","longitude","event_time",
        "pm25","prediction","probability","ingest_time"
      )
      .write
      .jdbc(url=jdbc_url, table="predictions", mode="append", properties=jdbc_props)
    )

# build streaming read from socket
spark = SparkSession.builder.appName("Section5").getOrCreate()
raw = (spark.readStream.format("socket")
       .option("host","localhost").option("port",9999).load())

# parse CSV-style text → columns
parsed = raw.withColumn("value", regexp_replace(regexp_replace(col("value"), r"[\\[\\]]",""),"'","")) \
            .withColumn("parts", split(col("value"),",\s*")) \
            .select(
               trim(col("parts")[0]).alias("location_id"),
               to_timestamp(trim(col("parts")[3]), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("event_time"),
               col("parts")[4].cast("double").alias("latitude"),
               col("parts")[5].cast("double").alias("longitude"),
               trim(col("parts")[6]).alias("parameter"),
               col("parts")[8].cast("double").alias("value")
            )

# read JDBC settings from AIRQ_JDBC env var
raw_jdbc = os.getenv("AIRQ_JDBC")
url, params = raw_jdbc.split("?",1)
jdbc_props = dict(p.split("=",1) for p in params.split("&"))
jdbc_props["driver"] = "org.postgresql.Driver"

# start streaming query
(parsed.writeStream
       .foreachBatch(foreach_batch)
       .trigger(processingTime="10 seconds")
       .option("checkpointLocation","output/checkpoints/section5")
       .outputMode("append")
       .start()
       .awaitTermination()
)
