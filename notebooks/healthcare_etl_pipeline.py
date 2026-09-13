# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 Automated Healthcare Patient Lakehouse ETL Pipeline
# MAGIC **Architecture**: Medallion (Bronze ➔ Silver ➔ Gold)  
# MAGIC **Platform**: Databricks, Apache Spark (PySpark), Delta Lake, Databricks SQL  
# MAGIC **Author**: Rajapaksha Minindu

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 1: Landing Zone & Ingestion Simulation
# MAGIC Generates realistic raw patient vitals with intentional data anomalies (0 values in BP/glucose, missing readings) simulating enterprise source systems.

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create synthetic patient telemetry with realistic dirty records
np.random.seed(42)
n_records = 1500

patient_ids = [f"P_{1000 + i}" for i in range(n_records)]
ages = np.random.randint(18, 80, size=n_records)
glucose = np.random.normal(120, 35, size=n_records).clip(50, 300).astype(int)
blood_pressure = np.random.normal(75, 12, size=n_records).clip(40, 150).astype(int)
bmi = np.round(np.random.normal(28, 6, size=n_records).clip(15, 55), 1)
insulin = np.random.randint(0, 350, size=n_records)

# Introduce realistic data quality issues: zero values representing dropped sensor telemetry
blood_pressure[np.random.choice(n_records, 25, replace=False)] = 0
glucose[np.random.choice(n_records, 15, replace=False)] = 0

cities = np.random.choice(["Bengaluru", "Delhi", "Chennai", "Ahmedabad", "Hyderabad", "Bhopal"], size=n_records)
genders = np.random.choice(["Male", "Female", "Other"], size=n_records, p=[0.48, 0.48, 0.04])
timestamps = [datetime.now() - timedelta(days=int(d)) for d in np.random.randint(0, 30, size=n_records)]

df_raw = pd.DataFrame({
    "patient_id": patient_ids,
    "gender": genders,
    "age": ages,
    "city": cities,
    "glucose_level": glucose,
    "blood_pressure": blood_pressure,
    "bmi": bmi,
    "insulin": insulin,
    "reading_timestamp": [t.strftime("%Y-%m-%d %H:%M:%S") for t in timestamps]
})

# Convert to PySpark DataFrame
spark_df = spark.createDataFrame(df_raw)
display(spark_df.limit(5))
print(f"✅ Staged {n_records} raw patient records into Spark DataFrame.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 2: Bronze Layer (Raw Delta Storage with Audit Trail)
# MAGIC Stores append-only raw data in Delta format and appends metadata columns (`_ingested_at`, `_source_batch`) for end-to-end data lineage.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

bronze_df = spark_df \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_batch", lit("batch_2026_09"))

# Create catalog schema if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS healthcare_lakehouse")

# Write to Bronze Delta Table (Preserving raw truth)
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("healthcare_lakehouse.bronze_patient_vitals")

print("✅ Bronze Delta table created: healthcare_lakehouse.bronze_patient_vitals")
display(spark.table("healthcare_lakehouse.bronze_patient_vitals").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 3: Silver Layer (Cleansing, Validation & Risk Scoring)
# MAGIC Implements data quality filters (cleaning medically invalid 0 values), timestamp casting, and business rule feature engineering (`bmi_category`, `diabetes_risk_score`, `age_bracket`).

# COMMAND ----------

from pyspark.sql.functions import col, when, to_timestamp

bronze_data = spark.table("healthcare_lakehouse.bronze_patient_vitals")

silver_df = bronze_data \
    .filter(col("patient_id").isNotNull()) \
    .withColumn("reading_timestamp", to_timestamp(col("reading_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn(
        "blood_pressure_clean",
        when(col("blood_pressure") <= 40, None).otherwise(col("blood_pressure"))
    ) \
    .withColumn(
        "glucose_level_clean",
        when(col("glucose_level") <= 40, None).otherwise(col("glucose_level"))
    ) \
    .withColumn(
        "bmi_category",
        when(col("bmi") < 18.5, "Underweight")
        .when((col("bmi") >= 18.5) & (col("bmi") < 25.0), "Normal")
        .when((col("bmi") >= 25.0) & (col("bmi") < 30.0), "Overweight")
        .otherwise("Obese")
    ) \
    .withColumn(
        "diabetes_risk_score",
        when((col("glucose_level_clean") >= 140) | (col("bmi") >= 30.0), "High Risk")
        .when((col("glucose_level_clean") >= 100) & (col("glucose_level_clean") < 140), "Moderate Risk")
        .otherwise("Low Risk")
    ) \
    .withColumn(
        "age_bracket",
        when(col("age") < 30, "18-29")
        .when((col("age") >= 30) & (col("age") < 50), "30-49")
        .when((col("age") >= 50) & (col("age") < 65), "50-64")
        .otherwise("65+")
    ) \
    .withColumn("_transformed_at", current_timestamp())

# Write to Silver Delta Table with Schema Evolution enabled
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("healthcare_lakehouse.silver_patient_health")

print("✅ Silver Delta table created: healthcare_lakehouse.silver_patient_health")
display(spark.table("healthcare_lakehouse.silver_patient_health").select(
    "patient_id", "city", "age_bracket", "glucose_level_clean", "bmi_category", "diabetes_risk_score"
).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 4: Gold Layer (Aggregations for Analytics & Power BI)
# MAGIC Pre-computes dimensional KPIs for low-latency queries and reporting.

# COMMAND ----------

from pyspark.sql.functions import count, avg, round

silver_data = spark.table("healthcare_lakehouse.silver_patient_health")

# KPI 1: City-level Health & High-Risk Indicators
gold_city_summary = silver_data.groupBy("city").agg(
    count("patient_id").alias("total_patients"),
    round(avg("glucose_level_clean"), 2).alias("avg_glucose"),
    round(avg("blood_pressure_clean"), 2).alias("avg_blood_pressure"),
    round(avg("bmi"), 2).alias("avg_bmi"),
    count(when(col("diabetes_risk_score") == "High Risk", True)).alias("high_risk_patients")
).withColumn(
    "high_risk_percentage",
    round((col("high_risk_patients") / col("total_patients")) * 100, 2)
)

gold_city_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("healthcare_lakehouse.gold_city_health_kpi")

# KPI 2: Demographic Age Group Risk Matrix
gold_age_risk = silver_data.groupBy("age_bracket", "diabetes_risk_score").agg(
    count("patient_id").alias("patient_count")
)

gold_age_risk.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("healthcare_lakehouse.gold_age_risk_distribution")

print("✅ Gold tables created successfully in Delta format.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 5: SCD Type 2 Dimension (Patient Medical & Demographic History)
# MAGIC Tracks historical changes when patient demographic or risk tiers change over time.
# MAGIC Preserves historical context using `start_date`, `end_date`, and `is_current` flags with Delta Lake.

# COMMAND ----------

from pyspark.sql.functions import current_date, to_date, lit, col
from delta.tables import DeltaTable

# 1. Initial Load of SCD2 Dimension from Silver (First Snapshot)
initial_patient_dim = spark.table("healthcare_lakehouse.silver_patient_health") \
    .select(
        "patient_id",
        "gender",
        "city",
        "age_bracket",
        "bmi_category",
        "diabetes_risk_score"
    ) \
    .dropDuplicates(["patient_id"]) \
    .withColumn("start_date", to_date(lit("2026-01-01"))) \
    .withColumn("end_date", lit(None).cast("date")) \
    .withColumn("is_current", lit(True))

initial_patient_dim.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("healthcare_lakehouse.dim_patient_scd2")

print("✅ Initialized SCD Type 2 Dimension: healthcare_lakehouse.dim_patient_scd2")
display(spark.table("healthcare_lakehouse.dim_patient_scd2").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simulate Ingestion of New Patient Updates (Batch 2)
# MAGIC Demonstrating SCD Type 2 handling: Patient moves city or their diabetes risk score changes.

# COMMAND ----------

# Simulate 3 updated patients with new vitals/cities
updates_data = [
    ("P_1000", "Female", "Delhi", "30-49", "Obese", "High Risk"),        # Moved from Bengaluru to Delhi
    ("P_1001", "Male", "Mumbai", "30-49", "Overweight", "High Risk"),     # Moved from Delhi to Mumbai, Risk increased
    ("P_1002", "Female", "Chennai", "50-64", "Normal", "Moderate Risk"),  # Risk improved to Moderate Risk
]

updates_df = spark.createDataFrame(
    updates_data,
    ["patient_id", "gender", "city", "age_bracket", "bmi_category", "diabetes_risk_score"]
)

# SCD Type 2 Logic using Delta Lake:
# Step A: Identify changed records and expire old active records
target_table = DeltaTable.forName(spark, "healthcare_lakehouse.dim_patient_scd2")

# Staged dataframe for merge: 
# Records that match existing patient and have changes in tracked attributes
staged_updates = updates_df.alias("updates") \
    .join(
        target_table.toDF().filter(col("is_current") == True).alias("target"),
        "patient_id"
    ) \
    .where(
        (col("updates.city") != col("target.city")) |
        (col("updates.diabetes_risk_score") != col("target.diabetes_risk_score")) |
        (col("updates.bmi_category") != col("target.bmi_category"))
    ) \
    .select(
        col("updates.patient_id").alias("merge_key"),
        col("updates.*")
    )

# Union staged updates with a dummy null key row to trigger insert for new version
dummy_updates = updates_df.select(
    lit(None).cast("string").alias("merge_key"),
    col("*")
)

scd_source_df = staged_updates.unionByName(dummy_updates)

# Delta Table MERGE:
# 1. When matched & key is present -> Close out old record
# 2. When not matched -> Insert new current record
target_table.alias("target").merge(
    scd_source_df.alias("source"),
    "target.patient_id = source.merge_key AND target.is_current = true"
).whenMatchedUpdate(
    set={
        "is_current": lit(False),
        "end_date": current_date()
    }
).whenNotMatchedInsert(
    values={
        "patient_id": "source.patient_id",
        "gender": "source.gender",
        "city": "source.city",
        "age_bracket": "source.age_bracket",
        "bmi_category": "source.bmi_category",
        "diabetes_risk_score": "source.diabetes_risk_score",
        "start_date": current_date(),
        "end_date": lit(None).cast("date"),
        "is_current": lit(True)
    }
).execute()

print("✅ Successfully executed SCD Type 2 MERGE into dim_patient_scd2.")
display(spark.table("healthcare_lakehouse.dim_patient_scd2")
    .filter(col("patient_id").isin(["P_1000", "P_1001", "P_1002"]))
    .orderBy("patient_id", "start_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 6: SQL Analytics Verification
# MAGIC Demonstrating serverless T-SQL querying over Gold & SCD2 Dimension tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     city,
# MAGIC     total_patients,
# MAGIC     avg_glucose,
# MAGIC     avg_bmi,
# MAGIC     high_risk_percentage
# MAGIC FROM 
# MAGIC     healthcare_lakehouse.gold_city_health_kpi
# MAGIC ORDER BY 
# MAGIC     high_risk_percentage DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## STEP 7: Delta Lake Time Travel & Transaction Log Audit

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY healthcare_lakehouse.silver_patient_health;

