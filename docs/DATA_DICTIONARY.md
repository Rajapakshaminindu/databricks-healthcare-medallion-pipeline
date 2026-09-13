# 📖 Healthcare Lakehouse Data Dictionary

This document serves as the centralized Data Dictionary for the **Automated Healthcare Patient Lakehouse ETL Pipeline**. It specifies data schemas, column descriptions, data types, business calculation rules, constraints, and lineage for the analytical **Gold Layer** tables and views, as well as their upstream **Bronze** and **Silver** foundations.

---

## 🏛️ Medallion Architecture Summary

| Layer | Catalog & Database | Description | Storage Format |
| :--- | :--- | :--- | :--- |
| **Bronze** | `healthcare_lakehouse.bronze_patient_vitals` | Raw append-only telemetry ingestion with metadata audit stamps | Delta Lake |
| **Silver** | `healthcare_lakehouse.silver_patient_health` | Cleansed patient records, nullified telemetry dropouts, enriched clinical features | Delta Lake (ACID, schema evolution) |
| **Gold** | `healthcare_lakehouse.gold_city_health_kpi`<br/>`healthcare_lakehouse.gold_age_risk_distribution`<br/>`healthcare_lakehouse.v_clinical_escalations` | Pre-aggregated operational metrics, demographic risk matrix, and clinical escalation views | Delta Lake & Databricks SQL Views |
| **Dimension** | `healthcare_lakehouse.dim_patient_scd2` | Slowly Changing Dimension (SCD Type 2) tracking patient demographic and clinical risk history over time | Delta Lake (ACID, MERGE) |


---

## 🥇 Gold Layer Tables & Views

The Gold layer contains business-level aggregate tables designed for high-concurrency SQL analytics in Databricks SQL and downstream reporting in Power BI dashboards.

### 1. `healthcare_lakehouse.gold_city_health_kpi`

* **Type**: Delta Lake Table (Managed)  
* **Source**: `healthcare_lakehouse.silver_patient_health`  
* **Grain**: One row per municipality (`city`)  
* **Update Frequency**: Batch execution per pipeline run  
* **Downstream Consumers**: Databricks SQL Serverless, Power BI Executive Health Overview  

#### Schema & Column Specifications

| Column Name | Data Type | Nullable | Unit / Format | Description & Transformation Logic | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `city` | `STRING` | No | Text | Municipality / city where patient vitals were collected. Serves as primary grouping dimension. | `Bengaluru` |
| `total_patients` | `BIGINT` | No | Count | Total distinct patient count recorded in the city: `COUNT(patient_id)`. | `265` |
| `avg_glucose` | `DOUBLE` | Yes | $\text{mg/dL}$ | Average blood glucose level for the city rounded to 2 decimal places: `ROUND(AVG(glucose_level_clean), 2)`. Null values from sensor drops are excluded from calculation. | `121.48` |
| `avg_blood_pressure` | `DOUBLE` | Yes | $\text{mm Hg}$ | Average systolic blood pressure for the city rounded to 2 decimal places: `ROUND(AVG(blood_pressure_clean), 2)`. | `74.82` |
| `avg_bmi` | `DOUBLE` | Yes | $\text{kg/m}^2$ | Average Body Mass Index for the municipality rounded to 2 decimal places: `ROUND(AVG(bmi), 2)`. | `28.15` |
| `high_risk_patients` | `BIGINT` | No | Count | Count of patients in the municipality categorized with `diabetes_risk_score = 'High Risk'`: `COUNT(WHEN(diabetes_risk_score == 'High Risk'))`. | `94` |
| `high_risk_percentage` | `DOUBLE` | Yes | $\%$ | Percentage of total patients in the city identified as high risk: `ROUND((high_risk_patients / total_patients) * 100, 2)`. | `35.47` |

---

### 2. `healthcare_lakehouse.gold_age_risk_distribution`

* **Type**: Delta Lake Table (Managed)  
* **Source**: `healthcare_lakehouse.silver_patient_health`  
* **Grain**: One row per age cohort (`age_bracket`) and risk classification (`diabetes_risk_score`)  
* **Update Frequency**: Batch execution per pipeline run  
* **Downstream Consumers**: Demographic cohort analysis, Power BI cross-filtering matrices  

#### Schema & Column Specifications

| Column Name | Data Type | Nullable | Allowed Values | Description & Transformation Logic | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `age_bracket` | `STRING` | No | `'18-29'`, `'30-49'`, `'50-64'`, `'65+'` | Patient age stratification:<br/>• `< 30`: `'18-29'`<br/>• `30 - 49`: `'30-49'`<br/>• `50 - 64`: `'50-64'`<br/>• `&ge; 65`: `'65+'` | `30-49` |
| `diabetes_risk_score` | `STRING` | No | `'High Risk'`, `'Moderate Risk'`, `'Low Risk'` | Clinical risk tier based on glucose and BMI thresholds:<br/>• `'High Risk'`: `glucose_level_clean &ge; 140` OR `bmi &ge; 30.0`<br/>• `'Moderate Risk'`: `100 &le; glucose_level_clean < 140`<br/>• `'Low Risk'`: all other qualifying vitals | `High Risk` |
| `patient_count` | `BIGINT` | No | Count (&ge; 0) | Aggregate number of patients within the age bracket and risk classification: `COUNT(patient_id)`. | `210` |

---

### 3. `healthcare_lakehouse.v_clinical_escalations`

* **Type**: Databricks SQL View  
* **Source**: `healthcare_lakehouse.silver_patient_health`  
* **Grain**: One row per clinical encounter meeting escalation thresholds  
* **Filter Criteria**: `diabetes_risk_score = 'High Risk' AND blood_pressure_clean > 130 AND bmi >= 30.0`  
* **Downstream Consumers**: Clinical triage alerting, acute risk monitoring dashboards  

#### Schema & Column Specifications

| Column Name | Data Type | Description | Clinical Significance |
| :--- | :--- | :--- | :--- |
| `patient_id` | `STRING` | Unique alphanumeric identifier (`P_1000` to `P_2499`) | Primary key for patient record lookup |
| `city` | `STRING` | Municipality where reading was recorded | Geographic dispatch / local clinic routing |
| `gender` | `STRING` | Patient gender (`Male`, `Female`, `Other`) | Demographic indicator |
| `age` | `INT` | Patient age in years | Comorbidity factor |
| `blood_pressure_clean` | `INT` | Cleansed systolic blood pressure in $\text{mm Hg}$ | Stage 1 / Stage 2 hypertension indicator ($> 130\ \text{mm Hg}$) |
| `glucose_level_clean` | `INT` | Cleansed fasting/random blood glucose in $\text{mg/dL}$ | Hyperglycemia / diabetic threshold indicator ($\ge 140\ \text{mg/dL}$) |
| `bmi` | `DOUBLE` | Body Mass Index in $\text{kg/m}^2$ | Clinical obesity index ($\ge 30.0$) |
| `bmi_category` | `STRING` | WHO standard classification | Category confirmation (`Obese`) |
| `diabetes_risk_score` | `STRING` | Clinical risk tier classification | Priority classification (`High Risk`) |
| `reading_timestamp` | `TIMESTAMP` | Standardized UTC timestamp (`yyyy-MM-dd HH:mm:ss`) | Recency and temporal tracking for immediate intervention |

---

### 4. `healthcare_lakehouse.dim_patient_scd2`

* **Type**: Delta Lake Table (Managed SCD Type 2 Dimension)  
* **Source**: `healthcare_lakehouse.silver_patient_health`  
* **Grain**: One row per patient version state (historical or current)  
* **Update Method**: Delta Lake `MERGE` operation  
* **Downstream Consumers**: Longitudinal patient studies, historical point-in-time cohort lookups  

#### Schema & Column Specifications

| Column Name | Data Type | Nullable | Description & SCD Type 2 Logic | Example Value |
| :--- | :--- | :--- | :--- | :--- |
| `patient_id` | `STRING` | No | Unique patient identifier (`P_1000` to `P_2499`). Non-unique in table across multiple versions. | `P_1000` |
| `gender` | `STRING` | No | Biological sex / gender identity (`Male`, `Female`, `Other`). | `Female` |
| `city` | `STRING` | No | Municipality of residence during this version interval. | `Delhi` |
| `age_bracket` | `STRING` | No | Age demographic cohort (`18-29`, `30-49`, `50-64`, `65+`). | `30-49` |
| `bmi_category` | `STRING` | No | WHO BMI category during this interval (`Normal`, `Overweight`, `Obese`). | `Obese` |
| `diabetes_risk_score` | `STRING` | No | Clinical risk status during this interval (`High Risk`, `Moderate Risk`, `Low Risk`). | `High Risk` |
| `start_date` | `DATE` | No | Date when this version of the patient profile became active. | `2026-01-01` |
| `end_date` | `DATE` | Yes | Date when this version expired. Set to `NULL` for the currently active version. | `NULL` |
| `is_current` | `BOOLEAN` | No | Boolean flag: `true` if this is the active current record; `false` if historical. | `true` |

---

## 🥈 Upstream Lineage Reference: Silver Layer

### `healthcare_lakehouse.silver_patient_health`

Contains standardized, validated, and enriched patient records used to build Gold tables.

| Column | Type | Business Rule / Data Quality Constraint |
| :--- | :--- | :--- |
| `patient_id` | `STRING` | Mandatory primary key; filtered for `IS NOT NULL`. |
| `gender` | `STRING` | Biological sex / gender identity (`Male`, `Female`, `Other`). |
| `age` | `INT` | Age in years ($18 - 80$). |
| `city` | `STRING` | Participating healthcare service region. |
| `blood_pressure` | `INT` | Raw blood pressure as ingested. |
| `blood_pressure_clean` | `INT` | Quality filtered: values $\le 40$ are converted to `NULL` (sensor drop anomaly mitigation). |
| `glucose_level` | `INT` | Raw blood glucose as ingested. |
| `glucose_level_clean` | `INT` | Quality filtered: values $\le 40$ are converted to `NULL` (sensor drop anomaly mitigation). |
| `bmi` | `DOUBLE` | Body mass index ($15.0 - 55.0$). |
| `bmi_category` | `STRING` | Categorized via WHO thresholds: `< 18.5` (Underweight), `18.5 - 24.9` (Normal), `25.0 - 29.9` (Overweight), `&ge; 30.0` (Obese). |
| `insulin` | `INT` | Serum insulin level ($\mu\text{U/mL}$). |
| `diabetes_risk_score` | `STRING` | Clinical rule-based categorization (`High Risk`, `Moderate Risk`, `Low Risk`). |
| `age_bracket` | `STRING` | Cohort stratification (`18-29`, `30-49`, `50-64`, `65+`). |
| `reading_timestamp` | `TIMESTAMP` | Normalized UTC timestamp parsed from `yyyy-MM-dd HH:mm:ss`. |
| `_ingested_at` | `TIMESTAMP` | Lineage metadata inherited from Bronze ingestion. |
| `_source_batch` | `STRING` | Batch identifier inherited from Bronze ingestion (e.g., `batch_2026_09`). |
| `_transformed_at` | `TIMESTAMP` | Timestamp when Silver transformation occurred. |

---

## 🥉 Upstream Lineage Reference: Bronze Layer

### `healthcare_lakehouse.bronze_patient_vitals`

Append-only raw layer preserving untampered telemetry data with ingestion audit metadata.

| Column | Type | Description |
| :--- | :--- | :--- |
| `patient_id` | `STRING` | Raw patient ID |
| `gender` | `STRING` | Raw gender field |
| `age` | `INT` | Raw age field |
| `city` | `STRING` | Source city / location |
| `glucose_level` | `INT` | Raw glucose reading (including anomalies $\le 40$) |
| `blood_pressure` | `INT` | Raw blood pressure reading (including anomalies $\le 40$) |
| `bmi` | `DOUBLE` | Raw BMI reading |
| `insulin` | `INT` | Raw insulin reading |
| `reading_timestamp` | `STRING` | Raw timestamp string from source systems |
| `_ingested_at` | `TIMESTAMP` | UTC timestamp appended during initial bronze landing |
| `_source_batch` | `STRING` | Batch ingestion partition identifier |
