-- =============================================================================
-- GOLD LAYER ANALYTICS & CLINICAL KPI QUERIES
-- Database: healthcare_lakehouse
-- Tables: gold_city_health_kpi, gold_age_risk_distribution, silver_patient_health
-- =============================================================================

-- Query 1: Top Cities by Diabetes High-Risk Prevalence
SELECT 
    city,
    total_patients,
    avg_glucose,
    avg_bmi,
    high_risk_patients,
    high_risk_percentage,
    DENSE_RANK() OVER (ORDER BY high_risk_percentage DESC) AS risk_rank
FROM 
    healthcare_lakehouse.gold_city_health_kpi
ORDER BY 
    risk_rank ASC;

-- Query 2: Risk Distribution by Age Bracket (Pivoted for Reporting)
SELECT 
    age_bracket,
    SUM(CASE WHEN diabetes_risk_score = 'High Risk' THEN patient_count ELSE 0 END) AS high_risk_count,
    SUM(CASE WHEN diabetes_risk_score = 'Moderate Risk' THEN patient_count ELSE 0 END) AS moderate_risk_count,
    SUM(CASE WHEN diabetes_risk_score = 'Low Risk' THEN patient_count ELSE 0 END) AS low_risk_count,
    SUM(patient_count) AS total_in_bracket
FROM 
    healthcare_lakehouse.gold_age_risk_distribution
GROUP BY 
    age_bracket
ORDER BY 
    age_bracket ASC;

-- Query 3: Deep-Dive Patient Segment with Elevated Blood Pressure & BMI (Clinical Escalation View)
CREATE OR REPLACE VIEW healthcare_lakehouse.v_clinical_escalations AS
SELECT 
    patient_id,
    city,
    gender,
    age,
    blood_pressure_clean,
    glucose_level_clean,
    bmi,
    bmi_category,
    diabetes_risk_score,
    reading_timestamp
FROM 
    healthcare_lakehouse.silver_patient_health
WHERE 
    diabetes_risk_score = 'High Risk'
    AND blood_pressure_clean > 130
    AND bmi >= 30.0;
