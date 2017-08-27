WITH diags (patient_id, timestamp_, r) AS (
 SELECT patient_id,timestamp_, row_number()  OVER (PARTITION BY patient_id ORDER BY timestamp_) AS r
 FROM cdiff_cohort_diagnoses
 WHERE icd9 = '008.45')
SELECT DISTINCT d1.patient_id
       FROM diags d1 JOIN diags d2 ON d1.patient_id = d2.patient_id
       WHERE TIMESTAMPDIFF(DAY, d2.timestamp_, d1.timestamp_) >=  15 AND TIMESTAMPDIFF(DAY, d2.timestamp_, d1.timestamp_) <= 56  AND d1.r+1 = d2.r
