SELECT patient_id, timestamp_, icd9 LIKE '414%'
FROM (SELECT patient_id, icd9, timestamp_
FROM mi_cohort_diagnoses) AS t
ORDER BY patient_id, timestamp_