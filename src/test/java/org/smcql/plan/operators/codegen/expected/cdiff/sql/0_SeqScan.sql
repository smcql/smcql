SELECT patient_id, timestamp_, icd9 = '008.45'
FROM cdiff_cohort_diagnoses
ORDER BY patient_id, timestamp_