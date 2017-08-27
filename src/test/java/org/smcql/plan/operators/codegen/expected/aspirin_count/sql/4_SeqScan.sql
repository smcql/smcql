SELECT patient_id, timestamp_, LOWER(medication) LIKE '%aspirin%'
FROM (SELECT patient_id, medication, timestamp_
FROM mi_cohort_medications) AS t
ORDER BY patient_id, timestamp_