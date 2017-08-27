WITH all_diagnoses AS (
    (SELECT * FROM cdiff_cohort_diagnoses)
    UNION ALL
    (SELECT * FROM remote_cdiff_cohort_diagnoses)
),
diags (patient_id, timestamp_, r) AS (
    SELECT patient_id,timestamp_, row_number()  OVER (PARTITION BY patient_id ORDER BY timestamp_) AS r
    FROM all_diagnoses
    WHERE icd9 = '008.45')

SELECT DISTINCT d1.patient_id
       FROM diags d1 JOIN diags d2 ON d1.patient_id = d2.patient_id
       WHERE date_part('day', d2.timestamp_::timestamp - d1.timestamp_::timestamp) >=  15 AND date_part('day', d2.timestamp_::timestamp - d1.timestamp_::timestamp) <= 56  AND d1.r+1 = d2.r
