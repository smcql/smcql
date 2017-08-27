WITH all_diagnoses AS (
    (SELECT * FROM cdiff_cohort_diagnoses)
    UNION ALL
    (SELECT * FROM remote_cdiff_cohort_diagnoses)
)
SELECT d.major_icd9, count(*) as cnt 
FROM all_diagnoses d 
GROUP BY d.major_icd9 
ORDER BY count(*) DESC 
LIMIT 10