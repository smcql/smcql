SELECT major_icd9, COUNT(*) AS cnt
FROM cdiff_cohort_diagnoses
GROUP BY major_icd9
ORDER BY major_icd9