SELECT COUNT(DISTINCT d.patient_id) as rx_cnt
FROM mi_cohort_diagnoses d JOIN mi_cohort_medications m ON d.patient_id = m.patient_id 
WHERE lower(m.medication) like '%aspirin%' AND d.icd9 like '414%' AND d.timestamp_ <= m.timestamp_