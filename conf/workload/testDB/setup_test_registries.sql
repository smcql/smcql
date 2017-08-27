DROP TABLE IF EXISTS cdiff_cohort;
CREATE TABLE cdiff_cohort AS (
       (SELECT DISTINCT patient_id FROM diagnoses WHERE icd9 = '008.45' AND year = 2006 AND (site=4 OR site=7))
       UNION
       (SELECT DISTINCT patient_id FROM remote_diagnoses WHERE icd9 = '008.45' AND year = 2006 AND (site=4 OR site=7))
);

DROP TABLE IF EXISTS mi_cohort;
CREATE TABLE mi_cohort AS (SELECT DISTINCT patient_id FROM (
   (SELECT DISTINCT patient_id FROM diagnoses WHERE icd9 LIKE '414%' AND year=2006 AND (site=4 OR site=7))
    UNION
   (SELECT DISTINCT patient_id FROM remote_diagnoses WHERE icd9 LIKE '414%' AND year=2006 AND (site=4 OR site=7))
) as test);

DROP TABLE IF EXISTS cdiff_cohort_diagnoses;
CREATE TABLE cdiff_cohort_diagnoses AS (
    SELECT * FROM diagnoses WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM cdiff_cohort)
);

DROP TABLE IF EXISTS mi_cohort_diagnoses;
CREATE TABLE mi_cohort_diagnoses AS (
    SELECT * FROM diagnoses WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM mi_cohort)
);

DROP TABLE IF EXISTS mi_cohort_medications;
CREATE TABLE mi_cohort_medications AS(
    SELECT * FROM medications WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM mi_cohort)
); 

DROP TABLE IF EXISTS sample_cdiff_cohort_diagnoses;
CREATE TABLE sample_cdiff_cohort_diagnoses AS (
    SELECT * FROM diagnoses WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM cdiff_cohort)
);

DROP TABLE IF EXISTS sample_mi_cohort_diagnoses;
CREATE TABLE sample_mi_cohort_diagnoses AS (
    SELECT * FROM diagnoses WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM mi_cohort)
);

DROP TABLE IF EXISTS sample_mi_cohort_medications;
CREATE TABLE sample_mi_cohort_medications AS(
    SELECT * FROM medications WHERE year=2006 AND (site=4 OR site=7) AND patient_id IN (SELECT * FROM mi_cohort)
); 