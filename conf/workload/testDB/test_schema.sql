-- Test DB configuration
\set site1 4
\set site2 7
\set test_year 2006

-- Create data tables
CREATE TABLE demographics (
       patient_id integer,
       birth_year integer,
       gender integer,
       race integer,
       ethnicity integer,
       insurance integer,
       zip integer);
 
CREATE TABLE remote_diagnoses (
	patient_id integer NOT NULL,
    site integer NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    visit_no integer NOT NULL,
    type_ integer NOT NULL,
    encounter_id integer NOT NULL,
    diag_src character varying NOT NULL,
    icd9 character varying NOT NULL,
    primary_ integer NOT NULL,
    timestamp_ timestamp without time zone,
    clean_icd9 character varying,
    major_icd9 character varying
);

CREATE TABLE diagnoses (
    patient_id integer NOT NULL,
    site integer NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    visit_no integer NOT NULL,
    type_ integer NOT NULL,
    encounter_id integer NOT NULL,
    diag_src character varying NOT NULL,
    icd9 character varying NOT NULL,
    primary_ integer NOT NULL,
    timestamp_ timestamp without time zone,
    clean_icd9 character varying,
    major_icd9 character varying
);

DROP TABLE IF EXISTS cdiff_cohort;
CREATE TABLE cdiff_cohort AS (
       SELECT DISTINCT patient_id FROM diagnoses WHERE icd9 = '008.45' AND year = :test_year AND (site=:site1 OR site=:site2));

DROP TABLE IF EXISTS mi_cohort;
CREATE TABLE mi_cohort AS (
       SELECT DISTINCT patient_id FROM diagnoses WHERE icd9 LIKE '410%'  AND year = :test_year AND (site=:site1 OR site=:site2));



 -- data used in test to simulate patient registry
DROP TABLE IF EXISTS cdiff_cohort_diagnoses;
CREATE TABLE cdiff_cohort_diagnoses AS SELECT * FROM diagnoses WHERE year = :test_year AND (site=:site1 OR site=:site2) AND patient_id IN (SELECT * FROM cdiff_cohort); 
DROP TABLE IF EXISTS mi_cohort_diagnoses;
CREATE TABLE mi_cohort_diagnoses AS SELECT * FROM diagnoses  WHERE year = :test_year AND (site=:site1 OR site=:site2) AND patient_id IN (SELECT * FROM mi_cohort); 

-- 

CREATE TABLE vitals (
	patient_id integer,
	height_timestamp timestamp,
	height_visit_no integer,
	height real,
	height_units character varying,
	weight_timestamp timestamp,
	weight_visit_no integer,
	weight real,
	weight_units character varying,
	bmi_timestamp timestamp,
	bmi_visit_no integer,
	bmi real,
	bmi_units character varying,
	pulse integer,
	systolic integer,
	diastolic integer ,
	bp_method character varying);       


CREATE TABLE labs (
	patient_id integer,
	timestamp_ timestamp,
	test_name character varying,
	value_ character varying,
	unit character varying,
	value_low real,
	value_high real);


CREATE TABLE medications (
    patient_id integer NOT NULL,
    site integer NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    medication character varying NOT NULL,
    dosage character varying NOT NULL,
    route character varying,
    timestamp_ timestamp without time zone
);

CREATE TABLE site (
       id integer);



CREATE TABLE remote_medications (
	patient_id integer NOT NULL,
    site integer NOT NULL,
    year integer NOT NULL,
    month integer NOT NULL,
    medication character varying NOT NULL,
    dosage character varying NOT NULL,
    route character varying,
    timestamp_ timestamp without time zone
);

DROP TABLE IF EXISTS mi_cohort_medications;
CREATE TABLE mi_cohort_medications AS SELECT * FROM medications  WHERE year = :test_year AND (site=:site1 OR site=:site2) AND patient_id IN (SELECT * FROM mi_cohort); 

CREATE TABLE a_diagnoses (
	patient_id integer,
	icd9 character varying,
	timestamp_ timestamp);

CREATE TABLE a_init (
	lenD integer,
	lenM integer,
	pad timestamp);

CREATE TABLE test (
	privc varchar(1),
	privi integer);


-- set up security level of attributes
-- default setting: attribute is private
CREATE ROLE public_attribute;
CREATE ROLE protected_attribute;
GRANT SELECT(patient_id) ON diagnoses TO public_attribute;
GRANT SELECT(visit_no) ON diagnoses TO public_attribute;
GRANT SELECT(primary_) ON diagnoses TO public_attribute;
GRANT SELECT(diag_src) ON diagnoses TO protected_attribute;
GRANT SELECT(icd9) ON diagnoses TO protected_attribute;
GRANT SELECT(major_icd9) ON diagnoses TO protected_attribute;


GRANT SELECT(patient_id) ON cdiff_cohort_diagnoses TO public_attribute;
GRANT SELECT(visit_no) ON cdiff_cohort_diagnoses TO public_attribute;
GRANT SELECT(primary_) ON cdiff_cohort_diagnoses TO public_attribute;
GRANT SELECT(diag_src) ON cdiff_cohort_diagnoses TO protected_attribute;
GRANT SELECT(icd9) ON cdiff_cohort_diagnoses TO protected_attribute;
GRANT SELECT(major_icd9) ON cdiff_cohort_diagnoses TO protected_attribute;

GRANT SELECT(patient_id) ON mi_cohort_diagnoses TO public_attribute;
GRANT SELECT(visit_no) ON mi_cohort_diagnoses TO public_attribute;
GRANT SELECT(primary_) ON mi_cohort_diagnoses TO public_attribute;
GRANT SELECT(diag_src) ON mi_cohort_diagnoses TO protected_attribute;
GRANT SELECT(icd9) ON mi_cohort_diagnoses TO protected_attribute;
GRANT SELECT(major_icd9) ON mi_cohort_diagnoses TO protected_attribute;

GRANT SELECT(patient_id) ON medications TO public_attribute;
GRANT SELECT(dosage) ON medications TO public_attribute;
GRANT SELECT(route) ON medications TO public_attribute;
GRANT SELECT(medication) ON medications TO protected_attribute;

GRANT SELECT(patient_id) ON mi_cohort_medications TO public_attribute;
GRANT SELECT(dosage) ON mi_cohort_medications TO public_attribute;
GRANT SELECT(route) ON mi_cohort_medications TO public_attribute;
GRANT SELECT(medication) ON mi_cohort_medications TO protected_attribute;