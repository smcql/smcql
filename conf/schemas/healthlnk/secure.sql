
CREATE TABLE r_demographics (
       patient_id integer,
       birth_year protected_integer,
       gender protected_integer,
       race protected_integer,
       ethnicity protected_integer,
       insurance protected_integer,
       zip private_integer);
 
CREATE TABLE s_diagnoses (
	patient_id integer,
	year private_integer,
	timestamp_ private_timestamp,
	visit_no integer,
	type_ protected_integer,
	encounter_id integer,
	diag_src protected_varchar(32),
	icd9 protected_varchar(7),
	major_icd9 protected_varchar(4),
	primary_ integer);

CREATE TABLE s_diag_counts (
       d_cnt integer );

CREATE TABLE r_cdiff_cohort (
	patient_id integer);

CREATE TABLE r_rcdiff_cohort (
	patient_id integer);




CREATE TABLE s_vitals (
	patient_id integer,
	height_timestamp private_timestamp,
	height_visit_no integer,
	height real,
	height_units varchar(10),
	weight_timestamp private_timestamp,
	weight_visit_no integer,
	weight real,
	weight_units varchar(10),
	bmi_timestamp private_timestamp,
	bmi_visit_no integer,
	bmi real,
	bmi_units varchar(10),
	pulse integer,
	systolic integer,
	diastolic integer ,
	bp_method varchar(10));       


CREATE TABLE s_labs (
	patient_id integer,
	timestamp_ private_timestamp,
	test_name protected_varchar(30),
	value_ protected_varchar(30),
	unit varchar(10),
	value_low real,
	value_high real);


CREATE TABLE s_medications (
	patient_id integer,
	year private_integer,
	timestamp_ private_timestamp,
	medication protected_varchar(32),
        dosage varchar(10),
	route varchar(14));

CREATE TABLE r_site (
       id integer);


CREATE TABLE s_cdiff (
	patient_id integer,
	timestamp_ timestamp);

CREATE TABLE s_a_medications (
	patient_id integer,
	medication varchar(10),
	timestamp_ timestamp);

CREATE TABLE s_a_diagnoses (
	patient_id integer,
	icd9 varchar(7),
	timestamp_ timestamp);

CREATE TABLE s_a_init (
	lenD integer,
	lenM integer,
	pad timestamp);

CREATE TABLE s_test (
	privc protected_varchar(1),
	privi protected_integer);



