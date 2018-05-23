#!/bin/bash

#run from smcql home, which contains src/ 
path=$(pwd)
echo "Using test data from $path"

if [ ! -d "$path/src" ]; then
  echo "Running from incorrect directory. Please run from project home directory."
  exit
fi

echo "Creating test database..."

dbPrefix='smcql_testDB'
dropdb $dbPrefix
createdb $dbPrefix
psql $dbPrefix -f $path/conf/workload/testDB/test_schema.sql

for i in 1 2
do
    dbName=$dbPrefix'_site'$i
    dropdb $dbName
    createdb $dbName

    psql $dbName -f $path/conf/workload/testDB/test_schema.sql
    psql $dbName -c "\COPY diagnoses FROM '$path/conf/workload/testDB/$i/diagnoses.csv' WITH DELIMITER ','"
    psql $dbName -c "\COPY medications FROM '$path/conf/workload/testDB/$i/medications.csv' WITH DELIMITER ','"
    psql $dbName -c "\COPY site FROM '$path/conf/workload/testDB/$i/site.csv' WITH DELIMITER ','"

    val=$i
    if (($val == 1)); then
        psql $dbName -c "\COPY remote_diagnoses FROM '$path/conf/workload/testDB/2/diagnoses.csv' WITH DELIMITER ','"
    else
        psql $dbName -c "\COPY remote_diagnoses FROM '$path/conf/workload/testDB/1/diagnoses.csv' WITH DELIMITER ','"
    fi
    psql $dbName -c "CREATE TABLE remote_cdiff_cohort_diagnoses AS (SELECT * FROM remote_diagnoses WHERE icd9='008.45')"
    psql $dbName -c "CREATE TABLE remote_mi_cohort_diagnoses AS (SELECT * FROM remote_diagnoses WHERE icd9 like '414%')"
    psql $dbName -c "CREATE TABLE remote_mi_cohort_medications AS (SELECT * FROM remote_medications WHERE lower(medication) like '%aspirin%')"

    psql $dbName -f $path/conf/workload/testDB/setup_test_registries.sql
done

psql -lqt | cut -d \| -f 1 | grep -qw $dbPrefix
res0=$?
psql -lqt | cut -d \| -f 1 | grep -qw $dbPrefix'_site1'
res1=$?
psql -lqt | cut -d \| -f 1 | grep -qw $dbPrefix'_site2'
res2=$?

if (($res0 == 0)) && (($res1 == 0)) && (($res2 == 0)); then
    exit 0
else
    exit 1
fi
