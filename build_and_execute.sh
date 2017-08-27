#!/bin/bash

#Check usage
if [ "$#" -ne 3 ]; then
  printf "Usage: ./build_and_execute.sh <arg1> <arg2> <arg3>
    arg1: Path (including file name) containing SQL query. E.g. conf/workload/sql/comorbidity.sql
    arg2: Name of worker 1 (set in conf/connections) E.g. testDB1
    arg3: Name of worker 2 (set in conf/connections) E.g. testDB2\n"
  exit 1
fi

#Build jar
echo "Creating executable..."
mvn package -Dmaven.test.skip=true >/dev/null

if (($?==1)); then
    echo "Error creating executable"
    exit 1
fi

#Execute query
query=$(<$1)
echo "Executing Query..."
java -cp "target/smcql-open-source-0.5.jar" org.smcql.runner.SMCQLRunner "$query" "$2" "$3"
