#!/bin/bash

#setup test databases for unit tests
echo "Populating test databases..."
/bin/bash conf/workload/testDB/create_test_dbs.sh

if (($?==1)); then
    echo "Error populating test databases"
    exit 1
fi

#create executable jar
echo "Creating executable..."
mvn package -Dmaven.test.skip=true

if (($?==1)); then
    echo "Error creating executable"
    exit 1
fi

echo "Setup completed successfully."
exit 0