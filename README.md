# SMCQL : Secure Querying for Federated Databases

--------------------------------------------------------------------------------
Author
--------------------------------------------------------------------------------

SMCQL is developed and currently maintained by Johes Bater, under the direction of Jennie Rogers.


--------------------------------------------------------------------------------
Disclaimer
--------------------------------------------------------------------------------

The code is a research-quality proof of concept, and is still under development for more features and bug-fixing.

--------------------------------------------------------------------------------
Requirements
--------------------------------------------------------------------------------
PostgreSQL 9.5+
Oracle Java 8+
JavaCC 5.0+
Python 2.7+

--------------------------------------------------------------------------------
Setup
--------------------------------------------------------------------------------
Edit the configuration files:
    conf/setup.localhost
    conf/connections/localhost

This configures your local environment for SMCQL.

Run the following command:
    $ ./setup.sh

This sets up the test databases in PostgreSQL. 

--------------------------------------------------------------------------------
Running the example queries
--------------------------------------------------------------------------------
Run the following commands for the respective queries:

    Query 1: Comorbidity
    $ ./build_and_execute.sh conf/workload/sql/comorbidity.sql testDB1 testDB2 

    Query 2: Aspirin Count
    $ ./build_and_execute.sh conf/workload/sql/aspirin-count.sql testDB1 testDB2

    Query 3: Recurrent C.Diff
    $ ./build_and_execute.sh conf/workload/sql/cdiff.sql testDB1 testDB2 

Note that these queries are CPU-intensive. They may take several minutes to run, depending on hardware.

--------------------------------------------------------------------------------
Running SMCQL with your own data and schema
--------------------------------------------------------------------------------
Refer to conf/workload/testDB for the necessary files:

1. create_test_dbs.sh - This script creates and populates the PostgreSQL databases that house the data
2. test_schema.sql - This SQL query sets the schema, as well as the security level for each attribute

The above files are an automated example for setting up the test databases and annotated schema. You can choose to set these up manually, or to use existing databases. Please note that you must set the security levels for your attributes. Look at the bottom of test_schema.sql for an example. Remember that each attribute must be set as either a public, protected, or private variable.


--------------------------------------------------------------------------------
Running the example queries on different machines
--------------------------------------------------------------------------------
1. Create new configuration files for your remote hosts. 
Example:
    conf/setup.remote
    conf/connections/remote-hosts

2. Ensure that you have three machines with direct ssh access to one another.

3. Populate two of the machines with your data, in their local PostgreSQL instances. These will be your two workers, so each will have their own data.

4. Make sure that both your workers have their schemas set correctly (as detailed in the previous section). Remember that the schemas, and security annotations, must be the same for both machines.

5. On the third machine (the honest broker), ensure that you have a copy of the repository and that the configuration files are populated with the correct worker configuration information.

6. Run the example commands on the honest broker, from the SMCQL repository.
Example:
    Machine 1: Contains PostgreSQL database 'remoteDB1' and correct schema
    Machine 2: Contains PostgreSQL database 'remoteDB2' and correct schema
    Machine 3: Contains configuration files that specify the locations and connection information for 'remoteDB1' and 'remoteDB2'
        $ ./build_and_execute.sh conf/workload/sql/comorbidity.sql remoteDB1 remoteDB2

7. View your results on the honest broker.

--------------------------------------------------------------------------------
References
--------------------------------------------------------------------------------

J. Bater, G. Elliott, C. Eggen, A. Kho, and J. Rogers, “SMCQL : Secure Querying for Federated Databases.”