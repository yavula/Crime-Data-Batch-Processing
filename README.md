# Crime data- Batch Processing:
## RDBMS Data Extraction Implementation 
This project is intended to implement a solution designed to extract, transform and load Chicago crime data from an RDS instance to other services in AWS.
- There is an airflow dag script, 2 pyspark application scripts, and a bootstrap actions script in this project which are explained below.

## Deployment
Preparation:
- An AWS RDS MySQL instance is created to store the batch of data.
   - An EC2 instance is created to communicate with the RDS instance. 
   - The data is loaded onto the EC2 instance. 
   - The database and table are created on the RDS instance with the help of the above created EC2 instance. The data is loaded in the table created above.
   - The `create&Load.sql` file contains the code for the above table data preparation step.
   - A secret on the Secrets Manager console is stored to communicate with the RDS instance secretly. Also, password rotation after 30 days has been configured for security purposes.
- The following dag loads the data created from the above step into the AWS environment.

Implementation:
-  The airflow dag is put in the `s3://yavula-da-capstone/dag/` location in the S3 bucket. An environment is created on the Amazon Managed Workflows for Apache Airflow(MWAA) console in a specific VPC. 
- The dag is scheduled to run on a `daily` basis along with SLA monitoring to trigger an alarm if the tasks take more than 36 minutes to finish the whole ETL process.
- It usually takes 32-34 minutes to finish the dag processes. But if it takes, more than that, it means that something has interrupted the dag from finishing its process and we can check the logs accordingly. 
### emr_job_flow_manual_steps_dag.py
This script is used to create an airflow dag.
### Description
- The script has steps for the airflow to create an EMR cluster on AWS for a process which is explained later in the next steps. 
- It runs the STEPS that process the spark script on the EMR along with the bootstrap actions present in the `bootstrap_actions.sh` script which is in an s3 bucket that will install the required package like boto3 onto the EMR instance. 
- Then the step checker is also added to watch this process. This step sensor will periodically check if that last step is completed or skipped or terminated.

### spark_ingest_script.py
The spark script which is put into S3 manually, is used to ingest the required data from a table which is present on an RDS isntance and store the data into a raw s3 bucket and catalog into Glue.
### Description
- The ingest script connects to the RDS instance using the mysql-connector.
- It takes the required crime data from the table and puts it into a spark dataframe which is then written to the AWS S3 and Glue data catalog.
- S3 File Structure where the snapshot data is saved
   - `<yavula-da-capstone>` (bucket)
   - `<crime_data/Crimes_2001_to_Present>` (key)
   - `<crime_data>` (db-name)
   - `<Crimes_2001_to_Present>` (table-name)
- Glue Data Catalog table pointing to the latest partition 
   - `<s3://yavula-da-capstone/crime_data/Crimes_2001_to_Present/y={year_value}/m={month_value}/d={day_value}/>`

### spark_process_script.py
The spark script which is put into S3 manually, is used to query the latest target table, filter required crime details from it, then store the query results into a new final table and further save it to a latest partition.
### Description
- The spark script uses the crime data and performs some query processing using it.
- It queries the required crime data from the table, performs some processing and puts it into a spark dataframe which is then written to the AWS S3 and Glue data catalog.
- S3 File Structure where the snapshot data is saved
   - `<yavula-da-capstone>` (bucket)
   - `<crime_data/Crime_type_details>` (key)
   - `<crime_data>` (db-name)
   - `<Crime_type_details>` (table-name)
- Glue Data Catalog table pointing to the latest partition 
   - `<s3://yavula-da-capstone/crime_data/Crime_type_details/y={year_value}/m={month_value}/d={day_value}/>`

 ### bootstrap_actions.sh
 Required for the bootstrap actions.
### Description
Used to install the packages and dependencies on the cluster that are required for the processes inside the spark script to run.
### Deployment
- This bootstrap script is put manually in an S3 bucket.
- The location of this bucket is used inside the airflow dag to mention in the bootstrap actions that the required actions are present in the script which is in this particular s3 location. 

## Business Analysis

The final processed table had the crime type details for all the crimes for which the arrest is not made yet. This business analysis can be viewed from Athena and also has been imported into QuickSight Spice to view the details of different types of crimes and their comparisions. 