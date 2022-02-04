import boto3
import argparse
import json
import os
import logging

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import lit

logFormatter = '%(levelname)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.INFO)
logger = logging.getLogger(__name__)

#initiate spark session
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
# .config("spark.driver.memory", "8g"

# accept the exec_date value here
parser = argparse.ArgumentParser()
parser.add_argument("--exec_date", type=lambda d: datetime.strptime(d, "%Y-%m-%d"))
args = parser.parse_args()
exec_date=args.exec_date

client = boto3.client(service_name='secretsmanager',region_name='us-west-2')
response = client.get_secret_value(SecretId='capstone-rds-secret')
secret = json.loads(response['SecretString'])

SOURCE_HOST = secret['host']
SOURCE_PORT = secret['port']
SOURCE_USERNAME = secret['username']
SOURCE_PASSWORD = secret['password']
SOURCE_DB = secret['dbname']

TARGET_BUCKET = "yavula-da-capstone"
TARGET_PREFIX = "crimedata/Crimes_2001_to_Present"
TARGET_DATABASE = 'crime_data'
TARGET_TABLE = 'Crimes_2001_to_Present'

TARGET_S3_URL  = os.path.join("s3://", TARGET_BUCKET, TARGET_PREFIX)
MY_SQL_CONNECTION_URL= "jdbc:mysql://"+str(SOURCE_HOST)+":"+str(SOURCE_PORT)+"/"+str(SOURCE_DB)
# jdbc:mysql://blackbelt-capstone-db.c1apjq8svdrz.us-west-2.rds.amazonaws.com:3306/crime_data
TARGET_S3_LATEST_PARTITION = os.path.join("s3://",TARGET_BUCKET,TARGET_PREFIX,"y={}".format(exec_date.strftime("%Y")),"m={}".format(exec_date.strftime("%-m")),"d={}".format(exec_date.strftime("%-d")))

#mysql connection
properties = {
        'user': SOURCE_USERNAME,
        'password': SOURCE_PASSWORD,
        'driver': 'com.mysql.cj.jdbc.Driver'
    }

#write the rds TARGET_TABLE output to a spark dataframe
def rds_to_dataframe():

    pushdown_query = "(SELECT * FROM crime_reported) crime_alias"
    df = spark.read.jdbc(MY_SQL_CONNECTION_URL, table=pushdown_query, properties=properties)
    logger.info('A spark dataframe has been created using required table from the RDS endpoint')
    return df

#save the dataframe to s3, create a TARGET_TABLE in glue datacatalog and update to the latest location
def create_exec_date_partitions(raw_df):

    df = (
    raw_df.withColumn("y", F.lit(exec_date.strftime("%Y")))
    .withColumn("m", F.lit(exec_date.strftime("%-m")))
    .withColumn("d", F.lit(exec_date.strftime("%-d")))
    )
    logger.info('The year, month and day partitions are created and aday_valueed to the database')
    return df

def create_glue_database(db_name):

    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(db_name))
        logging.info("Database created as not existing already")
    except Exception as e:
        logging.error(e)
        logging.error("Unable to create the database. Exiting...")
        os._exit(1)

#update the database to aws glue and s3 to reflect it in Athena
def update_database_and_snapshot_table(final_df):

    final_df.write.mode("overwrite").partitionBy("y", "m", "d").format(
        "parquet"
    ).saveAsTable(
        "{TARGET_DATABASE}.{TARGET_TABLE}".format(
            TARGET_DATABASE=TARGET_DATABASE, TARGET_TABLE=TARGET_TABLE+ "_snapshot"
        ),
        path=TARGET_S3_URL,
    )
    logger.info('The required table is updated in the glue data catalog and also the partitioned data is saved to s3')

def create_latest_snapshot_table():

    if (spark._jsparkSession.catalog().tableExists('crime_data.Crimes_2001_to_Present')):
        logger.info("Updating the partition for the latest raw snapshot table...")
        spark.sql("ALTER TABLE crime_data.Crimes_2001_to_Present SET LOCATION '{}'".format(TARGET_S3_LATEST_PARTITION))
    else:
        logger.info("Table pointing to the latest raw snapshot partition doesn't exist. Creating it...")
        spark.sql("CREATE TABLE crime_data.Crimes_2001_to_Present USING PARQUET LOCATION '{}'".format(TARGET_S3_LATEST_PARTITION))
        logger.info("The latest table has been updated.")

def main():

    raw_df=rds_to_dataframe()
    final_df=create_exec_date_partitions(raw_df)
    create_glue_database(TARGET_DATABASE)
    update_database_and_snapshot_table(final_df)
    create_latest_snapshot_table()

if __name__ == '__main__':
    main()
    