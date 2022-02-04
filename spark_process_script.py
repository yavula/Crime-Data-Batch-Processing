import boto3
import argparse
import json
import os
import logging
import datetime

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

# accept the exec_date value here
parser = argparse.ArgumentParser()
parser.add_argument("--exec_date", type=lambda d: datetime.strptime(d, "%Y-%m-%d"))
args = parser.parse_args()
exec_date=args.exec_date

TARGET_BUCKET = "yavula-da-capstone"
TARGET_PREFIX = "crimedata/Crime_type_details"
TARGET_DATABASE = 'crime_data'
TARGET_TABLE = 'Crime_type_details'
TARGET_S3_URL  = os.path.join("s3://", TARGET_BUCKET, TARGET_PREFIX)
TARGET_S3_LATEST_PARTITION = os.path.join("s3://",TARGET_BUCKET,TARGET_PREFIX,"y={}".format(exec_date.strftime("%Y")),"m={}".format(exec_date.strftime("%-m")),"d={}".format(exec_date.strftime("%-d")))

def table_results_to_df():
    
    df=spark.sql("SELECT primary_type,count(primary_type) as crime_count from crime_data.crimes_2001_to_present where arrest='False' group by primary_type")
    logger.info('A spark dataframe has been created using required results from target table')
    return df

def create_exec_date_partitions(raw_df):

    df = (
    raw_df.withColumn("y", F.lit(exec_date.strftime("%Y")))
    .withColumn("m", F.lit(exec_date.strftime("%-m")))
    .withColumn("d", F.lit(exec_date.strftime("%-d")))
    )
    logger.info('The year, month and day partitions are created and added to the database')
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

    if (spark._jsparkSession.catalog().tableExists('crime_data.Crime_type_details')):
        logger.info("Updating the partition for the latest raw snapshot table...")
        spark.sql("ALTER TABLE crime_data.Crime_type_details SET LOCATION '{}'".format(TARGET_S3_LATEST_PARTITION))
    else:
        logger.info("Table pointing to the latest raw snapshot partition doesn't exist. Creating it...")
        spark.sql("CREATE TABLE crime_data.Crime_type_details USING PARQUET LOCATION '{}'".format(TARGET_S3_LATEST_PARTITION))
        logger.info("The latest table has been updated.")

def main():
    
    raw_df=table_results_to_df()
    final_df=create_exec_date_partitions(raw_df)
    create_glue_database(TARGET_DATABASE)
    update_database_and_snapshot_table(final_df)
    create_latest_snapshot_table()

if __name__ == '__main__':
    main()