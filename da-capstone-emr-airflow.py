from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago


DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 25),
    'retries': 0,
    'sla': timedelta(minutes=36),
    'email': ['yesaswia@gmail.com']

}

BUCKET_NAME = "yavula-dev"
local_ingest_script_path = "s3://yavula-da-capstone/local_ingest_script_path/spark_ingest_script.py"
local_process_script_path = "s3://yavula-da-capstone/local_process_script_path/spark_process_script.py"
s3_ingest_script_path = "yavula-da-capstone/s3_ingest_script_path/spark_ingest_script.py"
s3_process_script_path = "yavula-da-capstone/s3_process_script_path/spark_process_script.py"

JOB_FLOW_OVERRIDES = {
    'Name': 'da-capstone-emr-airflow',
    'ReleaseLabel': 'emr-5.29.0',

    "Applications": [
    {"Name": "Spark"},
    {"Name": "Hadoop"},
    {"Name": "Sqoop"},
    {"Name": "Hive"},
],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"}, # by default EMR uses py2, change it to py3
                }
            ],
        }
    ],

    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 2,
            },
        ],
        'Ec2SubnetId': 'subnet-0289890cae7fce106',
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'BootstrapActions':[{
            'Name': 'Install',
            'ScriptBootstrapAction': {
                'Path': 's3://yavula-da-capstone/bootstrap_scripts/bootstrap_actions.sh'
            }
        }],
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Configurations': [
    {
        "Classification": "spark-hive-site",
        "Properties": {
        "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
        }
    },
    {
        "Classification": "spark-defaults",
        "Properties": {
        "spark.jars.packages": "mysql:mysql-connector-java:8.0.25"
        }
    },
    ],
}

dag = DAG(dag_id='da-capstone-emr-airflow', default_args=DEFAULT_ARGS, catchup=False, schedule_interval='@daily')

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    dag=dag,
)

SPARK_INGEST_STEPS= [
    {
        'Name': 'execute the ingest script which is inside the s3 bucket',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://yavula-da-capstone/s3_ingest_script_path/spark_ingest_script.py",
               '--exec_date={{ execution_date.strftime("%Y-%m-%d") }}'
            ],
        },
    }
]

ingest_step_adder = EmrAddStepsOperator(
    task_id='add_ingest_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_INGEST_STEPS,
    dag=dag,
)

ingest_step_checker = EmrStepSensor(
    task_id='ingest_watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_ingest_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

SPARK_PROCESS_STEPS= [
    {
        'Name': 'execute the process script which is inside the s3 bucket',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://yavula-da-capstone/s3_process_script_path/spark_process_script.py",
                '--exec_date={{ execution_date.strftime("%Y-%m-%d") }}'
            ],
        },
    }
]

process_step_adder = EmrAddStepsOperator(
    task_id='add_process_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_PROCESS_STEPS,
    dag=dag,
)

process_step_checker = EmrStepSensor(
    task_id='process_watch_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_process_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag,
)

cluster_remover = EmrTerminateJobFlowOperator(
    task_id='remove_cluster',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag,
)

cluster_creator >> ingest_step_adder >> ingest_step_checker >> process_step_adder >> process_step_checker >> cluster_remover
