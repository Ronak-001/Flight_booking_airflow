from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 6),
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(
    'flight_booking_etl',
    default_args=default_args,
    description='ETL pipeline for flight booking data using Spark and S3',
    schedule_interval='@once',
    catchup=False
)

# ✅ Python task to check file existence in S3
def check_s3_file():
    s3 = boto3.client('s3')
    bucket = "flight-airflow-cicd"
    key = "input/flight_booking.csv"

    try:
        s3.head_object(Bucket=bucket, Key=key)
        print(f"✅ Found: s3://{bucket}/{key}")
    except Exception as e:
        raise FileNotFoundError(f"❌ Missing file: s3://{bucket}/{key}")

check_file_task = PythonOperator(
    task_id='check_s3_input_file',
    python_callable=check_s3_file,
    dag=dag,
)

# ✅ Bash task to run Spark job (your actual command)
spark_submit_command = """
spark-submit \
  --jars /home/vboxuser/.local/lib/python3.8/site-packages/pyspark/jars/hadoop-aws-3.3.4.jar,/home/vboxuser/.local/lib/python3.8/site-packages/pyspark/jars/aws-java-sdk-bundle-1.12.375.jar \
  /home/vboxuser/Downloads/Flight-Booking-Airflow-CICD/spark_job/spark_transformation_job.py
"""

spark_etl_task = BashOperator(
    task_id='run_spark_job',
    bash_command=spark_submit_command,
    dag=dag,
)

# ✅ DAG flow: check → run spark
check_file_task >> spark_etl_task
