# import argparse
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, avg, when, lit, expr
# import logging
# import sys

# # Initialize Logging
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)

# def main(env, bq_project, bq_dataset, transformed_table, route_insights_table, origin_insights_table):
#     try:
#         # Initialize SparkSession
#         spark = SparkSession.builder \
#             .appName("FlightBookingAnalysis") \
#             .config("spark.sql.catalogImplementation", "hive") \
#             .getOrCreate()

#         logger.info("Spark session initialized.")

#         # Resolve GCS path based on the environment
#         input_path = f"gs://airflow-projetcs-gds/airflow-project-1/source-{env}"
#         logger.info(f"Input path resolved: {input_path}")

#         # Read the data from GCS
#         data = spark.read.csv(input_path, header=True, inferSchema=True)
#         logger.info("Data read from GCS.")

#         # Data transformations
#         logger.info("Starting data transformations.")

#         # Add derived columns
#         transformed_data = data.withColumn(
#             "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
#         ).withColumn(
#             "lead_time_category", when(col("purchase_lead") < 7, lit("Last-Minute"))
#                                   .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 30), lit("Short-Term"))
#                                   .otherwise(lit("Long-Term"))
#         ).withColumn(
#             "booking_success_rate", expr("booking_complete / num_passengers")
#         )

#         # Aggregations for insights
#         route_insights = transformed_data.groupBy("route").agg(
#             count("*").alias("total_bookings"),
#             avg("flight_duration").alias("avg_flight_duration"),
#             avg("length_of_stay").alias("avg_stay_length")
#         )

#         booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
#             count("*").alias("total_bookings"),
#             avg("booking_success_rate").alias("success_rate"),
#             avg("purchase_lead").alias("avg_purchase_lead")
#         )

#         logger.info("Data transformations completed.")

#         # Write transformed data to BigQuery
#         logger.info(f"Writing transformed data to BigQuery table: {bq_project}:{bq_dataset}.{transformed_table}")
#         transformed_data.write \
#             .format("bigquery") \
#             .option("table", f"{bq_project}:{bq_dataset}.{transformed_table}") \
#             .option("writeMethod", "direct") \
#             .mode("overwrite") \
#             .save()

#         # Write route insights to BigQuery
#         logger.info(f"Writing route insights to BigQuery table: {bq_project}:{bq_dataset}.{route_insights_table}")
#         route_insights.write \
#             .format("bigquery") \
#             .option("table", f"{bq_project}:{bq_dataset}.{route_insights_table}") \
#             .option("writeMethod", "direct") \
#             .mode("overwrite") \
#             .save()

#         # Write booking origin insights to BigQuery
#         logger.info(f"Writing booking origin insights to BigQuery table: {bq_project}:{bq_dataset}.{origin_insights_table}")
#         booking_origin_insights.write \
#             .format("bigquery") \
#             .option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}") \
#             .option("writeMethod", "direct") \
#             .mode("overwrite") \
#             .save()

#         logger.info("Data written to BigQuery successfully.")

#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         sys.exit(1)

#     finally:
#         # Stop Spark session
#         spark.stop()
#         logger.info("Spark session stopped.")

# if __name__ == "__main__":
#     # Parse command-line arguments
#     parser = argparse.ArgumentParser(description="Process flight booking data and write to BigQuery.")
#     parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
#     parser.add_argument("--bq_project", required=True, help="BigQuery project ID")
#     parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
#     parser.add_argument("--transformed_table", required=True, help="BigQuery table for transformed data")
#     parser.add_argument("--route_insights_table", required=True, help="BigQuery table for route insights")
#     parser.add_argument("--origin_insights_table", required=True, help="BigQuery table for booking origin insights")

#     args = parser.parse_args()

#     # Call the main function with parsed arguments
#     main(
#         env=args.env,
#         bq_project=args.bq_project,
#         bq_dataset=args.bq_dataset,
#         transformed_table=args.transformed_table,
#         route_insights_table=args.route_insights_table,
#         origin_insights_table=args.origin_insights_table
#     )
# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, count, avg, split,substring
# # from dotenv import load_dotenv

# # load_dotenv(".env")  # Load environment variables from .env file




# def create_spark_session(app_name="FlightBookingETL"):
#     access_key = os.getenv("AWS_ACCESS_KEY_ID")
#     secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

#     if not access_key or not secret_key:
#         raise Exception("AWS credentials not loaded from .env!")

#     spark = SparkSession.builder \
#         .appName(app_name) \
#         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#         .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
#         .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
#         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
#         .getOrCreate()

#     # Debug what credentials Spark is using
#     hadoop_conf = spark._jsc.hadoopConfiguration()
#     print("DEBUG: Access Key:", hadoop_conf.get("fs.s3a.access.key"))
#     print("DEBUG: Secret Key:", hadoop_conf.get("fs.s3a.secret.key"))

#     return spark


# def main():
#     input_path = "s3a://flight-airflow-cicd/input/flight_booking.csv"
#     base_output_path = "s3a://flight-airflow-cicd/output"

#     print("Using AWS Access Key:", os.environ.get("AWS_ACCESS_KEY_ID"))

#     spark = create_spark_session()

#     # Read CSV
#     df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

#     # Extract source and destination from route
#     df = df.withColumn("source", substring("route", 1, 3)) \
#            .withColumn("destination", substring("route", 4, 3))

#     # Write full transformed data
#     df.write.mode("overwrite").csv(f"{base_output_path}/transformed", header=True)

#     # Route-level insights (replacing 'price' with 'purchase_lead')
#     route_insights = df.groupBy("source", "destination").agg(
#         count("*").alias("num_bookings"),
#         avg("purchase_lead").alias("avg_purchase_lead")
#     )
#     route_insights.write.mode("overwrite").csv(f"{base_output_path}/route_insights", header=True)

#     # Origin-level insights
#     origin_insights = df.groupBy("source").agg(
#         count("*").alias("total_departures"),
#         avg("purchase_lead").alias("avg_purchase_lead_from_origin")
#     )
#     origin_insights.write.mode("overwrite").csv(f"{base_output_path}/origin_insights", header=True)

#     spark.stop()
#     print("✅ Spark transformation job completed.")


# if __name__ == "__main__":
#     main()
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, substring
# from dotenv import load_dotenv

# Remove .env loading, not needed for Airflow

def create_spark_session(app_name="FlightBookingETL"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    return spark


def main():
    input_path = "s3a://flight-airflow-cicd/input/flight_booking.csv"
    base_output_path = "s3a://flight-airflow-cicd/output"

    print("✅ Running Spark ETL job...")
    print("AWS Access Key:", os.getenv("AWS_ACCESS_KEY_ID"))

    spark = create_spark_session()

    # Read CSV
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # Extract source and destination
    df = df.withColumn("source", substring("route", 1, 3)) \
           .withColumn("destination", substring("route", 4, 3))

    # Write full transformed data
    df.write.mode("overwrite").csv(f"{base_output_path}/transformed", header=True)

    # Route-level insights
    route_insights = df.groupBy("source", "destination").agg(
        count("*").alias("num_bookings"),
        avg("purchase_lead").alias("avg_purchase_lead")
    )
    route_insights.write.mode("overwrite").csv(f"{base_output_path}/route_insights", header=True)

    # Origin-level insights
    origin_insights = df.groupBy("source").agg(
        count("*").alias("total_departures"),
        avg("purchase_lead").alias("avg_purchase_lead_from_origin")
    )
    origin_insights.write.mode("overwrite").csv(f"{base_output_path}/origin_insights", header=True)

    spark.stop()
    print("✅ Spark transformation job completed.")


if __name__ == "__main__":
    main()
