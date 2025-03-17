import json
import boto3
import logging
from io import StringIO
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev, sum, trim, row_number
from pyspark.sql.window import Window

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session for Lambda (AWS Lambda has limited resources)
spark = SparkSession.builder \
    .appName("Rearc Data Analysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.2.2") \
    .getOrCreate()

# AWS Configuration
s3 = boto3.client('s3')
S3_BUCKET = "abh-de-rearc"
CSV_S3_PATH = "bls-data/pr.data.0.Current"
JSON_S3_PATH = "api-data/us_population.json"


def read_s3_csv(bucket, key):
    """
    Read CSV file from S3 into a PySpark DataFrame.
    """
    logger.info(f"Reading CSV from s3://{bucket}/{key}...")
    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_string = obj["Body"].read().decode("utf-8")
    csv_df = spark.read.option("header", "false").option("delimiter", "\t").csv(StringIO(csv_string))

    # Rename columns properly
    csv_df = csv_df.toDF("series_id", "year", "period", "value", "footnote_codes")

    # Trim strings to remove unwanted spaces
    csv_df = csv_df.withColumn("series_id", trim(col("series_id"))) \
        .withColumn("period", trim(col("period"))) \
        .withColumn("value", col("value").cast("float"))

    return csv_df


def read_s3_json(bucket, key):
    """
    Read JSON file from S3 into a PySpark DataFrame.
    """
    logger.info(f"Reading JSON from s3://{bucket}/{key}...")
    obj = s3.get_object(Bucket=bucket, Key=key)
    json_data = json.loads(obj["Body"].read().decode("utf-8"))

    json_df = spark.createDataFrame(json_data["data"])
    json_df = json_df.withColumn("year", col("Year").cast("int")) \
        .withColumn("population", col("Population").cast("int")) \
        .select("year", "population")

    return json_df


def process_reports(event, context):
    """
    AWS Lambda handler to process reports.
    """
    try:
        # Read data from S3
        csv_df = read_s3_csv(S3_BUCKET, CSV_S3_PATH)
        json_df = read_s3_json(S3_BUCKET, JSON_S3_PATH)

        # Rename columns properly
        csv_df = csv_df.toDF("series_id", "year", "period", "value", "footnote_codes")

        # Trim strings to remove unwanted spaces
        csv_df = csv_df.withColumn("series_id", trim(col("series_id"))) \
            .withColumn("period", trim(col("period"))) \
            .withColumn("value", col("value").cast("float"))

        # --- Task 1: Population Statistics (2013-2018) ---
        pop_stats_df = json_df.filter((col("year") >= 2013) & (col("year") <= 2018))
        pop_summary_df = pop_stats_df.select(
            mean("population").alias("mean_population"),
            stddev("population").alias("stddev_population")
        )

        logger.info("📊 Population Statistics (2013-2018):")
        pop_summary_df.show()

        # --- Task 2: Best Year for Each Series ID ---
        agg_df = csv_df.groupBy("series_id", "year").agg(sum("value").alias("total_value"))

        # Define ranking window by total_value per series_id
        window_spec = Window.partitionBy("series_id").orderBy(col("total_value").desc())

        best_year_df = agg_df.withColumn("rank", row_number().over(window_spec)) \
            .filter(col("rank") == 1).drop("rank")

        logger.info("Best Year for Each Series ID:")
        best_year_df.show(10,truncate=False)

        # --- Task 3: Report for `PRS30006032`, `Q01` ---
        filtered_df = csv_df.filter((col("series_id") == "PRS30006032") & (col("period") == "Q01"))

        final_report_df = filtered_df.join(json_df, "year", "inner").select(
            "series_id", "year", "period", "value", "population"
        )

        logger.info("Final Report for `PRS30006032`, `Q01`:")
        final_report_df.show()

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Report processing complete!",
                "population_stats": pop_summary_df.collect(),
                "best_year_per_series": best_year_df.collect(),
                "final_report": final_report_df.collect(),
            }, default=str)
        }

    except Exception as e:
        logger.error(f"Error processing reports: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
