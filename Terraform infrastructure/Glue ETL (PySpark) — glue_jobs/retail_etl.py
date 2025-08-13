import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, trim, regexp_replace
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, DoubleType)

# Glue boilerplate (works in Glue 4.0)
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME','RAW_S3_PATH','CURATED_S3_PATH'])

spark = SparkSession.builder.appName("RetailSuperstoreETL").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Define schema for common "Superstore" columns (adjust to your CSV) ---
schema = StructType([
    StructField("Order ID", StringType(), True),
    StructField("Order Date", StringType(), True),
    StructField("Ship Date", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Customer Name", StringType(), True),
    StructField("Segment", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("Postal Code", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Product ID", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Sub-Category", StringType(), True),
    StructField("Product Name", StringType(), True),
    StructField("Sales", StringType(), True),
    StructField("Quantity", StringType(), True),
    StructField("Discount", StringType(), True),
    StructField("Profit", StringType(), True),
])

raw = (spark.read
       .option("header", True)
       .schema(schema)
       .csv(args['RAW_S3_PATH']))

# Clean & cast
def as_double(colname):
    # remove currency, commas, etc.
    return regexp_replace(trim(col(colname)), r"[^0-9\.\-]", "").cast(DoubleType())

def as_int(colname):
    return regexp_replace(trim(col(colname)), r"[^0-9\-]", "").cast(IntegerType())

df = (raw
    .withColumn("order_date", to_date(col("Order Date"), "MM/dd/yyyy"))
    .withColumn("ship_date",  to_date(col("Ship Date"), "MM/dd/yyyy"))
    .withColumn("sales",      as_double("Sales"))
    .withColumn("discount",   as_double("Discount"))
    .withColumn("profit",     as_double("Profit"))
    .withColumn("quantity",   as_int("Quantity"))
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .select(
        col("Order ID").alias("order_id"),
        "order_date","ship_date",
        col("Customer ID").alias("customer_id"),
        col("Customer Name").alias("customer_name"),
        "Segment".alias("segment"),
        "City".alias("city"),
        "State".alias("state"),
        "Postal Code".alias("postal_code"),
        "Region".alias("region"),
        col("Product ID").alias("product_id"),
        "Category".alias("category"),
        "Sub-Category".alias("sub_category"),
        col("Product Name").alias("product_name"),
        "sales","quantity","discount","profit",
        "order_year","order_month"
    )
)

# Write curated Parquet partitioned by year/month
(df.write
   .mode("overwrite")
   .format("parquet")
   .partitionBy("order_year","order_month")
   .save(args['CURATED_S3_PATH'])
)

job.commit()
