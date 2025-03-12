import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, BooleanType, LongType
from dotenv import load_dotenv


def clean_location_data():
    # Load environment variables
    load_dotenv()
    
    # Validate credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables!")
    
    # Initialize Spark with S3 configurations
    spark = SparkSession.builder \
        .appName("RawToStaging_Locations") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    # Define schema for the location data with corrected types
    location_schema = StructType([
        StructField("bounds", ArrayType(DoubleType())),
        StructField("coordinates", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType())
        ])),
        StructField("country", StructType([
            StructField("code", StringType()),
            StructField("id", LongType()),
            StructField("name", StringType())
        ])),
        StructField("datetimeFirst", StructType([
            StructField("local", StringType()),
            StructField("utc", StringType())
        ])),
        StructField("datetimeLast", StructType([
            StructField("local", StringType()),
            StructField("utc", StringType())
        ])),
        StructField("distance", StringType()),
        StructField("id", LongType()),
        StructField("instruments", ArrayType(StructType([
            StructField("id", LongType()),
            StructField("name", StringType())
        ]))),
        StructField("isMobile", BooleanType()),
        StructField("isMonitor", BooleanType()),
        StructField("licenses", ArrayType(StructType([
            StructField("attribution", StructType([
                StructField("name", StringType()),
                StructField("url", StringType())
            ])),
            StructField("dateFrom", StringType()),
            StructField("dateTo", StringType()),
            StructField("id", LongType()),
            StructField("name", StringType())
        ]))),
        StructField("locality", StringType()),
        StructField("name", StringType()),
        StructField("owner", StructType([
            StructField("id", LongType()),
            StructField("name", StringType())
        ])),
        StructField("provider", StructType([
            StructField("id", LongType()),
            StructField("name", StringType())
        ])),
        StructField("sensors", ArrayType(StructType([
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("parameter", StructType([
                StructField("displayName", StringType()),
                StructField("id", LongType()),
                StructField("name", StringType()),
                StructField("units", StringType())
            ]))
        ]))),
        StructField("timezone", StringType())
    ])

    # Define the top-level schema
    schema = StructType([
        StructField("meta", StructType([
            StructField("found", LongType()),
            StructField("limit", LongType()),
            StructField("name", StringType()),
            StructField("page", LongType()),
            StructField("website", StringType())
        ])),
        StructField("results", ArrayType(location_schema))
    ])

    # S3 paths
    bronze_path = "s3a://openaq-locations-data/bronze/locations/*.json"  # Replace with your S3 bronze path
    silver_path = "s3a://openaq-locations-data/silver/locations"  # Replace with your S3 silver path

    # Reading JSON with the updated schema from S3
    raw_df = spark.read \
        .option("multiline", "true") \
        .schema(schema) \
        .json(bronze_path)

    print(f"Raw data count: {raw_df.count()}")
    raw_df.show(5, truncate=False)

    # Explode the array into individual rows
    processed_df = raw_df.select(explode("results").alias("location")).select(
        col("location.id"),
        col("location.name"),
        col("location.locality"),
        col("location.timezone"),
        col("location.country.name").alias("country"),
        col("location.provider.name").alias("provider"),
        col("location.isMobile"),
        col("location.isMonitor"),
        col("location.coordinates.latitude").alias("lat"),
        col("location.coordinates.longitude").alias("lon")
    )

    print(f"Processed data count: {processed_df.count()}")
    processed_df.show(5, truncate=False)

    # Clean the data: remove duplicates and invalid rows
    cleaned_df = processed_df \
        .dropDuplicates(["id"]) \
        .na.drop(subset=["id", "name", "country", "lat", "lon"])

    print(f"Cleaned data count: {cleaned_df.count()}")
    cleaned_df.show(5, truncate=False)

    # Write the cleaned data to S3
    cleaned_df.write \
        .mode("overwrite") \
        .parquet(silver_path)

    print(f"Cleaned data written to S3: {silver_path}")

if __name__ == "__main__":
        
    clean_location_data()