import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min
from dotenv import load_dotenv

def transform_location_data():
    # Load environment variables
    load_dotenv()

    # Validate credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables!")

    # Initialize Spark session with S3 configurations
    spark = SparkSession.builder \
        .appName("TransformLocationData") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    # S3 paths
    silver_path = "s3a://openaq-locations-data/silver/locations"  # Replace with your S3 silver path
    gold_path = "s3a://openaq-locations-data/gold"  # Replace with your S3 gold path

    # Read the cleaned data from the silver layer
    cleaned_df = spark.read.parquet(silver_path)

    # Show the cleaned data for reference
    print("Cleaned Data:")
    cleaned_df.show(5, truncate=False)

    ### Transformation 1: Enrich Data with Country Code and Timezone
    # Add a new column `country_code` and `timezone` for easier analysis
    enriched_df = cleaned_df.withColumn("country_code", col("country")) \
                            .withColumn("timezone", col("timezone"))

    ### Transformation 2: Aggregate Data by Country
    # Count the number of locations per country and calculate average latitude/longitude
    country_stats_df = enriched_df.groupBy("country_code") \
        .agg(
            count("id").alias("location_count"),
            avg("lat").alias("avg_latitude"),
            avg("lon").alias("avg_longitude")
        )

    print("Country Statistics:")
    country_stats_df.show(5, truncate=False)

    ### Transformation 3: Identify Mobile and Monitor Locations
    # Filter and count mobile and monitor locations
    mobile_locations_df = enriched_df.filter(col("isMobile") == True) \
        .select("id", "name", "country_code", "lat", "lon")

    monitor_locations_df = enriched_df.filter(col("isMonitor") == True) \
        .select("id", "name", "country_code", "lat", "lon")

    print("Mobile Locations:")
    mobile_locations_df.show(5, truncate=False)

    print("Monitor Locations:")
    monitor_locations_df.show(5, truncate=False)

    ### Transformation 4: Calculate Geographic Bounds per Country
    # Find the min and max latitude/longitude for each country
    geographic_bounds_df = enriched_df.groupBy("country_code") \
        .agg(
            min("lat").alias("min_latitude"),
            max("lat").alias("max_latitude"),
            min("lon").alias("min_longitude"),
            max("lon").alias("max_longitude")
        )

    print("Geographic Bounds per Country:")
    geographic_bounds_df.show(5, truncate=False)

    ### Transformation 5: Create a Dataset for Provider Analysis
    # Count the number of locations per provider
    provider_stats_df = enriched_df.groupBy("provider") \
        .agg(
            count("id").alias("location_count")
        )

    print("Provider Statistics:")
    provider_stats_df.show(5, truncate=False)

    ### Write Transformed Data to Gold Layer
    # Write each transformed dataset to the gold layer in S3
    country_stats_df.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/country_stats")

    mobile_locations_df.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/mobile_locations")

    monitor_locations_df.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/monitor_locations")

    geographic_bounds_df.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/geographic_bounds")

    provider_stats_df.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/provider_stats")

    print(f"Transformed data written to S3: {gold_path}")

if __name__ == "__main__":
    transform_location_data()