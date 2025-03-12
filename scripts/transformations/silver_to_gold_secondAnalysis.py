import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from dotenv import load_dotenv

def analyze_sensor_instrument_data():
    # Load environment variables
    load_dotenv()

    # Validate credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not aws_access_key or not aws_secret_key:
        raise ValueError("AWS credentials not found in environment variables!")

    # Initialize Spark session with S3 configurations
    spark = SparkSession.builder \
        .appName("AnalyzeSensorInstrumentData") \
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
    print("Cleaned Data Schema:")
    cleaned_df.printSchema()

    print("Cleaned Data Sample:")
    cleaned_df.show(5, truncate=False)

    ### Check if the `sensors` column exists
    if "sensors" in cleaned_df.columns:
        ### Transformation 1: Explode Sensors
        exploded_sensors_df = cleaned_df.select(
            col("id").alias("location_id"),
            col("name").alias("location_name"),
            col("country").alias("country_name"),
            explode("sensors").alias("sensor")
        ).select(
            col("location_id"),
            col("location_name"),
            col("country_name"),
            col("sensor.id").alias("sensor_id"),
            col("sensor.name").alias("sensor_name"),
            col("sensor.parameter.displayName").alias("parameter_name"),
            col("sensor.parameter.units").alias("parameter_units")
        )

        print("Exploded Sensors Data:")
        exploded_sensors_df.show(5, truncate=False)

        ### Transformation 2: Sensor Statistics
        sensor_stats_df = exploded_sensors_df.groupBy("location_id", "location_name", "parameter_name") \
            .agg(
                count("sensor_id").alias("sensor_count")
            )

        print("Sensor Statistics:")
        sensor_stats_df.show(5, truncate=False)

        ### Transformation 3: Parameter Analysis
        parameter_stats_df = exploded_sensors_df.groupBy("parameter_name", "parameter_units") \
            .agg(
                count("sensor_id").alias("sensor_count")
            ).orderBy(col("sensor_count").desc())

        print("Parameter Statistics:")
        parameter_stats_df.show(5, truncate=False)

        ### Write Sensor Data to Gold Layer
        exploded_sensors_df.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/exploded_sensors")

        sensor_stats_df.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/sensor_stats")

        parameter_stats_df.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/parameter_stats")
    else:
        print("The `sensors` column is missing in the cleaned data. Skipping sensor-related transformations.")

    ### Check if the `instruments` column exists
    if "instruments" in cleaned_df.columns:
        ### Transformation 4: Explode Instruments
        exploded_instruments_df = cleaned_df.select(
            col("id").alias("location_id"),
            col("name").alias("location_name"),
            col("country").alias("country_name"),
            explode("instruments").alias("instrument")
        ).select(
            col("location_id"),
            col("location_name"),
            col("country_name"),
            col("instrument.id").alias("instrument_id"),
            col("instrument.name").alias("instrument_name")
        )

        print("Exploded Instruments Data:")
        exploded_instruments_df.show(5, truncate=False)

        ### Transformation 5: Instrument Statistics
        instrument_stats_df = exploded_instruments_df.groupBy("location_id", "location_name") \
            .agg(
                count("instrument_id").alias("instrument_count")
            )

        print("Instrument Statistics:")
        instrument_stats_df.show(5, truncate=False)

        ### Write Instrument Data to Gold Layer
        exploded_instruments_df.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/exploded_instruments")

        instrument_stats_df.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/instrument_stats")
    else:
        print("The `instruments` column is missing in the cleaned data. Skipping instrument-related transformations.")

    print(f"Transformed data written to S3: {gold_path}")

if __name__ == "__main__":
    analyze_sensor_instrument_data()