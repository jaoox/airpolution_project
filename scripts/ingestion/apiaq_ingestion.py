import requests
import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3  # AWS SDK for Python
from minio import Minio  # MinIO SDK for Python
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
API_BASE = "https://api.openaq.org/v3"
REQUEST_DELAY = 2
TIMEOUT = 15  # Seconds before timing out a request
MAX_RETRIES = 2  # Retries per failed request
API_KEY = "cf4349d9377ef4a58c895a306e2cc50e6a011882e45e32dd07d0adce80b13a5b"
headers = {"X-API-Key": API_KEY}
S3_BUCKET_NAME = "openaq-locations-data"  # S3 bucket name (used for both MinIO and AWS S3)
S3_SAVE_DIR = "bronze/locations"  # S3 folder path
MAX_WORKERS = 4  # Parallel threads for location processing

load_dotenv()

# Initialize AWS S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION"),
)

# Initialize MinIO client
minio_client = Minio(
    "minio:9001",  # MinIO server address
    access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),  # MinIO access key
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),  # MinIO secret key
    secure=False  # Use HTTP (not HTTPS)
)

def save_to_s3(data, location_id):
    """Save location data to both MinIO and AWS S3"""
    filename = f"location_{location_id}.json"
    s3_key = f"bronze/locations/{filename}"
    bucket_name = "openaq-locations-data"

    # Save to MinIO
    try:
        # Check if the bucket exists
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            logger.info(f"Created MinIO bucket: {bucket_name}")

        minio_client.put_object(
            bucket_name,
            s3_key,
            json.dumps(data, indent=2),
            len(json.dumps(data, indent=2)),
            content_type="application/json"
        )
        logger.info(f"Saved location {location_id} to MinIO: {s3_key}")
    except Exception as e:
        logger.error(f"Failed to save location {location_id} to MinIO: {str(e)}")

    # Save to AWS S3
    try:
        s3_client.put_object(
            Bucket="openaq-locations-data",
            Key=s3_key,
            Body=json.dumps(data, indent=2),
            ContentType="application/json"
        )
        logger.info(f"Saved location {location_id} to AWS S3: s3://openaq-locations-data/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to save location {location_id} to AWS S3: {str(e)}")

def fetch_with_retry(url, retries=MAX_RETRIES):
    """Fetch with exponential backoff retry"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=TIMEOUT)
            response.raise_for_status()
            return response
        except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
            if attempt < retries - 1:
                wait = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {wait}s... (Attempt {attempt + 1}/{retries})")
                time.sleep(wait)
                continue
            raise

def fetch_location_data(location_id):
    """Fetch data for a single location by ID"""
    url = f"{API_BASE}/locations/{location_id}"
    try:
        response = fetch_with_retry(url)
        data = response.json()
        if "results" in data and data["results"]:
            save_to_s3(data, location_id)
        else:
            print(f"No data for location {location_id}")
    except Exception as e:
        print(f"Failed to fetch location {location_id}: {str(e)}")

def main():
    print(f"Saving data to S3 bucket: {S3_BUCKET_NAME}/{S3_SAVE_DIR}")
    location_ids = range(1, 1501)  # Adjust the range as needed

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_location_data, location_id) for location_id in location_ids]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Location processing failed: {str(e)}")

if __name__ == "__main__":
    start_time = datetime.now()
    try:
        main()
        duration = datetime.now() - start_time
        print(f"Completed in {duration}")
    except Exception as e:
        print(f"Script failed: {str(e)}")
        raise