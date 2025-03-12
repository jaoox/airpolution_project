from fastapi import FastAPI, HTTPException
from minio import Minio
import boto3
import os
from dotenv import load_dotenv
from datetime import timedelta  # <-- Add this import

# Load environment variables
load_dotenv()

# Initialize MinIO client
minio_client = Minio(
    "minio:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False
)

# Initialize AWS S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

app = FastAPI()

@app.get("/")
async def read_root():
    return {"message": "API Gateway is working!"}

@app.get("/health")
async def health_check():
    """Check health of MinIO and S3"""
    try:
        minio_health = minio_client.bucket_exists("openaq-locations-data")
        s3_health = s3_client.list_buckets()  # Simple check
        return {
            "minio": "OK" if minio_health else "Degraded",
            "s3": "OK" if s3_health else "Degraded"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/raw")
async def get_raw_data(object_name: str):
    """Get presigned URL for raw data from MinIO or S3"""
    try:
        # MinIO (unchanged)
        minio_url = minio_client.presigned_get_object(
            "openaq-locations-data",
            f"bronze/locations/{object_name}",
            expires=3600  # Integer is OK for MinIO
        )
        # AWS S3 (fix ExpiresIn)
        s3_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": "openaq-locations-data",
                "Key": f"bronze/locations/{object_name}"
            },
            ExpiresIn=timedelta(seconds=3600)  # Use timedelta here
        )
        return {
            "minio_url": minio_url,
            "s3_url": s3_url
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/curated")
async def get_curated_data(object_name: str):
    """Get presigned URL for curated data from MinIO or S3"""
    try:
        # MinIO (unchanged)
        minio_url = minio_client.presigned_get_object(
            "openaq-locations-data",
            f"gold/{object_name}",
            expires=3600  # Integer is OK for MinIO
        )
        # AWS S3 (fix ExpiresIn)
        s3_url = s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": "openaq-locations-data",
                "Key": f"gold/{object_name}"
            },
            ExpiresIn=timedelta(seconds=3600)  # Use timedelta here
        )
        return {
            "minio_url": minio_url,
            "s3_url": s3_url
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get statistics about data in MinIO and S3"""
    try:
        # MinIO stats
        minio_objects = list(minio_client.list_objects("openaq-locations-data", recursive=True))
        minio_stats = {
            "raw_count": len([obj for obj in minio_objects if obj.object_name.startswith("bronze/")]),
            "curated_count": len([obj for obj in minio_objects if obj.object_name.startswith("gold/")])
        }
        # S3 stats
        s3_objects = s3_client.list_objects_v2(Bucket="openaq-locations-data")
        s3_stats = {
            "raw_count": len([obj for obj in s3_objects.get("Contents", []) if obj["Key"].startswith("bronze/")]),
            "curated_count": len([obj for obj in s3_objects.get("Contents", []) if obj["Key"].startswith("gold/")])
        }
        return {
            "minio": minio_stats,
            "s3": s3_stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))