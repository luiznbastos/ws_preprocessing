import logging
import os
import io
import boto3
from botocore.exceptions import ClientError
from typing import Tuple
import pandas as pd

logger = logging.getLogger(__name__)


def object_exists(s3_client, bucket_name, key):
    """Check if an object exists in S3 bucket"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def get_s3_client():
    """Get configured AWS S3 client"""
    region = os.environ.get("AWS_REGION", "us-east-1")
    session = boto3.Session(region_name=region)
    return session.client("s3")


def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Parse S3 path into bucket and key components
    
    Args:
        s3_path: Full S3 path in format s3://bucket/key
        
    Returns:
        Tuple of (bucket, key)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path format: {s3_path}")
    
    path_without_prefix = s3_path[5:]
    parts = path_without_prefix.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    
    return bucket, key


def write_parquet_to_s3(df: pd.DataFrame, s3_path: str, s3_client) -> None:
    """Write DataFrame to S3 as Parquet using pyarrow
    
    Args:
        df: DataFrame to write
        s3_path: Full S3 path (s3://bucket/key)
        s3_client: Boto3 S3 client
    """
    if df.empty:
        logger.warning(f"DataFrame is empty, skipping write to {s3_path}")
        return
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
    buffer.seek(0)
    
    bucket, key = parse_s3_path(s3_path)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    logger.info(f"Wrote {len(df)} rows to S3 Parquet: {s3_path}")


def parquet_exists_on_s3(s3_path: str, s3_client) -> bool:
    """Check if Parquet file exists on S3
    
    Args:
        s3_path: Full S3 path (s3://bucket/key)
        s3_client: Boto3 S3 client
        
    Returns:
        True if file exists, False otherwise
    """
    bucket, key = parse_s3_path(s3_path)
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise