#!/usr/bin/env python3
"""
Carris GTFS Saver - Downloads GTFS data from Carris and uploads to S3 with hash-based change detection
"""

import os
import sys
import hashlib
import requests
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
GTFS_URL = os.environ.get("GTFS_URL", "")
AGENCY_ID = os.environ.get("AGENCY_ID", "1")
LOCAL_GTFS_FILE = "GTFS_Carris.zip"
S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
S3_PREFIX = "gtfs/"  # S3 prefix for organizing files


def calculate_file_hash(file_path):
    """Calculate SHA256 hash of a file"""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            # Read file in chunks to handle large files efficiently
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash: {e}")
        raise


def download_gtfs_file(url, default_filename="GTFS_Carris.zip"):
    """Download GTFS file from Carris and return the filename"""
    logger.info(f"Downloading GTFS file from {url}...")
    try:
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        
        # Extract filename from Content-Disposition header if available
        filename = default_filename
        if 'Content-Disposition' in response.headers:
            content_disposition = response.headers['Content-Disposition']
            if 'filename=' in content_disposition:
                # Extract filename from header (e.g., 'attachment; filename="file.zip"')
                import re
                matches = re.findall(r'filename="?([^"]+)"?', content_disposition)
                if matches:
                    filename = matches[0]
                    logger.info(f"Using filename from server: {filename}")
        
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded GTFS file to {filename}")
        return filename
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading GTFS file: {e}")
        raise


def get_s3_client():
    """Initialize and return S3 client"""
    try:
        # Configure S3 client with custom endpoint (Scaleway or other S3-compatible storage)
        config_params = {
            'service_name': 's3',
            'region_name': S3_REGION
        }
        
        # Add custom endpoint if provided (for S3-compatible services like Scaleway)
        if S3_ENDPOINT_URL:
            config_params['endpoint_url'] = S3_ENDPOINT_URL
        
        # Add custom credentials if provided
        if S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY:
            config_params['aws_access_key_id'] = S3_ACCESS_KEY_ID
            config_params['aws_secret_access_key'] = S3_SECRET_ACCESS_KEY
        
        s3_client = boto3.client(**config_params)
        return s3_client
    except Exception as e:
        logger.error(f"Error creating S3 client: {e}")
        raise


def get_remote_hash(s3_client, bucket_name, hash_key):
    """Retrieve the stored hash from S3"""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=hash_key)
        remote_hash = response['Body'].read().decode('utf-8').strip()
        logger.info(f"Retrieved remote hash: {remote_hash}")
        return remote_hash
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            logger.info("No previous hash found in S3")
            return None
        else:
            logger.error(f"Error retrieving remote hash: {e}")
            raise


def upload_to_s3(s3_client, bucket_name, file_path, s3_key):
    """Upload file to S3"""
    try:
        logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}...")
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info("Upload successful")
        return True
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")
        raise


def save_hash_to_s3(s3_client, bucket_name, hash_key, hash_value):
    """Save hash value to S3"""
    try:
        logger.info(f"Saving hash to s3://{bucket_name}/{hash_key}...")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=hash_key,
            Body=hash_value.encode('utf-8'),
            ContentType='text/plain'
        )
        logger.info("Hash saved successfully")
        return True
    except Exception as e:
        logger.error(f"Error saving hash to S3: {e}")
        raise


def cleanup_local_file(file_path):
    """Remove local file"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Cleaned up local file: {file_path}")
    except Exception as e:
        logger.warning(f"Could not remove local file {file_path}: {e}")


def check_s3_file_exists(s3_client, bucket_name, s3_key):
    """Check if a file exists in S3"""
    try:
        s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise


def generate_unique_s3_key(s3_client, bucket_name, original_key):
    """Generate a unique S3 key by adding timestamp if file exists"""
    # If file doesn't exist, return original key
    if not check_s3_file_exists(s3_client, bucket_name, original_key):
        return original_key
    
    # File exists, generate new key with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Parse the original key to extract path and filename
    parts = original_key.rsplit('/', 1)
    if len(parts) == 2:
        prefix, filename = parts
    else:
        prefix = ""
        filename = original_key
    
    # Insert timestamp before the file extension
    name_parts = filename.rsplit('.', 1)
    if len(name_parts) == 2:
        new_filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
    else:
        new_filename = f"{filename}_{timestamp}"
    
    new_key = f"{prefix}/{new_filename}" if prefix else new_filename
    
    # Check if this timestamped key also exists (unlikely, but safe)
    if check_s3_file_exists(s3_client, bucket_name, new_key):
        logger.warning(f"Timestamped key {new_key} already exists, adding counter")
        counter = 1
        while True:
            if len(name_parts) == 2:
                new_filename = f"{name_parts[0]}_{timestamp}_{counter}.{name_parts[1]}"
            else:
                new_filename = f"{filename}_{timestamp}_{counter}"
            new_key = f"{prefix}/{new_filename}" if prefix else new_filename
            if not check_s3_file_exists(s3_client, bucket_name, new_key):
                break
            counter += 1
    
    logger.info(f"File {original_key} exists in S3, will upload as {new_key}")
    return new_key


def main():
    """Main execution function"""
    # Validate environment variables
    if not S3_BUCKET_NAME:
        logger.error("S3_BUCKET_NAME environment variable is not set")
        sys.exit(1)
    
    if S3_ENDPOINT_URL and not (S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY):
        logger.error("When using custom S3_ENDPOINT_URL, both S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY are required")
        sys.exit(1)
    
    if not GTFS_URL:
        logger.error("GTFS_URL environment variable is not set")
        sys.exit(1)
    
    logger.info("=== Carris GTFS Saver Started ===")
    logger.info(f"Target S3 bucket: {S3_BUCKET_NAME}")
    logger.info(f"GTFS URL: {GTFS_URL}")
    
    try:
        # Step 1: Download GTFS file from Carris
        local_file = download_gtfs_file(GTFS_URL, LOCAL_GTFS_FILE)
        
        # Construct S3 keys
        s3_gtfs_key = f"{S3_PREFIX}{local_file}"
        s3_hash_key = f"{S3_PREFIX}hash.txt"  # Always use hash.txt
        
        logger.info(f"S3 GTFS key: {s3_gtfs_key}")
        logger.info(f"S3 hash key: {s3_hash_key}")
        
        # Step 2: Calculate hash of downloaded file
        local_hash = calculate_file_hash(local_file)
        logger.info(f"Local file hash: {local_hash}")
        
        # Step 3: Initialize S3 client
        s3_client = get_s3_client()
        
        # Step 4: Get remote hash from S3
        remote_hash = get_remote_hash(s3_client, S3_BUCKET_NAME, s3_hash_key)
        
        # Step 5: Compare hashes
        if local_hash == remote_hash:
            logger.info("File hash matches remote hash - no changes detected, skipping upload")
            cleanup_local_file(local_file)
            logger.info("=== Carris GTFS Saver Completed (No Update Needed) ===")
            return
        
        logger.info("File has changed - proceeding with upload")
        
        # Step 6: Generate unique S3 key (adds timestamp if file exists)
        final_s3_key = generate_unique_s3_key(s3_client, S3_BUCKET_NAME, s3_gtfs_key)
        
        # Step 7: Upload GTFS file to S3
        upload_to_s3(s3_client, S3_BUCKET_NAME, local_file, final_s3_key)
        
        # Step 8: Save new hash to S3
        save_hash_to_s3(s3_client, S3_BUCKET_NAME, s3_hash_key, local_hash)
        
        # Step 8: Cleanup local file
        cleanup_local_file(local_file)
        
        logger.info("=== Carris GTFS Saver Completed Successfully ===")
        
    except Exception as e:
        logger.error(f"Script failed with error: {e}")
        # Try to cleanup any downloaded file
        if 'local_file' in locals():
            cleanup_local_file(local_file)
        sys.exit(1)


if __name__ == "__main__":
    main()
