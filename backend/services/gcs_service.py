"""
GCS (Google Cloud Storage) service for Gemini CLI telemetry deployment.

This module handles creation of GCS buckets and uploading files for Dataflow:
- Create bucket for Dataflow staging/temp files
- Upload JavaScript UDF (transform.js) for Dataflow transformation
- Set appropriate bucket permissions

Per IMPLEMENTATION_PLAN.md Step 8b:
- Bucket naming: {project_id}-dataflow
- UDF file: transform.js (for JSON transformation in Dataflow)
- Public access: Disabled (private bucket)
"""

import logging
import os
from typing import Dict
from google.cloud import storage
from google.api_core.exceptions import Conflict, NotFound
from utils.validators import (
    validate_gcp_project_id,
    validate_bucket_name,
    validate_region,
    ValidationError
)

logger = logging.getLogger(__name__)


async def create_bucket(
    project_id: str,
    bucket_name: str = None,
    region: str = "us-central1"
) -> Dict:
    """
    Create a GCS bucket for Dataflow staging and temp files.

    Args:
        project_id: GCP project ID
        bucket_name: Name of the bucket to create (default: {project_id}-dataflow)
        region: GCS bucket region (default: us-central1)

    Returns:
        Dict with bucket details:
        {
            "bucket": bucket_name,
            "region": region,
            "status": "created" or "already_exists",
            "url": "gs://{bucket_name}"
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Default bucket name if not provided
        if not bucket_name:
            bucket_name = f"{project_id}-dataflow"

        # Validate bucket name
        try:
            bucket_name = validate_bucket_name(bucket_name)
        except ValidationError as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise ValueError(f"Invalid input: {str(e)}")

        storage_client = storage.Client(project=project_id)

        try:
            # Attempt to create the bucket
            bucket = storage_client.create_bucket(
                bucket_name,
                location=region
            )

            # Disable public access
            bucket.iam_configuration.uniform_bucket_level_access_enabled = True
            bucket.patch()

            logger.info(f"✓ GCS bucket created: gs://{bucket_name}")
            logger.info(f"  - Region: {region}")
            logger.info(f"  - Public access: Disabled")

            return {
                "bucket": bucket_name,
                "region": region,
                "status": "created",
                "url": f"gs://{bucket_name}",
                "note": f"Bucket {bucket_name} created for Dataflow staging"
            }

        except Conflict:
            logger.info(f"Bucket gs://{bucket_name} already exists")

            return {
                "bucket": bucket_name,
                "region": region,
                "status": "already_exists",
                "url": f"gs://{bucket_name}",
                "note": f"Bucket {bucket_name} already exists and will be reused"
            }

    except Exception as e:
        logger.error(f"Failed to create GCS bucket: {str(e)}")
        raise


async def upload_udf(
    project_id: str,
    bucket_name: str = None,
    local_file_path: str = "transform.js",
    destination_blob_name: str = "transform.js"
) -> Dict:
    """
    Upload JavaScript UDF file to GCS bucket for Dataflow transformation.

    Checks if file already exists before uploading. If it exists, skips upload.

    Args:
        project_id: GCP project ID
        bucket_name: Name of the bucket to upload to (default: {project_id}-dataflow)
        local_file_path: Path to local UDF file (default: transform.js)
        destination_blob_name: Name of the file in GCS (default: transform.js)

    Returns:
        Dict with upload details:
        {
            "bucket": bucket_name,
            "file": destination_blob_name,
            "url": "gs://{bucket_name}/{file}",
            "status": "uploaded" or "already_exists",
            "size_bytes": file_size
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Default bucket name if not provided
        if not bucket_name:
            bucket_name = f"{project_id}-dataflow"

        # Validate bucket name
        try:
            bucket_name = validate_bucket_name(bucket_name)
        except ValidationError as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise ValueError(f"Invalid input: {str(e)}")

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Check if file already exists in GCS
        if blob.exists():
            file_size = blob.size
            logger.info(f"UDF file gs://{bucket_name}/{destination_blob_name} already exists, skipping upload")
            logger.info(f"  - Size: {file_size} bytes")

            return {
                "bucket": bucket_name,
                "file": destination_blob_name,
                "url": f"gs://{bucket_name}/{destination_blob_name}",
                "status": "already_exists",
                "size_bytes": file_size,
                "note": f"UDF {destination_blob_name} already exists and will be reused"
            }

        # Verify local file exists
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"UDF file not found: {local_file_path}")

        # Upload the file
        blob.upload_from_filename(local_file_path)

        # Get file size
        file_size = os.path.getsize(local_file_path)

        logger.info(f"✓ UDF uploaded: gs://{bucket_name}/{destination_blob_name}")
        logger.info(f"  - Size: {file_size} bytes")
        logger.info(f"  - Local file: {local_file_path}")

        return {
            "bucket": bucket_name,
            "file": destination_blob_name,
            "url": f"gs://{bucket_name}/{destination_blob_name}",
            "status": "uploaded",
            "size_bytes": file_size,
            "note": f"UDF {destination_blob_name} ready for Dataflow pipeline"
        }

    except Exception as e:
        logger.error(f"Failed to upload UDF: {str(e)}")
        raise


async def verify_bucket_exists(
    project_id: str,
    bucket_name: str = None
) -> bool:
    """
    Verify that a GCS bucket exists.

    Args:
        project_id: GCP project ID
        bucket_name: Name of the bucket to verify (default: {project_id}-dataflow)

    Returns:
        True if bucket exists, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Default bucket name if not provided
        if not bucket_name:
            bucket_name = f"{project_id}-dataflow"

        # Validate bucket name
        try:
            bucket_name = validate_bucket_name(bucket_name)
        except ValidationError as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise ValueError(f"Invalid input: {str(e)}")

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)

        # Check if bucket exists
        bucket.reload()
        logger.info(f"Bucket gs://{bucket_name} exists")
        return True

    except NotFound:
        logger.warning(f"Bucket gs://{bucket_name} not found")
        return False
    except Exception as e:
        logger.error(f"Bucket verification failed: {str(e)}")
        return False


async def verify_file_exists(
    project_id: str,
    bucket_name: str = None,
    file_name: str = "transform.js"
) -> bool:
    """
    Verify that a file exists in a GCS bucket.

    Args:
        project_id: GCP project ID
        bucket_name: Name of the bucket (default: {project_id}-dataflow)
        file_name: Name of the file to verify (default: transform.js)

    Returns:
        True if file exists, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Default bucket name if not provided
        if not bucket_name:
            bucket_name = f"{project_id}-dataflow"

        # Validate bucket name
        try:
            bucket_name = validate_bucket_name(bucket_name)
        except ValidationError as e:
            logger.error(f"Input validation failed: {str(e)}")
            raise ValueError(f"Invalid input: {str(e)}")

        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Check if file exists
        exists = blob.exists()

        if exists:
            logger.info(f"File gs://{bucket_name}/{file_name} exists")
        else:
            logger.warning(f"File gs://{bucket_name}/{file_name} not found")

        return exists

    except Exception as e:
        logger.error(f"File verification failed: {str(e)}")
        return False


async def setup_gcs_for_dataflow(
    project_id: str,
    region: str = "us-central1",
    local_udf_path: str = "transform.js"
) -> Dict:
    """
    Set up GCS bucket and upload UDF for Dataflow pipeline.

    This is a convenience function that:
    1. Creates the GCS bucket
    2. Uploads the JavaScript UDF file
    3. Returns complete setup details

    Args:
        project_id: GCP project ID
        region: GCS bucket region (default: us-central1)
        local_udf_path: Path to local UDF file (default: transform.js)

    Returns:
        Dict with all setup details:
        {
            "bucket": {...},
            "udf": {...},
            "dataflow_params": {
                "staging_location": "gs://{bucket}/temp",
                "udf_gcs_path": "gs://{bucket}/transform.js"
            }
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        result = {}
        bucket_name = f"{project_id}-dataflow"

        # Create bucket
        bucket_result = await create_bucket(project_id, bucket_name, region)
        result["bucket"] = bucket_result

        # Upload UDF
        udf_result = await upload_udf(project_id, bucket_name, local_udf_path)
        result["udf"] = udf_result

        # Add Dataflow-specific parameters
        result["dataflow_params"] = {
            "staging_location": f"gs://{bucket_name}/temp",
            "udf_gcs_path": f"gs://{bucket_name}/transform.js",
            "note": "Use these parameters when deploying Dataflow pipeline"
        }

        logger.info(f"✓ GCS setup complete for Dataflow")
        logger.info(f"  - Bucket: gs://{bucket_name}")
        logger.info(f"  - UDF: gs://{bucket_name}/transform.js")
        logger.info(f"  - Staging: gs://{bucket_name}/temp")

        return result

    except Exception as e:
        logger.error(f"Failed to set up GCS for Dataflow: {str(e)}")
        raise
