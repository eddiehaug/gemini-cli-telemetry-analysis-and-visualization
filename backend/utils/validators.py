"""
Input validation utilities for GCP identifiers.

This module provides validation functions to prevent administrator mistakes
when deploying telemetry infrastructure to Google Cloud Platform.

All validators follow GCP naming conventions and return the validated value
or raise ValidationError with a helpful error message.
"""
import re
from typing import List


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


def validate_gcp_project_id(project_id: str) -> str:
    """
    Validate GCP project ID format.

    GCP project IDs must:
    - Be 6-30 characters
    - Start with lowercase letter
    - Contain only lowercase letters, digits, hyphens
    - Not end with hyphen

    Args:
        project_id: Project ID to validate

    Returns:
        Validated project ID

    Raises:
        ValidationError: If project ID format is invalid
    """
    if not project_id:
        raise ValidationError("Project ID cannot be empty")

    pattern = r'^[a-z][a-z0-9-]{4,28}[a-z0-9]$'
    if not re.match(pattern, project_id):
        raise ValidationError(
            f"Invalid GCP project ID: '{project_id}'. "
            f"Must be 6-30 characters, start with lowercase letter, "
            f"contain only lowercase letters, digits, hyphens."
        )
    return project_id


def validate_dataset_name(dataset_name: str) -> str:
    """
    Validate BigQuery dataset name.

    Args:
        dataset_name: Dataset name to validate

    Returns:
        Validated dataset name

    Raises:
        ValidationError: If dataset name is invalid
    """
    if not dataset_name:
        raise ValidationError("Dataset name cannot be empty")

    pattern = r'^[a-zA-Z0-9_]{1,1024}$'
    if not re.match(pattern, dataset_name):
        raise ValidationError(
            f"Invalid dataset name: '{dataset_name}'. "
            f"Must contain only letters, digits, underscores (1-1024 chars)."
        )
    return dataset_name


def validate_region(region: str) -> str:
    """
    Validate GCP region name.

    Args:
        region: Region name to validate

    Returns:
        Validated region name

    Raises:
        ValidationError: If region format is invalid
    """
    if not region:
        raise ValidationError("Region cannot be empty")

    # Common GCP region pattern: us-central1, europe-west1, etc.
    pattern = r'^[a-z]+-[a-z]+[0-9]+$'
    if not re.match(pattern, region):
        raise ValidationError(
            f"Invalid region: '{region}'. "
            f"Expected format: 'us-central1', 'europe-west1', etc."
        )
    return region


def validate_network_name(network_name: str) -> str:
    """
    Validate VPC network name.

    Args:
        network_name: Network name to validate

    Returns:
        Validated network name

    Raises:
        ValidationError: If network name is invalid
    """
    if not network_name:
        raise ValidationError("Network name cannot be empty")

    # RFC 1035: lowercase letters, digits, hyphens, 1-63 chars
    pattern = r'^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$'
    if not re.match(pattern, network_name):
        raise ValidationError(
            f"Invalid network name: '{network_name}'. "
            f"Must be lowercase letters, digits, hyphens (1-63 chars)."
        )
    return network_name


def validate_bucket_name(bucket_name: str) -> str:
    """
    Validate GCS bucket name.

    Args:
        bucket_name: Bucket name to validate

    Returns:
        Validated bucket name

    Raises:
        ValidationError: If bucket name is invalid
    """
    if not bucket_name:
        raise ValidationError("Bucket name cannot be empty")

    # GCS bucket naming rules
    pattern = r'^[a-z0-9][a-z0-9_-]{1,61}[a-z0-9]$'
    if not re.match(pattern, bucket_name):
        raise ValidationError(
            f"Invalid bucket name: '{bucket_name}'. "
            f"Must be lowercase letters, digits, hyphens, underscores (3-63 chars)."
        )
    if '..' in bucket_name or bucket_name.startswith('goog'):
        raise ValidationError(f"Invalid bucket name: '{bucket_name}'")
    return bucket_name


def validate_topic_name(topic_name: str) -> str:
    """
    Validate Pub/Sub topic name.

    Args:
        topic_name: Topic name to validate

    Returns:
        Validated topic name

    Raises:
        ValidationError: If topic name is invalid
    """
    if not topic_name:
        raise ValidationError("Topic name cannot be empty")

    # Pub/Sub topic naming rules
    pattern = r'^[a-zA-Z][a-zA-Z0-9_-]{2,254}$'
    if not re.match(pattern, topic_name):
        raise ValidationError(
            f"Invalid topic name: '{topic_name}'. "
            f"Must start with letter, contain letters/digits/hyphens/underscores (3-255 chars)."
        )
    return topic_name


def validate_table_name(table_name: str) -> str:
    """
    Validate BigQuery table name.

    Args:
        table_name: Table name to validate

    Returns:
        Validated table name

    Raises:
        ValidationError: If table name is invalid
    """
    if not table_name:
        raise ValidationError("Table name cannot be empty")

    pattern = r'^[a-zA-Z0-9_]{1,1024}$'
    if not re.match(pattern, table_name):
        raise ValidationError(
            f"Invalid table name: '{table_name}'. "
            f"Must contain only letters, digits, underscores (1-1024 chars)."
        )
    return table_name


def validate_view_name(view_name: str) -> str:
    """
    Validate BigQuery view name (same rules as table).

    Args:
        view_name: View name to validate

    Returns:
        Validated view name

    Raises:
        ValidationError: If view name is invalid
    """
    return validate_table_name(view_name)
