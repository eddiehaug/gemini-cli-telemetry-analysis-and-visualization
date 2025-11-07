"""
Configuration validation service.
Validates deployment configuration parameters.
"""
import re
import logging

logger = logging.getLogger(__name__)


async def validate_config(config) -> bool:
    """
    Validate deployment configuration.
    Checks:
    - Project ID format
    - Dataset name format
    - Region is valid
    """
    errors = []

    # Validate project ID
    if not config.projectId:
        errors.append("Project ID is required")
    elif not re.match(r'^[a-z][a-z0-9-]{4,28}[a-z0-9]$', config.projectId):
        errors.append("Invalid project ID format. Must be 6-30 characters, lowercase, start with a letter")

    # Validate dataset name
    if not config.datasetName:
        errors.append("Dataset name is required")
    elif not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', config.datasetName):
        errors.append("Invalid dataset name. Must contain only alphanumeric characters and underscores, start with a letter or underscore")

    # Validate region
    valid_regions = [
        'us-central1', 'us-east1', 'us-west1', 'us-west2', 'us-west3', 'us-west4',
        'us-east4', 'us-east5', 'us-south1',
        'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4', 'europe-west6',
        'europe-central2', 'europe-north1',
        'asia-east1', 'asia-east2', 'asia-northeast1', 'asia-northeast2', 'asia-northeast3',
        'asia-south1', 'asia-southeast1', 'asia-southeast2',
        'australia-southeast1', 'australia-southeast2',
        'southamerica-east1', 'southamerica-west1',
        'northamerica-northeast1', 'northamerica-northeast2'
    ]

    if not config.region:
        errors.append("Region is required")
    elif config.region not in valid_regions:
        errors.append(f"Invalid region: {config.region}")

    if errors:
        error_msg = "; ".join(errors)
        logger.error(f"Configuration validation failed: {error_msg}")
        raise Exception(error_msg)

    logger.info(f"Configuration validated successfully for project {config.projectId}")
    return True
