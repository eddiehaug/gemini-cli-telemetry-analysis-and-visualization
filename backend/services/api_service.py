"""
API enablement service.
Enables required GCP APIs and verifies propagation.
"""
import subprocess
import asyncio
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Required APIs for the deployment
# Updated for ELT architecture: Pub/Sub -> Dataflow -> BigQuery
REQUIRED_APIS = [
    "bigquery.googleapis.com",           # BigQuery dataset/table operations
    "logging.googleapis.com",            # Cloud Logging sink creation
    "pubsub.googleapis.com",             # Pub/Sub topic/subscription
    "dataflow.googleapis.com",           # Dataflow pipeline deployment
    "datapipelines.googleapis.com",      # Data Pipelines API (required by Dataflow)
    "cloudscheduler.googleapis.com",     # Cloud Scheduler API (required by Dataflow)
    "compute.googleapis.com",            # Compute Engine (required for Dataflow workers)
    "storage.googleapis.com",            # GCS bucket for UDF storage
    "cloudresourcemanager.googleapis.com",  # Project resource management
    "serviceusage.googleapis.com",       # API enablement service
    "bigquerydatatransfer.googleapis.com",  # BigQuery Data Transfer for scheduled queries
    "aiplatform.googleapis.com",         # Vertex AI API for ML.GENERATE_TEXT in analytics views
]


async def enable_apis(project_id: str) -> Dict:
    """
    Enable required GCP APIs and verify they're accessible.

    Returns:
        Dict with success, enabled list, and failed list
    """
    enabled = []
    failed = []

    try:
        # Check which APIs are already enabled
        currently_enabled = await get_enabled_apis(project_id)
        logger.info(f"Currently enabled APIs: {len(currently_enabled)}")

        for api in REQUIRED_APIS:
            if api in currently_enabled:
                logger.info(f"API already enabled: {api}")
                enabled.append(api)
            else:
                # Enable the API
                logger.info(f"Enabling API: {api}")
                success = await enable_api(project_id, api)

                if success:
                    enabled.append(api)
                else:
                    failed.append(api)

        # If we enabled any APIs, wait for propagation
        newly_enabled = [api for api in enabled if api not in currently_enabled]
        if newly_enabled:
            logger.info(f"Waiting for API propagation ({len(newly_enabled)} APIs)...")
            await asyncio.sleep(30)  # Wait 30 seconds for API propagation

            # Verify APIs are accessible
            for api in newly_enabled:
                accessible = await verify_api_accessible(project_id, api)
                if not accessible:
                    logger.warning(f"API {api} not yet accessible, waiting longer...")
                    await asyncio.sleep(30)  # Additional wait

        if failed:
            raise Exception(f"Failed to enable APIs: {', '.join(failed)}")

        return {
            "success": True,
            "enabled": enabled,
            "failed": failed
        }

    except Exception as e:
        logger.error(f"API enablement failed: {str(e)}")
        raise


async def get_enabled_apis(project_id: str) -> List[str]:
    """Get list of currently enabled APIs."""
    try:
        result = subprocess.run(
            [
                "gcloud", "services", "list",
                f"--project={project_id}",
                "--enabled",
                "--format=value(config.name)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            apis = [api.strip() for api in result.stdout.strip().split('\n') if api.strip()]
            return apis
        else:
            logger.warning(f"Could not get enabled APIs: {result.stderr}")
            return []

    except Exception as e:
        logger.error(f"Failed to get enabled APIs: {str(e)}")
        return []


async def enable_api(project_id: str, api: str) -> bool:
    """Enable a single API."""
    try:
        result = subprocess.run(
            [
                "gcloud", "services", "enable", api,
                f"--project={project_id}"
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            logger.info(f"Successfully enabled API: {api}")
            return True
        else:
            logger.error(f"Failed to enable API {api}: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error(f"API enablement timed out for: {api}")
        return False
    except Exception as e:
        logger.error(f"Failed to enable API {api}: {str(e)}")
        return False


async def verify_api_accessible(project_id: str, api: str) -> bool:
    """
    Verify that an API is accessible (propagation complete).

    This performs a simple test operation to ensure the API is ready to use.
    """
    try:
        # For each API, we can do a simple check
        if api == "bigquery.googleapis.com":
            return await verify_bigquery_api(project_id)
        elif api == "logging.googleapis.com":
            return await verify_logging_api(project_id)
        else:
            # For other APIs, just check if they're listed as enabled
            enabled_apis = await get_enabled_apis(project_id)
            return api in enabled_apis

    except Exception as e:
        logger.warning(f"API verification failed for {api}: {str(e)}")
        return False


async def verify_bigquery_api(project_id: str) -> bool:
    """Verify BigQuery API is accessible."""
    try:
        result = subprocess.run(
            [
                "gcloud", "bq", "ls",
                f"--project_id={project_id}",
                "--max_results=1"
            ],
            capture_output=True,
            text=True,
            timeout=15
        )

        # Even if there are no datasets, a successful response means API is accessible
        return result.returncode == 0 or "listed 0 datasets" in result.stdout.lower()

    except Exception as e:
        logger.warning(f"BigQuery API verification failed: {str(e)}")
        return False


async def verify_logging_api(project_id: str) -> bool:
    """Verify Cloud Logging API is accessible."""
    try:
        result = subprocess.run(
            [
                "gcloud", "logging", "logs", "list",
                f"--project={project_id}",
                "--limit=1"
            ],
            capture_output=True,
            text=True,
            timeout=15
        )

        return result.returncode == 0

    except Exception as e:
        logger.warning(f"Logging API verification failed: {str(e)}")
        return False
