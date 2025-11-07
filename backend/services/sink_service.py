"""
Log sink service.
Creates and verifies Cloud Logging sinks to Pub/Sub (ELT pattern).
"""
import subprocess
import asyncio
import logging
from typing import Dict
from google.cloud import logging as cloud_logging
from utils.validators import (
    validate_gcp_project_id,
    validate_topic_name,
    ValidationError
)

logger = logging.getLogger(__name__)


async def create_sink(
    gemini_cli_project_id: str,
    telemetry_project_id: str,
    topic_name: str = "gemini-telemetry-topic"
) -> Dict:
    """
    Create a Cloud Logging sink that exports logs to Pub/Sub topic (ELT pattern).

    This creates a sink in the TELEMETRY project that filters for gemini-cli logs
    from the GEMINI CLI project (cross-project log routing) and exports them
    to the Pub/Sub topic in the telemetry project.

    If a sink with the same name already exists, it will be deleted and recreated
    to ensure clean configuration.

    Args:
        gemini_cli_project_id: GCP project ID where Gemini CLI runs (log source)
        telemetry_project_id: GCP project ID where sink and topic exist
        topic_name: Pub/Sub topic name (default: gemini-telemetry-topic)

    Returns:
        {
            "sink_name": str,
            "destination": str,
            "service_account": str,
            "topic_name": str,
            "filter": str
        }
    """
    # Validate inputs
    try:
        gemini_cli_project_id = validate_gcp_project_id(gemini_cli_project_id)
        telemetry_project_id = validate_gcp_project_id(telemetry_project_id)
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        sink_name = "gemini-cli-to-pubsub"
        # Destination is in the telemetry project (same project as sink)
        destination = f"pubsub.googleapis.com/projects/{telemetry_project_id}/topics/{topic_name}"

        # Filter to capture gemini_cli logs from the Gemini CLI project
        # Note: logName uses underscores, not hyphens
        log_filter = f'logName="projects/{gemini_cli_project_id}/logs/gemini_cli"'

        # Exclusion filter for diagnostic logs that can break BigQuery streaming parser
        exclusion_filter = f'logName="projects/{gemini_cli_project_id}/logs/gemini_cli" AND jsonPayload."logging.googleapis.com/diagnostic" : *'

        logger.info(f"Creating log sink: {sink_name} in telemetry project")
        logger.info(f"  Telemetry Project: {telemetry_project_id}")
        logger.info(f"  Gemini CLI Project (log source): {gemini_cli_project_id}")
        logger.info(f"  Destination: {destination}")
        logger.info(f"  Filter: {log_filter}")
        logger.info(f"  Exclusion: {exclusion_filter}")

        # Step 1: Check if sink already exists in telemetry project
        existing_sinks = await list_sinks(telemetry_project_id)
        if sink_name in existing_sinks:
            logger.info(f"Sink '{sink_name}' already exists in telemetry project. Deleting it to ensure clean configuration...")
            await delete_sink(telemetry_project_id, sink_name)
            logger.info("Old sink deleted successfully")

        # Step 2: Create sink using gcloud in the TELEMETRY project
        # The sink filters logs from the Gemini CLI project (cross-project routing)
        # NOTE: Do NOT use --use-partitioned-tables (that's BigQuery-only)
        # Use --exclusion to filter out diagnostic logs
        result = subprocess.run(
            [
                "gcloud", "logging", "sinks", "create", sink_name,
                destination,
                f"--log-filter={log_filter}",
                f"--exclusion=name=exclude-diagnostic-logs,filter={exclusion_filter}",
                f"--project={telemetry_project_id}"  # Create sink in telemetry project
            ],
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode != 0:
            logger.error(f"Sink creation failed: {result.stderr}")
            raise Exception(f"Failed to create sink: {result.stderr}")

        logger.info("Sink created successfully in telemetry project")

        # Extract service account from output
        # The sink creation returns the service account immediately
        service_account = await get_sink_service_account(telemetry_project_id, sink_name)

        if not service_account:
            logger.error("Failed to retrieve sink service account - sink may not work!")
            raise Exception("Could not retrieve sink service account (writerIdentity)")

        logger.info(f"Sink created with service account: {service_account}")

        # Grant Pub/Sub Publisher permissions to the sink's service account
        await grant_pubsub_publisher(telemetry_project_id, topic_name, service_account)

        return {
            "sink_name": sink_name,
            "destination": destination,
            "service_account": service_account,
            "topic_name": topic_name,
            "filter": log_filter
        }

    except subprocess.TimeoutExpired:
        logger.error("Sink creation timed out")
        raise Exception("Sink creation timed out")
    except Exception as e:
        logger.error(f"Sink creation failed: {str(e)}")
        raise


async def get_sink_service_account(project_id: str, sink_name: str) -> str:
    """Get the service account associated with a sink."""
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        result = subprocess.run(
            [
                "gcloud", "logging", "sinks", "describe", sink_name,
                f"--project={project_id}",
                "--format=value(writerIdentity)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            service_account = result.stdout.strip()
            logger.info(f"Sink service account: {service_account}")
            return service_account
        else:
            logger.warning("Could not get sink service account")
            return ""

    except Exception as e:
        logger.warning(f"Failed to get sink service account: {str(e)}")
        return ""


async def grant_pubsub_publisher(project_id: str, topic_name: str, service_account: str) -> None:
    """
    Grant Pub/Sub Publisher permissions to the sink's service account.

    This allows the sink to publish messages to the Pub/Sub topic for ELT processing.
    The sink creates its own service account (writerIdentity) which needs
    explicit permission to publish to Pub/Sub.

    Retries up to 3 times if the service account doesn't exist yet, as Google-managed
    service accounts may take time to be provisioned.

    Args:
        project_id: GCP project ID
        topic_name: Pub/Sub topic name
        service_account: Sink service account (writerIdentity)
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        topic_name = validate_topic_name(topic_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Extract just the service account email (remove "serviceAccount:" prefix if present)
        if service_account.startswith("serviceAccount:"):
            service_account = service_account.replace("serviceAccount:", "")

        logger.info(f"Granting Pub/Sub Publisher role to sink service account: {service_account}")

        # Retry logic for service account provisioning
        max_retries = 3
        retry_delay = 20  # seconds
        last_error = ""

        for attempt in range(max_retries):
            if attempt > 0:
                logger.info(f"Retry attempt {attempt + 1}/{max_retries} after {retry_delay}s wait...")
                await asyncio.sleep(retry_delay)

            # Use gcloud to add IAM binding to Pub/Sub topic
            result = subprocess.run(
                [
                    "gcloud", "pubsub", "topics", "add-iam-policy-binding", topic_name,
                    f"--member=serviceAccount:{service_account}",
                    "--role=roles/pubsub.publisher",
                    f"--project={project_id}"
                ],
                capture_output=True,
                text=True,
                timeout=30
            )

            # Check if successful
            if result.returncode == 0:
                logger.info("✓ Pub/Sub Publisher role granted successfully")
                logger.info(f"  Service Account: {service_account}")
                logger.info(f"  Role: roles/pubsub.publisher")
                logger.info(f"  Topic: {topic_name}")

                # Wait for IAM propagation (critical for sink to work)
                logger.info("Waiting for IAM propagation (90 seconds)...")
                await asyncio.sleep(90)
                logger.info("IAM propagation wait complete")
                return  # Success - exit function
            else:
                error_msg = result.stderr.strip()
                last_error = error_msg

                # Check if permission already exists
                if "already has" in error_msg.lower() or "already exists" in error_msg.lower():
                    logger.info("Permission already exists - skipping")
                    # Still wait for propagation to be safe
                    logger.info("Waiting for IAM propagation (90 seconds)...")
                    await asyncio.sleep(90)
                    logger.info("IAM propagation wait complete")
                    return  # Success - exit function

                # Check if service account doesn't exist yet
                if "does not exist" in error_msg.lower() and attempt < max_retries - 1:
                    logger.warning(f"Service account not ready yet (attempt {attempt + 1}/{max_retries})")
                    continue  # Retry
                elif "does not exist" in error_msg.lower():
                    logger.error(f"Service account still doesn't exist after {max_retries} attempts")
                    raise Exception(f"Failed to grant Pub/Sub permissions: Service account not provisioned after {max_retries * retry_delay}s")
                else:
                    # Other error - don't retry
                    logger.error(f"Failed to grant permissions: {error_msg}")
                    raise Exception(f"Failed to grant Pub/Sub permissions to sink service account: {error_msg}")

    except subprocess.TimeoutExpired:
        logger.error("Permission granting timed out")
        raise Exception("Failed to grant permissions: command timed out")
    except Exception as e:
        logger.error(f"Failed to grant sink permissions: {str(e)}")
        raise


async def verify_sink(project_id: str, sink_name: str = "gemini-cli-to-pubsub") -> Dict:
    """
    Verify that the log sink is properly configured for Pub/Sub (ELT pattern).

    Checks:
    1. Sink exists
    2. Sink has correct Pub/Sub destination
    3. Sink has service account (writerIdentity)
    4. Service account has Pub/Sub Publisher permissions

    Args:
        project_id: GCP project ID
        sink_name: Sink name (default: gemini-cli-to-pubsub)

    Returns:
        {
            "verified": bool,
            "destination": str,
            "destination_type": str,
            "writer_identity": str,
            "filter": str,
            "has_permissions": bool
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        logger.info(f"Verifying sink: {sink_name}")

        # Get sink details
        result = subprocess.run(
            [
                "gcloud", "logging", "sinks", "describe", sink_name,
                f"--project={project_id}",
                "--format=json"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.error(f"Sink not found: {result.stderr}")
            raise Exception(f"Sink {sink_name} not found")

        import json
        sink_info = json.loads(result.stdout)

        # Verify sink properties
        destination = sink_info.get("destination", "")
        writer_identity = sink_info.get("writerIdentity", "")
        filter_str = sink_info.get("filter", "")

        # Verify destination is Pub/Sub
        destination_type = "unknown"
        if "pubsub.googleapis.com" in destination:
            destination_type = "pubsub"
        elif "bigquery.googleapis.com" in destination:
            destination_type = "bigquery"

        if destination_type != "pubsub":
            logger.error(f"Sink destination is not Pub/Sub: {destination}")
            raise Exception(f"Sink destination should be Pub/Sub, but is: {destination_type}")

        if not writer_identity:
            logger.error("Sink has no writerIdentity (service account)")
            raise Exception("Sink is missing writerIdentity - cannot export data")

        # Verify the service account has Pub/Sub Publisher permissions
        has_permissions = await verify_service_account_permissions(project_id, writer_identity, destination)

        verified = bool(destination and writer_identity and filter_str and has_permissions and destination_type == "pubsub")

        logger.info(f"✓ Sink verification complete")
        logger.info(f"  Destination Type: {destination_type}")
        logger.info(f"  Destination: {destination}")
        logger.info(f"  Service Account: {writer_identity}")
        logger.info(f"  Has Permissions: {has_permissions}")
        logger.info(f"  Filter: {filter_str}")

        return {
            "verified": verified,
            "destination": destination,
            "destination_type": destination_type,
            "writer_identity": writer_identity,
            "filter": filter_str,
            "has_permissions": has_permissions
        }

    except Exception as e:
        logger.error(f"Sink verification failed: {str(e)}")
        raise


async def verify_service_account_permissions(project_id: str, service_account: str, destination: str) -> bool:
    """
    Verify that the service account has Pub/Sub Publisher permissions.

    Checks the IAM policy to ensure the sink's service account can publish to Pub/Sub topic.

    Args:
        project_id: GCP project ID
        service_account: Sink service account
        destination: Sink destination (pubsub.googleapis.com/projects/.../topics/...)

    Returns:
        True if service account has publisher role, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Clean up service account format
        if service_account.startswith("serviceAccount:"):
            sa_email = service_account.replace("serviceAccount:", "")
        else:
            sa_email = service_account

        # Extract topic name from destination
        # Format: pubsub.googleapis.com/projects/{project}/topics/{topic}
        if "pubsub.googleapis.com" in destination:
            topic_name = destination.split("/topics/")[-1]
        else:
            logger.warning("Not a Pub/Sub destination, skipping permission check")
            return False

        logger.info(f"Checking IAM permissions for {sa_email} on topic {topic_name}...")

        # Get IAM policy for the Pub/Sub topic
        result = subprocess.run(
            [
                "gcloud", "pubsub", "topics", "get-iam-policy", topic_name,
                f"--project={project_id}",
                "--flatten=bindings[].members",
                f"--filter=bindings.members:serviceAccount:{sa_email}",
                "--format=value(bindings.role)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            roles = result.stdout.strip().split('\n')
            roles = [r for r in roles if r]  # Remove empty strings

            # Check if Pub/Sub Publisher role is present
            has_publisher_role = "roles/pubsub.publisher" in roles

            if has_publisher_role:
                logger.info(f"✓ Service account has Pub/Sub Publisher role")
                return True
            else:
                logger.warning(f"✗ Service account missing Pub/Sub Publisher role")
                logger.warning(f"  Current roles: {roles}")
                return False
        else:
            logger.warning(f"Could not verify permissions: {result.stderr}")
            return False

    except Exception as e:
        logger.warning(f"Permission verification failed: {str(e)}")
        return False


async def list_sinks(project_id: str) -> list:
    """List all sinks in the project."""
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        result = subprocess.run(
            [
                "gcloud", "logging", "sinks", "list",
                f"--project={project_id}",
                "--format=value(name)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            sinks = [s.strip() for s in result.stdout.strip().split('\n') if s.strip()]
            return sinks
        else:
            return []

    except Exception as e:
        logger.warning(f"Failed to list sinks: {str(e)}")
        return []


async def delete_sink(project_id: str, sink_name: str) -> None:
    """Delete a log sink."""
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        logger.info(f"Deleting sink: {sink_name}")

        result = subprocess.run(
            [
                "gcloud", "logging", "sinks", "delete", sink_name,
                f"--project={project_id}",
                "--quiet"  # Skip confirmation prompt
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            # If sink doesn't exist, that's fine
            if "not found" in result.stderr.lower():
                logger.info(f"Sink {sink_name} does not exist (already deleted)")
            else:
                logger.error(f"Failed to delete sink: {result.stderr}")
                raise Exception(f"Failed to delete sink: {result.stderr}")
        else:
            logger.info(f"Sink {sink_name} deleted successfully")

    except Exception as e:
        logger.error(f"Failed to delete sink: {str(e)}")
        raise
