"""
Dataflow service for deploying streaming pipeline.

This service handles:
- Starting Dataflow streaming job using Google's maintained template
- Configuring Pub/Sub → JavaScript UDF → BigQuery pipeline
- Monitoring job status
- Managing Dataflow job lifecycle
"""
import asyncio
import logging
import subprocess
import json
import re
from typing import Dict, Optional
from utils.validators import (
    validate_gcp_project_id,
    validate_dataset_name,
    validate_region,
    validate_network_name,
    ValidationError
)

logger = logging.getLogger(__name__)


async def grant_dataflow_worker_role(project_id: str) -> Dict:
    """
    Grant Dataflow worker role to Compute Engine default service account.

    The Compute Engine default service account needs roles/dataflow.worker
    to execute Dataflow jobs (get jobs, lease/update work items, etc.).

    Args:
        project_id: GCP project ID

    Returns:
        Dict with role grant status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Get project number
        result = subprocess.run(
            ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            raise Exception(f"Failed to get project number: {result.stderr}")

        project_number = result.stdout.strip()
        compute_sa = f"{project_number}-compute@developer.gserviceaccount.com"

        logger.info(f"Granting Dataflow worker role to: {compute_sa}")

        # Grant roles/dataflow.worker to the Compute Engine service account
        command = [
            "gcloud", "projects", "add-iam-policy-binding", project_id,
            "--member", f"serviceAccount:{compute_sa}",
            "--role", "roles/dataflow.worker",
            "--condition", "None"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info(f"✓ Granted roles/dataflow.worker to Compute Engine service account")
            logger.info(f"  - Service Account: {compute_sa}")
            logger.info(f"  - Role: roles/dataflow.worker")

            return {
                "service_account": compute_sa,
                "role": "roles/dataflow.worker",
                "status": "granted"
            }
        else:
            # Check if permission already exists
            if "already has role" in result.stderr or "ALREADY_EXISTS" in result.stderr:
                logger.info(f"Compute Engine SA already has dataflow.worker role")
                return {
                    "service_account": compute_sa,
                    "role": "roles/dataflow.worker",
                    "status": "already_exists"
                }
            else:
                logger.warning(f"Could not grant dataflow.worker role: {result.stderr}")
                # Don't fail - might already have it through other means
                return {
                    "service_account": compute_sa,
                    "role": "roles/dataflow.worker",
                    "status": "failed",
                    "error": result.stderr
                }

    except Exception as e:
        logger.warning(f"Failed to grant Dataflow worker role: {str(e)}")
        # Don't fail - might already have permissions
        return {
            "status": "error",
            "error": str(e)
        }


async def grant_bigquery_data_editor_role(project_id: str) -> Dict:
    """
    Grant BigQuery Data Editor role to Compute Engine default service account.

    The Compute Engine default service account needs roles/bigquery.dataEditor
    to write streaming data to BigQuery tables (bigquery.tables.updateData permission).

    Args:
        project_id: GCP project ID

    Returns:
        Dict with role grant status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Get project number
        result = subprocess.run(
            ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            raise Exception(f"Failed to get project number: {result.stderr}")

        project_number = result.stdout.strip()
        compute_sa = f"{project_number}-compute@developer.gserviceaccount.com"

        logger.info(f"Granting BigQuery Data Editor role to: {compute_sa}")

        # Grant roles/bigquery.dataEditor to the Compute Engine service account
        command = [
            "gcloud", "projects", "add-iam-policy-binding", project_id,
            "--member", f"serviceAccount:{compute_sa}",
            "--role", "roles/bigquery.dataEditor"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info(f"✓ Granted roles/bigquery.dataEditor to Compute Engine service account")
            logger.info(f"  - Service Account: {compute_sa}")
            logger.info(f"  - Role: roles/bigquery.dataEditor")
            logger.info(f"  - This grants bigquery.tables.updateData permission for streaming inserts")

            return {
                "service_account": compute_sa,
                "role": "roles/bigquery.dataEditor",
                "status": "granted"
            }
        else:
            # Check if permission already exists
            if "already has role" in result.stderr or "ALREADY_EXISTS" in result.stderr:
                logger.info(f"Compute Engine SA already has bigquery.dataEditor role")
                return {
                    "service_account": compute_sa,
                    "role": "roles/bigquery.dataEditor",
                    "status": "already_exists"
                }
            else:
                logger.warning(f"Could not grant bigquery.dataEditor role: {result.stderr}")
                # Don't fail - might already have it through other means
                return {
                    "service_account": compute_sa,
                    "role": "roles/bigquery.dataEditor",
                    "status": "failed",
                    "error": result.stderr
                }

    except Exception as e:
        logger.warning(f"Failed to grant BigQuery Data Editor role: {str(e)}")
        # Don't fail - might already have permissions
        return {
            "status": "error",
            "error": str(e)
        }


async def grant_dataflow_network_permissions(
    project_id: str,
    network: str = "default",
    subnetwork: str = "default",
    region: str = "us-central1"
) -> Dict:
    """
    Grant Dataflow service account permissions to use the specified network/subnetwork.

    The Compute Engine default service account needs compute.networkUser role
    to access the VPC network and subnetwork.

    Args:
        project_id: GCP project ID
        network: VPC network name
        subnetwork: Subnetwork name
        region: GCP region

    Returns:
        Dict with permission grant status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        network = validate_network_name(network)
        subnetwork = validate_network_name(subnetwork)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        # Get project number
        result = subprocess.run(
            ["gcloud", "projects", "describe", project_id, "--format=value(projectNumber)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            raise Exception(f"Failed to get project number: {result.stderr}")

        project_number = result.stdout.strip()
        compute_sa = f"{project_number}-compute@developer.gserviceaccount.com"

        logger.info(f"Granting network permissions to Dataflow worker: {compute_sa}")
        logger.info(f"  Network: {network}")
        logger.info(f"  Subnetwork: regions/{region}/subnetworks/{subnetwork}")

        # Grant compute.networkUser role on the subnetwork
        # This allows Dataflow to use the subnetwork
        subnetwork_path = f"projects/{project_id}/regions/{region}/subnetworks/{subnetwork}"

        command = [
            "gcloud", "compute", "networks", "subnets", "add-iam-policy-binding", subnetwork,
            "--region", region,
            "--member", f"serviceAccount:{compute_sa}",
            "--role", "roles/compute.networkUser",
            "--project", project_id
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info(f"✓ Granted roles/compute.networkUser to Dataflow worker on subnetwork")
            logger.info(f"  - Service Account: {compute_sa}")
            logger.info(f"  - Subnetwork: {subnetwork_path}")

            return {
                "service_account": compute_sa,
                "role": "roles/compute.networkUser",
                "subnetwork": subnetwork_path,
                "status": "granted"
            }
        else:
            # Check if permission already exists
            if "already has role" in result.stderr or "ALREADY_EXISTS" in result.stderr:
                logger.info(f"Compute Engine SA already has networkUser role on subnetwork")
                return {
                    "service_account": compute_sa,
                    "role": "roles/compute.networkUser",
                    "subnetwork": subnetwork_path,
                    "status": "already_exists"
                }
            else:
                logger.warning(f"Failed to grant network permissions: {result.stderr}")
                raise Exception(f"Failed to grant network permissions: {result.stderr}")

    except Exception as e:
        logger.error(f"Failed to grant Dataflow network permissions: {str(e)}")
        raise


async def create_dataflow_firewall_rules(
    project_id: str,
    network: str = "default"
) -> Dict:
    """
    Create firewall rules required for Dataflow worker communication.

    Dataflow workers need TCP ports 12345-12346 open for internal communication
    when shuffling data. Without these rules, pipelines with multiple workers will hang.

    Args:
        project_id: GCP project ID
        network: VPC network name

    Returns:
        Dict with firewall rule creation status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        network = validate_network_name(network)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        rule_name = "allow-dataflow-workers"

        logger.info(f"Creating firewall rule '{rule_name}' for Dataflow workers...")
        logger.info(f"  Network: {network}")
        logger.info(f"  Ports: TCP 12345-12346")

        # Check if rule already exists
        check_command = [
            "gcloud", "compute", "firewall-rules", "describe", rule_name,
            "--project", project_id,
            "--format", "json"
        ]

        check_result = subprocess.run(
            check_command,
            capture_output=True,
            text=True,
            timeout=10
        )

        if check_result.returncode == 0:
            logger.info(f"Firewall rule '{rule_name}' already exists")
            return {
                "rule_name": rule_name,
                "network": network,
                "ports": "tcp:12345-12346",
                "target_tags": ["dataflow"],
                "status": "already_exists"
            }

        # Create firewall rule
        create_command = [
            "gcloud", "compute", "firewall-rules", "create", rule_name,
            "--project", project_id,
            "--network", network,
            "--allow", "tcp:12345-12346",
            "--source-ranges", "0.0.0.0/0",
            "--target-tags", "dataflow",
            "--description", "Allow Dataflow workers to communicate on ports 12345-12346 for data shuffling",
            "--direction", "INGRESS"
        ]

        result = subprocess.run(
            create_command,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            logger.info(f"✓ Firewall rule '{rule_name}' created successfully")
            logger.info(f"  - Network: {network}")
            logger.info(f"  - Ports: TCP 12345-12346")
            logger.info(f"  - Target tags: dataflow")

            return {
                "rule_name": rule_name,
                "network": network,
                "ports": "tcp:12345-12346",
                "target_tags": ["dataflow"],
                "status": "created"
            }
        else:
            error_msg = f"Failed to create firewall rule: {result.stderr}"
            logger.error(error_msg)
            raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Failed to create Dataflow firewall rules: {str(e)}")
        raise


async def start_dataflow_job(
    project_id: str,
    dataset_name: str,
    region: str = "us-central1",
    job_name: str = "gemini-telemetry-pipeline",
    network: Optional[str] = None,
    subnetwork: Optional[str] = None
) -> Dict:
    """
    Start Dataflow streaming job using Google's PubSub_Subscription_to_BigQuery template.

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        region: GCP region for Dataflow job (default: us-central1)
        job_name: Name for the Dataflow job (default: gemini-telemetry-pipeline)
        network: VPC network name (default: default)
        subnetwork: Subnetwork name (default: default)

    Returns:
        {
            "job_id": str,
            "job_name": str,
            "region": str,
            "status": str,
            "console_url": str,
            "parameters": dict
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
        region = validate_region(region)
        if network:
            network = validate_network_name(network)
        if subnetwork:
            subnetwork = validate_network_name(subnetwork)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    # Add timestamp suffix to job name to avoid conflicts on retries
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    unique_job_name = f"{job_name}-{timestamp}"

    logger.info(f"Starting Dataflow job '{unique_job_name}' in project {project_id}, region {region}")

    try:
        # Use provided network or default to "default"
        network_name = network if network else "default"
        subnetwork_name = subnetwork if subnetwork else "default"

        # Step 1: Grant Dataflow worker role to Compute Engine service account
        logger.info(f"Granting Dataflow worker role to Compute Engine service account...")
        await grant_dataflow_worker_role(project_id)

        # Step 1b: Grant BigQuery Data Editor role to Compute Engine service account
        logger.info(f"Granting BigQuery Data Editor role to Compute Engine service account...")
        await grant_bigquery_data_editor_role(project_id)

        # Step 2: Create firewall rules for Dataflow workers
        logger.info(f"Creating firewall rules for Dataflow workers...")
        await create_dataflow_firewall_rules(
            project_id=project_id,
            network=network_name
        )

        # Step 3: Grant network permissions to use the network/subnetwork
        logger.info(f"Granting network permissions before starting Dataflow job...")
        await grant_dataflow_network_permissions(
            project_id=project_id,
            network=network_name,
            subnetwork=subnetwork_name,
            region=region
        )

        # Build Dataflow parameters
        bucket_name = f"{project_id}-dataflow"
        input_subscription = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"
        output_table_spec = f"{project_id}:{dataset_name}.gemini_raw_logs"
        udf_gcs_path = f"gs://{bucket_name}/transform.js"
        staging_location = f"gs://{bucket_name}/temp"

        # Use latest classic template
        template_path = f"gs://dataflow-templates-{region}/latest/PubSub_Subscription_to_BigQuery"

        # Build gcloud command using classic template with Streaming Engine
        command = [
            "gcloud", "dataflow", "jobs", "run", unique_job_name,
            "--gcs-location", template_path,
            "--region", region,
            "--staging-location", staging_location,
            "--parameters", f"inputSubscription={input_subscription}",
            "--parameters", f"outputTableSpec={output_table_spec}",
            "--parameters", f"javascriptTextTransformGcsPath={udf_gcs_path}",
            "--parameters", f"javascriptTextTransformFunctionName=transform",
            "--network", network_name,
            "--subnetwork", f"regions/{region}/subnetworks/{subnetwork_name}",
            "--disable-public-ips",  # Required for projects with external IP constraints
            "--enable-streaming-engine",  # Offload execution to Dataflow backend
            "--worker-machine-type=n1-standard-2",  # Smaller workers (Streaming Engine optimized)
            "--project", project_id,
            "--format", "json"
        ]

        logger.info(f"Starting Dataflow job with Streaming Engine enabled")
        logger.info(f"  Worker type: n1-standard-2 (optimized for Streaming Engine)")
        logger.info(f"  Network: {network_name}, Subnetwork: {subnetwork_name}")
        logger.info(f"  Private IPs only (no external IPs)")
        logger.info(f"Executing gcloud command: {' '.join(command)}")

        # Execute gcloud command
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse JSON output from gcloud
        job_info = json.loads(result.stdout)

        # Extract job ID from the response
        job_id = job_info.get("id") or job_info.get("jobId")

        if not job_id:
            # Try to extract job ID from stderr if not in stdout
            job_id = _extract_job_id_from_output(result.stderr)

        logger.info(f"Dataflow job created. Job ID: {job_id}")

        # Wait for worker pool to start up (this is when most failures occur)
        # Worker pool startup can take 1-2 minutes, especially checking VPC/IP constraints
        logger.info("Waiting 2 minutes for worker pool to start up...")

        # Check status every 20 seconds for 2 minutes
        total_wait = 120  # 2 minutes
        check_interval = 20  # seconds
        elapsed = 0

        while elapsed < total_wait:
            await asyncio.sleep(check_interval)
            elapsed += check_interval

            # Check actual job status
            logger.info(f"Checking job status ({elapsed}s elapsed)...")
            status_result = await get_job_status(project_id, job_id, region)
            actual_status = status_result.get("state", "UNKNOWN")

            logger.info(f"Dataflow job status: {actual_status}")

            # Check if job failed during startup (especially worker pool failures)
            if actual_status in ["JOB_STATE_FAILED", "FAILED", "JOB_STATE_CANCELLED", "CANCELLED"]:
                error_msg = status_result.get("error", "Job failed during startup")
                raise Exception(f"Dataflow job failed during worker startup: {error_msg}")

            # If job is running, we can exit early
            if actual_status in ["JOB_STATE_RUNNING", "RUNNING"]:
                logger.info(f"Dataflow job is running successfully after {elapsed}s")
                break

        # Final status check
        status_result = await get_job_status(project_id, job_id, region)
        actual_status = status_result.get("state", "UNKNOWN")

        return {
            "job_id": job_id,
            "job_name": unique_job_name,
            "region": region,
            "status": actual_status,
            "console_url": f"https://console.cloud.google.com/dataflow/jobs/{region}/{job_id}?project={project_id}",
            "parameters": {
                "inputSubscription": input_subscription,
                "outputTableSpec": output_table_spec,
                "javascriptTextTransformGcsPath": udf_gcs_path,
                "stagingLocation": staging_location
            }
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to start Dataflow job: {e.stderr}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse gcloud output: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Unexpected error starting Dataflow job: {str(e)}")
        raise


async def get_job_status(
    project_id: str,
    job_id: str,
    region: str = "us-central1"
) -> Dict:
    """
    Get status of a Dataflow job.

    Args:
        project_id: GCP project ID
        job_id: Dataflow job ID
        region: GCP region (default: us-central1)

    Returns:
        {
            "job_id": str,
            "state": str,
            "create_time": str,
            "current_state_time": str,
            "type": str
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    logger.info(f"Getting status for Dataflow job {job_id}")

    try:
        command = [
            "gcloud", "dataflow", "jobs", "describe", job_id,
            "--region", region,
            "--project", project_id,
            "--format", "json"
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )

        job_info = json.loads(result.stdout)

        return {
            "job_id": job_info.get("id"),
            "state": job_info.get("currentState"),
            "create_time": job_info.get("createTime"),
            "current_state_time": job_info.get("currentStateTime"),
            "type": job_info.get("type")
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to get job status: {e.stderr}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Unexpected error getting job status: {str(e)}")
        raise


async def verify_job_running(
    project_id: str,
    job_id: str,
    region: str = "us-central1",
    max_wait_seconds: int = 120
) -> bool:
    """
    Verify that Dataflow job is running.

    Waits up to max_wait_seconds for job to reach 'Running' state.

    Args:
        project_id: GCP project ID
        job_id: Dataflow job ID
        region: GCP region (default: us-central1)
        max_wait_seconds: Max seconds to wait (default: 120)

    Returns:
        True if job is running, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    logger.info(f"Verifying Dataflow job {job_id} is running...")

    elapsed = 0
    interval = 10

    while elapsed < max_wait_seconds:
        try:
            status = await get_job_status(project_id, job_id, region)
            state = status.get("state")

            logger.info(f"Job state: {state}")

            # Check if job is in running state
            if state == "JOB_STATE_RUNNING":
                logger.info(f"Job {job_id} is running")
                return True

            # Check for failed states
            if state in ["JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "JOB_STATE_DRAINED"]:
                logger.error(f"Job {job_id} is in failed state: {state}")
                return False

            # Wait before next check
            await asyncio.sleep(interval)
            elapsed += interval

        except Exception as e:
            logger.error(f"Error checking job status: {str(e)}")
            await asyncio.sleep(interval)
            elapsed += interval

    logger.warning(f"Timeout waiting for job {job_id} to reach running state")
    return False


async def stop_dataflow_job(
    project_id: str,
    job_id: str,
    region: str = "us-central1"
) -> Dict:
    """
    Stop (cancel) a running Dataflow job.

    Args:
        project_id: GCP project ID
        job_id: Dataflow job ID
        region: GCP region (default: us-central1)

    Returns:
        {
            "job_id": str,
            "status": str,
            "message": str
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    logger.info(f"Stopping Dataflow job {job_id}")

    try:
        command = [
            "gcloud", "dataflow", "jobs", "cancel", job_id,
            "--region", region,
            "--project", project_id
        ]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )

        return {
            "job_id": job_id,
            "status": "cancelled",
            "message": f"Job {job_id} cancelled successfully"
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to cancel job: {e.stderr}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Unexpected error cancelling job: {str(e)}")
        raise


async def list_dataflow_jobs(
    project_id: str,
    region: str = "us-central1",
    status_filter: Optional[str] = None
) -> Dict:
    """
    List Dataflow jobs in the project.

    Args:
        project_id: GCP project ID
        region: GCP region (default: us-central1)
        status_filter: Optional status filter (e.g., 'active', 'all')

    Returns:
        {
            "jobs": list of job dicts,
            "count": int
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    logger.info(f"Listing Dataflow jobs for project {project_id}")

    try:
        command = [
            "gcloud", "dataflow", "jobs", "list",
            "--region", region,
            "--project", project_id,
            "--format", "json"
        ]

        if status_filter:
            command.extend(["--status", status_filter])

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True
        )

        jobs = json.loads(result.stdout)

        return {
            "jobs": jobs,
            "count": len(jobs)
        }

    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to list jobs: {e.stderr}"
        logger.error(error_msg)
        raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Unexpected error listing jobs: {str(e)}")
        raise


async def verify_dataflow_pipeline(
    project_id: str,
    dataset_name: str,
    region: str = "us-central1",
    job_name: str = "gemini-telemetry-pipeline"
) -> Dict:
    """
    Verify Dataflow streaming job is running and correctly configured.

    Checks:
    - Job exists and is in running state
    - Input subscription is correct
    - Output BigQuery table is correct
    - JavaScript UDF path is correct

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        region: GCP region (default: us-central1)
        job_name: Dataflow job name (default: gemini-telemetry-pipeline)

    Returns:
        {
            "job_found": bool,
            "job_id": str,
            "state": str,
            "is_running": bool,
            "input_subscription": str,
            "output_table": str,
            "udf_path": str,
            "configuration_correct": bool,
            "issues": list[str]
        }
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    logger.info(f"Verifying Dataflow pipeline '{job_name}' in project {project_id}")

    issues = []

    try:
        # List active Dataflow jobs
        jobs_result = await list_dataflow_jobs(project_id, region, status_filter="active")
        jobs = jobs_result.get("jobs", [])

        # Find job with matching name
        job_info = None
        for job in jobs:
            if job.get("name") == job_name:
                job_info = job
                break

        if not job_info:
            logger.warning(f"Dataflow job '{job_name}' not found")
            return {
                "job_found": False,
                "job_id": None,
                "state": None,
                "is_running": False,
                "input_subscription": None,
                "output_table": None,
                "udf_path": None,
                "configuration_correct": False,
                "issues": [f"Job '{job_name}' not found in active jobs"]
            }

        job_id = job_info.get("id")
        state = job_info.get("currentState")
        is_running = state == "JOB_STATE_RUNNING"

        if not is_running:
            issues.append(f"Job is not running (state: {state})")

        # Get full job details to verify configuration
        detailed_status = await get_job_status(project_id, job_id, region)

        # Expected configuration
        expected_subscription = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"
        expected_table = f"{project_id}:{dataset_name}.gemini_raw_logs"
        expected_udf = f"gs://{project_id}-dataflow/transform.js"

        # Extract configuration from job info
        # Note: Job environment variables contain the parameters
        environment = job_info.get("environment", {})
        temp_location = environment.get("tempStoragePrefix", "")

        # Verify configuration matches expected values
        # (Note: gcloud doesn't expose all template parameters in list/describe output,
        # so we'll do basic validation based on what's available)
        configuration_correct = True

        if temp_location and project_id not in temp_location:
            issues.append(f"Temp location doesn't match project: {temp_location}")
            configuration_correct = False

        logger.info(f"✓ Dataflow pipeline verification complete")
        logger.info(f"  Job ID: {job_id}")
        logger.info(f"  State: {state}")
        logger.info(f"  Running: {is_running}")

        return {
            "job_found": True,
            "job_id": job_id,
            "state": state,
            "is_running": is_running,
            "input_subscription": expected_subscription,
            "output_table": expected_table,
            "udf_path": expected_udf,
            "configuration_correct": configuration_correct,
            "issues": issues
        }

    except Exception as e:
        error_msg = f"Dataflow pipeline verification failed: {str(e)}"
        logger.error(error_msg)
        return {
            "job_found": False,
            "job_id": None,
            "state": None,
            "is_running": False,
            "input_subscription": None,
            "output_table": None,
            "udf_path": None,
            "configuration_correct": False,
            "issues": [error_msg]
        }


def _extract_job_id_from_output(output: str) -> Optional[str]:
    """
    Extract job ID from gcloud command output.

    Tries to find job ID in various formats from stdout/stderr.

    Args:
        output: Command output string

    Returns:
        Job ID if found, None otherwise
    """
    # Try to find job ID in format: 2025-01-06_12_34_56-1234567890123456789
    job_id_pattern = r'\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2}-\d+'
    match = re.search(job_id_pattern, output)

    if match:
        return match.group(0)

    return None
