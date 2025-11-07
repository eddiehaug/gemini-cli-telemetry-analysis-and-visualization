"""
Vertex AI Setup Service

Helper functions for setting up BigQuery connections to Vertex AI and creating
remote Gemini models for use in BigQuery ML.GENERATE_TEXT operations.

This module provides:
- Connection creation to Vertex AI
- IAM permission management for service accounts
- Remote model creation for Gemini models
"""

import logging
from typing import Dict, Optional
from google.cloud import bigquery
from google.cloud import bigquery_connection_v1
from google.cloud import resourcemanager_v3
from google.iam.v1 import iam_policy_pb2, policy_pb2

logger = logging.getLogger(__name__)


def create_vertex_ai_connection(
    project_id: str,
    region: str = "us-central1",
    connection_id: str = "vertex_ai_connection"
) -> Dict:
    """
    Create a BigQuery connection to Vertex AI for remote models.

    Args:
        project_id: GCP project ID
        region: Region for the connection (must match dataset region)
        connection_id: ID for the connection

    Returns:
        Dict with connection info including service account email

    Raises:
        Exception if connection creation fails
    """
    try:
        # Initialize connection client
        connection_client = bigquery_connection_v1.ConnectionServiceClient()
        parent = f"projects/{project_id}/locations/{region}"

        # Check if connection already exists
        connection_name = f"{parent}/connections/{connection_id}"
        try:
            existing_connection = connection_client.get_connection(
                request={"name": connection_name}
            )
            logger.info(f"✓ Connection already exists: {connection_id}")
            service_account = existing_connection.cloud_resource.service_account_id
            return {
                "connection_id": connection_id,
                "connection_name": connection_name,
                "service_account": service_account,
                "status": "existing",
                "region": region
            }
        except Exception:
            # Connection doesn't exist, create it
            pass

        # Create connection
        connection = bigquery_connection_v1.Connection()
        connection.cloud_resource = bigquery_connection_v1.CloudResourceProperties()

        request = bigquery_connection_v1.CreateConnectionRequest(
            parent=parent,
            connection_id=connection_id,
            connection=connection
        )

        response = connection_client.create_connection(request=request)
        service_account = response.cloud_resource.service_account_id

        logger.info(f"✓ Created BigQuery connection: {connection_id}")
        logger.info(f"  Service account: {service_account}")

        return {
            "connection_id": connection_id,
            "connection_name": connection_name,
            "service_account": service_account,
            "status": "created",
            "region": region
        }

    except Exception as e:
        logger.error(f"✗ Failed to create connection: {str(e)}")
        raise


def grant_vertex_ai_permissions(
    project_id: str,
    service_account: str
) -> Dict:
    """
    Grant Vertex AI User role to the BigQuery connection service account.

    Args:
        project_id: GCP project ID
        service_account: Service account email from the connection

    Returns:
        Dict with permission grant status

    Raises:
        Exception if permission grant fails
    """
    try:
        # Initialize IAM client
        from google.cloud import resourcemanager_v3
        projects_client = resourcemanager_v3.ProjectsClient()

        # Get current IAM policy
        resource = f"projects/{project_id}"
        policy = projects_client.get_iam_policy(request={"resource": resource})

        # Add Vertex AI User role binding
        vertex_ai_role = "roles/aiplatform.user"
        member = f"serviceAccount:{service_account}"

        # Check if binding already exists
        binding_exists = False
        for binding in policy.bindings:
            if binding.role == vertex_ai_role:
                if member in binding.members:
                    binding_exists = True
                    logger.info(f"✓ Service account already has {vertex_ai_role}")
                    break
                else:
                    binding.members.append(member)
                    binding_exists = True
                    break

        # Create new binding if it doesn't exist
        if not binding_exists:
            from google.iam.v1 import policy_pb2
            new_binding = policy_pb2.Binding(
                role=vertex_ai_role,
                members=[member]
            )
            policy.bindings.append(new_binding)

        # Set the updated policy
        if not binding_exists or member not in [b.members for b in policy.bindings if b.role == vertex_ai_role][0]:
            projects_client.set_iam_policy(
                request={
                    "resource": resource,
                    "policy": policy
                }
            )
            logger.info(f"✓ Granted {vertex_ai_role} to {service_account}")
            return {"status": "granted", "role": vertex_ai_role, "service_account": service_account}
        else:
            return {"status": "existing", "role": vertex_ai_role, "service_account": service_account}

    except Exception as e:
        logger.error(f"✗ Failed to grant permissions: {str(e)}")
        logger.warning(
            "Please grant the Vertex AI User role manually:\n"
            f"  gcloud projects add-iam-policy-binding {project_id} \\\n"
            f"    --member='serviceAccount:{service_account}' \\\n"
            f"    --role='roles/aiplatform.user'"
        )
        raise


def create_remote_gemini_model(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    connection_id: str = "vertex_ai_connection",
    region: str = "us-central1",
    model_name: str = "gemini_flash_model",
    endpoint: str = "gemini-2.5-flash"
) -> Dict:
    """
    Create a remote Gemini model in BigQuery for use with ML.GENERATE_TEXT.

    Args:
        client: BigQuery client
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        connection_id: ID of the Vertex AI connection
        region: Region of the connection
        model_name: Name for the remote model
        endpoint: Vertex AI model endpoint (e.g., gemini-2.5-flash)

    Returns:
        Dict with model creation status

    Raises:
        Exception if model creation fails
    """
    try:
        model_id = f"{project_id}.{dataset_name}.{model_name}"
        connection_path = f"{project_id}.{region}.{connection_id}"

        query = f"""
        CREATE OR REPLACE MODEL `{model_id}`
        REMOTE WITH CONNECTION `{connection_path}`
        OPTIONS (
          endpoint = '{endpoint}'
        )
        """

        logger.info(f"Creating remote Gemini model: {model_name}")
        query_job = client.query(query)
        query_job.result()

        logger.info(f"✓ Created remote Gemini model: {model_id}")
        logger.info(f"  Endpoint: {endpoint}")

        return {
            "model_id": model_id,
            "model_name": model_name,
            "endpoint": endpoint,
            "connection": connection_path,
            "status": "created"
        }

    except Exception as e:
        logger.error(f"✗ Failed to create remote model: {str(e)}")
        raise


async def setup_vertex_ai_for_bigquery(
    project_id: str,
    dataset_name: str,
    region: str = "us-central1",
    connection_id: str = "vertex_ai_connection",
    model_name: str = "gemini_flash_model",
    endpoint: str = "gemini-2.5-flash"
) -> Dict:
    """
    Complete setup of Vertex AI integration for BigQuery.

    This function:
    1. Creates BigQuery connection to Vertex AI
    2. Grants Vertex AI User permissions to the service account
    3. Creates remote Gemini model in BigQuery

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        region: Region for connection and dataset
        connection_id: ID for the Vertex AI connection
        model_name: Name for the remote Gemini model
        endpoint: Vertex AI model endpoint

    Returns:
        Dict with complete setup status

    Raises:
        Exception if any step fails
    """
    results = {
        "connection": None,
        "permissions": None,
        "model": None,
        "status": "pending"
    }

    try:
        # Step 1: Create connection
        logger.info("Step 1/3: Creating Vertex AI connection...")
        connection_result = create_vertex_ai_connection(
            project_id=project_id,
            region=region,
            connection_id=connection_id
        )
        results["connection"] = connection_result

        # Step 2: Grant permissions
        logger.info("Step 2/3: Granting Vertex AI permissions...")
        permissions_result = grant_vertex_ai_permissions(
            project_id=project_id,
            service_account=connection_result["service_account"]
        )
        results["permissions"] = permissions_result

        # Step 3: Create remote model
        logger.info("Step 3/3: Creating remote Gemini model...")
        client = bigquery.Client(project=project_id)
        model_result = create_remote_gemini_model(
            client=client,
            project_id=project_id,
            dataset_name=dataset_name,
            connection_id=connection_id,
            region=region,
            model_name=model_name,
            endpoint=endpoint
        )
        results["model"] = model_result

        results["status"] = "completed"
        logger.info("✓ Vertex AI setup completed successfully!")

        return results

    except Exception as e:
        results["status"] = "failed"
        results["error"] = str(e)
        logger.error(f"✗ Vertex AI setup failed: {str(e)}")
        raise
