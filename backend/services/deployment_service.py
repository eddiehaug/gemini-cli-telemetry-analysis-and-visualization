"""
Deployment tracking service.
Manages deployment state and status.
"""
import logging
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# In-memory storage for deployment states
# In production, this would be a database
_deployments: Dict[str, Dict] = {}


async def get_status(deployment_id: str) -> Optional[Dict]:
    """
    Get deployment status by ID.

    Args:
        deployment_id: Unique deployment identifier

    Returns:
        Deployment state dict or None if not found
    """
    deployment = _deployments.get(deployment_id)

    if not deployment:
        logger.warning(f"Deployment not found: {deployment_id}")
        raise Exception(f"Deployment {deployment_id} not found")

    logger.info(f"Retrieved deployment status: {deployment_id}")
    return deployment


async def create_deployment(config: Dict) -> str:
    """
    Create a new deployment record.

    Args:
        config: Deployment configuration

    Returns:
        deployment_id
    """
    import uuid

    deployment_id = str(uuid.uuid4())

    deployment = {
        "deploymentId": deployment_id,
        "status": "idle",
        "currentStep": 0,
        "steps": _get_initial_steps(),
        "config": config,
        "createdResources": {},
        "createdAt": datetime.utcnow().isoformat(),
        "updatedAt": datetime.utcnow().isoformat()
    }

    _deployments[deployment_id] = deployment
    logger.info(f"Created deployment: {deployment_id}")

    return deployment_id


async def update_deployment_status(
    deployment_id: str,
    status: str,
    current_step: Optional[int] = None
) -> None:
    """
    Update deployment status.

    Args:
        deployment_id: Deployment ID
        status: New status ('idle', 'deploying', 'completed', 'failed')
        current_step: Current step number (optional)
    """
    if deployment_id not in _deployments:
        raise Exception(f"Deployment {deployment_id} not found")

    deployment = _deployments[deployment_id]
    deployment["status"] = status
    deployment["updatedAt"] = datetime.utcnow().isoformat()

    if current_step is not None:
        deployment["currentStep"] = current_step

    logger.info(f"Updated deployment {deployment_id}: status={status}, step={current_step}")


async def update_step_status(
    deployment_id: str,
    step_index: int,
    status: str,
    details: Optional[str] = None,
    error: Optional[str] = None
) -> None:
    """
    Update status of a specific deployment step.

    Args:
        deployment_id: Deployment ID
        step_index: Step index (0-based)
        status: Step status ('pending', 'in_progress', 'completed', 'failed')
        details: Optional details message
        error: Optional error message
    """
    if deployment_id not in _deployments:
        raise Exception(f"Deployment {deployment_id} not found")

    deployment = _deployments[deployment_id]
    steps = deployment["steps"]

    if step_index < 0 or step_index >= len(steps):
        raise Exception(f"Invalid step index: {step_index}")

    step = steps[step_index]
    step["status"] = status
    deployment["updatedAt"] = datetime.utcnow().isoformat()

    if details:
        step["details"] = details

    if error:
        step["error"] = error

    logger.info(f"Updated step {step_index} for deployment {deployment_id}: {status}")


async def add_created_resource(
    deployment_id: str,
    resource_type: str,
    resource_name: str
) -> None:
    """
    Add a created resource to the deployment record.

    Args:
        deployment_id: Deployment ID
        resource_type: Type of resource ('dataset', 'table', 'sink')
        resource_name: Name/ID of the resource
    """
    if deployment_id not in _deployments:
        raise Exception(f"Deployment {deployment_id} not found")

    deployment = _deployments[deployment_id]
    deployment["createdResources"][resource_type] = resource_name
    deployment["updatedAt"] = datetime.utcnow().isoformat()

    logger.info(f"Added resource to deployment {deployment_id}: {resource_type}={resource_name}")


def _get_initial_steps() -> list:
    """Get initial deployment steps."""
    return [
        {
            "id": "1",
            "name": "Verify Dependencies",
            "description": "Check required tools",
            "status": "pending"
        },
        {
            "id": "2",
            "name": "Collect Configuration",
            "description": "Project and dataset settings",
            "status": "pending"
        },
        {
            "id": "3",
            "name": "Authenticate",
            "description": "GCP authentication",
            "status": "pending"
        },
        {
            "id": "4",
            "name": "Check Permissions",
            "description": "Verify IAM roles",
            "status": "pending"
        },
        {
            "id": "5",
            "name": "Enable APIs",
            "description": "Enable required GCP APIs",
            "status": "pending"
        },
        {
            "id": "6",
            "name": "Configure Telemetry",
            "description": "Update Gemini CLI settings",
            "status": "pending"
        },
        {
            "id": "7",
            "name": "Create Dataset",
            "description": "BigQuery dataset setup",
            "status": "pending"
        },
        {
            "id": "8",
            "name": "Test Logging",
            "description": "Verify Cloud Logging",
            "status": "pending"
        },
        {
            "id": "9",
            "name": "Create Log Sink",
            "description": "Setup logging to BigQuery",
            "status": "pending"
        },
        {
            "id": "10",
            "name": "Verify Sink",
            "description": "Check sink configuration",
            "status": "pending"
        },
        {
            "id": "11",
            "name": "End-to-End Test",
            "description": "Verify complete data flow",
            "status": "pending"
        }
    ]


async def list_deployments() -> list:
    """List all deployments."""
    return list(_deployments.values())


async def delete_deployment(deployment_id: str) -> bool:
    """Delete a deployment record."""
    if deployment_id in _deployments:
        del _deployments[deployment_id]
        logger.info(f"Deleted deployment: {deployment_id}")
        return True
    else:
        logger.warning(f"Deployment not found for deletion: {deployment_id}")
        return False
