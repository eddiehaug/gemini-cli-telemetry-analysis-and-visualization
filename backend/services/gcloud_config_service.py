"""
gcloud Configuration Management Service.

Manages named gcloud configurations to support simultaneous authentication
to multiple GCP projects (e.g., Gemini CLI project + Telemetry project).

This allows:
- Gemini CLI to run in one project
- Infrastructure deployment in another project
- No conflict between project contexts
"""

import subprocess
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)


async def create_configuration(
    config_name: str,
    project_id: str,
    account_email: Optional[str] = None
) -> Dict:
    """
    Create a named gcloud configuration for a specific project.

    Args:
        config_name: Name for the configuration (e.g., "gemini-cli-project123")
        project_id: GCP project ID to associate with this configuration
        account_email: Email of authenticated account (optional - uses active account if not provided)

    Returns:
        {
            "config_name": str,
            "project_id": str,
            "account": str,
            "status": "created" or "already_exists"
        }
    """
    try:
        logger.info(f"Creating gcloud configuration: {config_name}")
        logger.info(f"  Project: {project_id}")
        logger.info(f"  Account: {account_email or 'active account'}")

        # Check if configuration already exists
        list_result = subprocess.run(
            ["gcloud", "config", "configurations", "list", "--format=value(name)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        existing_configs = [c.strip() for c in list_result.stdout.strip().split('\n') if c.strip()]

        if config_name in existing_configs:
            logger.info(f"Configuration '{config_name}' already exists - updating it")
            status = "already_exists"
        else:
            # Create new configuration
            create_result = subprocess.run(
                ["gcloud", "config", "configurations", "create", config_name, "--no-activate"],
                capture_output=True,
                text=True,
                timeout=10
            )

            if create_result.returncode != 0:
                raise Exception(f"Failed to create configuration: {create_result.stderr}")

            logger.info(f"✓ Created configuration: {config_name}")
            status = "created"

        # Set project for this configuration
        set_project_result = subprocess.run(
            ["gcloud", "config", "set", "project", project_id, "--configuration", config_name],
            capture_output=True,
            text=True,
            timeout=10
        )

        if set_project_result.returncode != 0:
            raise Exception(f"Failed to set project: {set_project_result.stderr}")

        logger.info(f"  ✓ Set project: {project_id}")

        # Set account if provided
        if account_email:
            set_account_result = subprocess.run(
                ["gcloud", "config", "set", "account", account_email, "--configuration", config_name],
                capture_output=True,
                text=True,
                timeout=10
            )

            if set_account_result.returncode != 0:
                logger.warning(f"Could not set account: {set_account_result.stderr}")
                # Don't fail - account might already be set
            else:
                logger.info(f"  ✓ Set account: {account_email}")

        logger.info(f"✓ Configuration '{config_name}' ready for use")

        return {
            "config_name": config_name,
            "project_id": project_id,
            "account": account_email or "active account",
            "status": status
        }

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout creating configuration '{config_name}'")
        raise Exception(f"Timeout creating gcloud configuration")
    except Exception as e:
        logger.error(f"Failed to create configuration: {str(e)}")
        raise


async def delete_configuration(config_name: str) -> Dict:
    """
    Delete a named gcloud configuration.

    Args:
        config_name: Name of the configuration to delete

    Returns:
        {
            "config_name": str,
            "status": "deleted" or "not_found"
        }
    """
    try:
        logger.info(f"Deleting gcloud configuration: {config_name}")

        # Check if configuration exists
        list_result = subprocess.run(
            ["gcloud", "config", "configurations", "list", "--format=value(name)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        existing_configs = [c.strip() for c in list_result.stdout.strip().split('\n') if c.strip()]

        if config_name not in existing_configs:
            logger.info(f"Configuration '{config_name}' does not exist (already deleted)")
            return {
                "config_name": config_name,
                "status": "not_found"
            }

        # Delete configuration
        delete_result = subprocess.run(
            ["gcloud", "config", "configurations", "delete", config_name, "--quiet"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if delete_result.returncode != 0:
            logger.warning(f"Could not delete configuration: {delete_result.stderr}")
            # Don't fail - configuration might be in use
            return {
                "config_name": config_name,
                "status": "deletion_failed",
                "error": delete_result.stderr
            }

        logger.info(f"✓ Deleted configuration: {config_name}")

        return {
            "config_name": config_name,
            "status": "deleted"
        }

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout deleting configuration '{config_name}'")
        raise Exception(f"Timeout deleting gcloud configuration")
    except Exception as e:
        logger.error(f"Failed to delete configuration: {str(e)}")
        raise


async def get_configuration_details(config_name: str) -> Optional[Dict]:
    """
    Get details about a named configuration.

    Args:
        config_name: Name of the configuration

    Returns:
        {
            "name": str,
            "is_active": bool,
            "account": str,
            "project": str
        } or None if not found
    """
    try:
        # Get configuration details
        result = subprocess.run(
            ["gcloud", "config", "configurations", "describe", config_name, "--format=json"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            logger.warning(f"Configuration '{config_name}' not found")
            return None

        import json
        config_data = json.loads(result.stdout)

        # Get properties
        properties = config_data.get("properties", {})
        core = properties.get("core", {})

        return {
            "name": config_data.get("name"),
            "is_active": config_data.get("is_active", False),
            "account": core.get("account"),
            "project": core.get("project")
        }

    except Exception as e:
        logger.warning(f"Could not get configuration details: {str(e)}")
        return None


def get_config_name_for_project(project_id: str, project_type: str = "telemetry") -> str:
    """
    Generate a standardized configuration name for a project.

    Args:
        project_id: GCP project ID
        project_type: Type of project ("telemetry" or "gemini-cli")

    Returns:
        Configuration name (e.g., "telemetry-project123" or "gemini-cli-project456")
    """
    # Truncate project ID if too long (gcloud config names have limits)
    max_length = 50
    suffix = project_id[:max_length - len(project_type) - 1]
    return f"{project_type}-{suffix}"
