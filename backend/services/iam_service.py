"""
IAM permission checking service.
Verifies required IAM roles and waits for propagation.
"""
import subprocess
import asyncio
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

# Required IAM roles for the deployment
# Updated for ELT architecture: Pub/Sub -> Dataflow -> BigQuery
REQUIRED_ROLES = [
    "roles/bigquery.admin",         # BigQuery dataset/table management (includes Data Transfer permissions)
    "roles/logging.admin",          # Cloud Logging sink creation
    "roles/serviceusage.serviceUsageAdmin",  # API enablement
    "roles/dataflow.admin",         # Dataflow pipeline deployment
    "roles/storage.admin",          # GCS bucket for UDF storage
]


async def check_permissions(project_id: str) -> Dict:
    """
    Check if the authenticated user has required IAM permissions.

    Returns:
        Dict with hasAll, missingRoles, and currentRoles
    """
    try:
        # Get the current authenticated user
        user_email = await get_current_user()
        logger.info(f"Checking permissions for user: {user_email}")

        # Get current roles for the user
        current_roles = await get_user_roles(project_id, user_email)
        logger.info(f"Current roles: {current_roles}")

        # Check which required roles are missing
        missing_roles = [role for role in REQUIRED_ROLES if role not in current_roles]

        if missing_roles:
            logger.warning(f"Missing required roles: {missing_roles}")
            # Try to grant missing roles (will fail if user doesn't have permission)
            await attempt_grant_roles(project_id, user_email, missing_roles)

            # Wait for IAM propagation (90 seconds as per implementation plan)
            logger.info("Waiting for IAM propagation (90 seconds)...")
            await asyncio.sleep(90)

            # Re-check roles after propagation
            current_roles = await get_user_roles(project_id, user_email)
            missing_roles = [role for role in REQUIRED_ROLES if role not in current_roles]

        has_all = len(missing_roles) == 0

        return {
            "hasAll": has_all,
            "missingRoles": missing_roles,
            "currentRoles": current_roles
        }

    except Exception as e:
        logger.error(f"Permission check failed: {str(e)}")
        raise


async def get_current_user() -> str:
    """Get the currently authenticated user email."""
    try:
        result = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().split('\n')[0]
        else:
            raise Exception("Could not determine authenticated user")

    except Exception as e:
        logger.error(f"Failed to get current user: {str(e)}")
        raise


async def get_user_roles(project_id: str, user_email: str) -> List[str]:
    """Get all IAM roles assigned to the user for the project."""
    try:
        result = subprocess.run(
            [
                "gcloud", "projects", "get-iam-policy", project_id,
                "--flatten=bindings[].members",
                f"--filter=bindings.members:user:{user_email}",
                "--format=value(bindings.role)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            roles = [role.strip() for role in result.stdout.strip().split('\n') if role.strip()]
            return roles
        else:
            logger.warning(f"Could not get user roles: {result.stderr}")
            return []

    except Exception as e:
        logger.error(f"Failed to get user roles: {str(e)}")
        return []


async def attempt_grant_roles(project_id: str, user_email: str, roles: List[str]) -> bool:
    """
    Attempt to grant missing roles to the user.
    This will only succeed if the current user has permission to grant roles.
    """
    try:
        for role in roles:
            logger.info(f"Attempting to grant role: {role}")
            result = subprocess.run(
                [
                    "gcloud", "projects", "add-iam-policy-binding", project_id,
                    f"--member=user:{user_email}",
                    f"--role={role}",
                    "--condition=None"
                ],
                capture_output=True,
                text=True,
                timeout=30
            )

            if result.returncode == 0:
                logger.info(f"Successfully granted role: {role}")
            else:
                logger.warning(f"Could not grant role {role}: {result.stderr}")

        return True

    except Exception as e:
        logger.warning(f"Failed to grant roles: {str(e)}")
        # Don't fail hard - user might already have roles or need admin to grant them
        return False


async def wait_for_iam_propagation(seconds: int = 90) -> None:
    """
    Wait for IAM policy changes to propagate globally.

    IAM changes can take up to 7 minutes to propagate, but we use 90 seconds
    as a reasonable balance per the implementation plan.
    """
    logger.info(f"Waiting {seconds} seconds for IAM propagation...")
    await asyncio.sleep(seconds)
    logger.info("IAM propagation wait complete")
