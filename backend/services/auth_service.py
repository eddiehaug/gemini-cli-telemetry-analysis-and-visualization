"""
Authentication service.
Handles Google Cloud authentication.
"""
import subprocess
import logging
import os
from typing import Dict

logger = logging.getLogger(__name__)


async def authenticate() -> Dict:
    """
    Verify Google Cloud authentication.
    Checks if user is authenticated via gcloud.
    """
    try:
        # Check if already authenticated
        result = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0 and result.stdout.strip():
            account = result.stdout.strip().split('\n')[0]
            logger.info(f"Already authenticated as: {account}")

            # Verify application default credentials exist
            adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
            if not os.path.exists(adc_path):
                logger.warning("Application default credentials not found, creating...")
                await setup_application_default_credentials()

            return {
                "authenticated": True,
                "account": account,
                "method": "gcloud"
            }
        else:
            # Not authenticated, need to authenticate
            logger.error("No active authentication found")
            raise Exception(
                "Not authenticated with Google Cloud. Please run 'gcloud auth login' "
                "and 'gcloud auth application-default login' before deploying."
            )

    except subprocess.TimeoutExpired:
        logger.error("Authentication check timed out")
        raise Exception("Authentication verification timed out")
    except Exception as e:
        logger.error(f"Authentication check failed: {str(e)}")
        raise


async def setup_application_default_credentials() -> bool:
    """
    Ensure application default credentials are set up.
    This is required for the Google Cloud libraries to work.
    """
    try:
        result = subprocess.run(
            ["gcloud", "auth", "application-default", "print-access-token"],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode != 0:
            logger.warning("Application default credentials not set up")
            raise Exception(
                "Application default credentials not configured. "
                "Please run 'gcloud auth application-default login'"
            )

        logger.info("Application default credentials verified")
        return True

    except subprocess.TimeoutExpired:
        logger.error("ADC setup timed out")
        raise Exception("Application default credentials setup timed out")
    except Exception as e:
        logger.error(f"ADC setup failed: {str(e)}")
        raise


async def get_active_account() -> str:
    """Get the currently active gcloud account."""
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
            raise Exception("No active account found")

    except Exception as e:
        logger.error(f"Failed to get active account: {str(e)}")
        raise


async def authenticate_oauth_flow(project_id: str) -> Dict:
    """
    Authenticate with a specific project using OAuth browser flow.

    This is used when the Gemini CLI project is different from the
    telemetry project and needs separate authentication.

    Opens browser window for Google Login OAuth flow. Provides manual
    URL as fallback if browser doesn't open automatically.

    Args:
        project_id: GCP project ID to authenticate against

    Returns:
        Dict with auth status, account email, and method

    Raises:
        Exception if authentication fails or times out
    """
    try:
        logger.info(f"Starting OAuth flow for project: {project_id}")

        # Use gcloud auth login with project-specific context
        # This will open a browser window automatically
        result = subprocess.run(
            ["gcloud", "auth", "login", "--project", project_id, "--brief"],
            capture_output=True,
            text=True,
            timeout=120  # OAuth flow can take time
        )

        if result.returncode != 0:
            # If automatic browser open failed, provide manual URL
            if "browser" in result.stderr.lower() or "open" in result.stderr.lower():
                logger.warning("Browser didn't open automatically, providing manual URL...")

                # Get manual auth URL
                url_result = subprocess.run(
                    ["gcloud", "auth", "login", "--project", project_id, "--no-launch-browser"],
                    capture_output=True,
                    text=True,
                    timeout=120
                )

                # Extract URL from output
                # Format: "Go to the following link in your browser: https://..."
                import re
                url_match = re.search(r'https://accounts\.google\.com[^\s]+', url_result.stdout)
                if url_match:
                    auth_url = url_match.group(0)
                    raise Exception(
                        f"MANUAL_AUTH_REQUIRED:{auth_url}\n\n"
                        f"Browser didn't open automatically. Please:\n"
                        f"1. Open this URL in your browser\n"
                        f"2. Complete the authentication\n"
                        f"3. Click 'Retry Authentication' below"
                    )

            raise Exception(f"OAuth authentication failed: {result.stderr}")

        # Get the authenticated account
        account = await get_active_account()

        logger.info(f"OAuth authentication successful for account: {account}")

        # Create named gcloud configuration for this project
        # This allows simultaneous authentication to multiple projects
        from services import gcloud_config_service

        config_name = gcloud_config_service.get_config_name_for_project(project_id, "gemini-cli")
        logger.info(f"Creating gcloud configuration for Gemini CLI project: {config_name}")

        config_result = await gcloud_config_service.create_configuration(
            config_name=config_name,
            project_id=project_id,
            account_email=account
        )

        logger.info(f"✓ Created gcloud configuration: {config_name}")
        logger.info(f"  Project: {project_id}")
        logger.info(f"  Account: {account}")

        return {
            "authenticated": True,
            "account": account,
            "project_id": project_id,
            "method": "oauth",
            "config_name": config_name,
            "config_status": config_result.get("status")
        }

    except subprocess.TimeoutExpired:
        logger.error("OAuth authentication timed out")
        raise Exception("OAuth authentication timed out after 120 seconds")
    except Exception as e:
        logger.error(f"OAuth flow failed: {str(e)}")
        raise
