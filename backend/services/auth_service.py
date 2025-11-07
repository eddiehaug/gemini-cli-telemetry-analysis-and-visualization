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


async def check_auth_status() -> Dict:
    """
    Check gcloud CLI installation and authentication status (non-blocking).

    This function checks if:
    1. gcloud CLI is installed
    2. User is authenticated with gcloud

    Unlike authenticate(), this does NOT raise exceptions if not authenticated.
    It simply returns the current status.

    Returns:
        Dict with:
        - gcloud_installed: bool
        - authenticated: bool
        - account: str | None
        - has_adc: bool (application default credentials exist)
    """
    result = {
        "gcloud_installed": False,
        "authenticated": False,
        "account": None,
        "has_adc": False
    }

    try:
        # Check if gcloud is installed by running gcloud version
        version_check = subprocess.run(
            ["gcloud", "version"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if version_check.returncode == 0:
            result["gcloud_installed"] = True
            logger.info("gcloud CLI is installed")
        else:
            logger.warning("gcloud CLI is not installed")
            return result

    except (FileNotFoundError, subprocess.TimeoutExpired):
        logger.warning("gcloud CLI not found or check timed out")
        return result

    # If gcloud is installed, check authentication
    try:
        auth_check = subprocess.run(
            ["gcloud", "auth", "list", "--filter=status:ACTIVE", "--format=value(account)"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if auth_check.returncode == 0 and auth_check.stdout.strip():
            account = auth_check.stdout.strip().split('\n')[0]
            result["authenticated"] = True
            result["account"] = account
            logger.info(f"User authenticated as: {account}")
        else:
            logger.info("User not authenticated with gcloud")

    except subprocess.TimeoutExpired:
        logger.warning("Auth check timed out")

    # Check for application default credentials
    try:
        adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        result["has_adc"] = os.path.exists(adc_path)
        if result["has_adc"]:
            logger.info("Application default credentials found")
        else:
            logger.info("Application default credentials not found")
    except Exception as e:
        logger.warning(f"Could not check ADC: {str(e)}")

    return result


async def initiate_oauth_flow() -> Dict:
    """
    Initiate OAuth authentication flow and return the auth URL.

    This starts the OAuth flow with --no-launch-browser to get the URL
    that the frontend can open in a new tab.

    Returns:
        Dict with:
        - auth_url: str (URL for user to visit)
        - message: str (instructions for user)

    Raises:
        Exception if OAuth initiation fails
    """
    try:
        logger.info("Initiating OAuth flow (no-launch-browser mode)")

        # Run gcloud auth login with --no-launch-browser to get URL
        result = subprocess.run(
            ["gcloud", "auth", "login", "--no-launch-browser"],
            capture_output=True,
            text=True,
            timeout=30,
            input="\n"  # Send newline to prevent hanging
        )

        # Extract URL from output (check both stdout and stderr)
        # Format: "Go to the following link in your browser: https://..."
        import re
        combined_output = result.stdout + "\n" + result.stderr

        # Try to match the OAuth URL - look for the complete URL on its own line
        url_match = re.search(r'https://accounts\.google\.com/o/oauth2/[^\s\n]+', combined_output)

        if url_match:
            auth_url = url_match.group(0)
            logger.info("OAuth URL generated successfully")

            return {
                "auth_url": auth_url,
                "message": "Please complete authentication in the browser window that will open."
            }
        else:
            logger.error(f"Could not extract OAuth URL from gcloud output. Output: {combined_output[:500]}")
            raise Exception("Failed to generate OAuth URL")

    except subprocess.TimeoutExpired:
        logger.error("OAuth initiation timed out")
        raise Exception("OAuth initiation timed out")
    except Exception as e:
        logger.error(f"Failed to initiate OAuth: {str(e)}")
        raise


async def authenticate_oauth_flow(project_id: str) -> Dict:
    """
    Authenticate with a specific project using OAuth browser flow to create ADC.

    This creates Application Default Credentials (ADC) required for Gemini CLI
    OAuth authentication mode. ADC is stored at:
    ~/.config/gcloud/application_default_credentials.json

    Opens browser window for Google Login OAuth flow. Provides manual
    URL as fallback if browser doesn't open automatically.

    Args:
        project_id: GCP project ID to authenticate against (used for quota attribution)

    Returns:
        Dict with auth status, account email, and method

    Raises:
        Exception if authentication fails or times out
    """
    try:
        logger.info(f"Starting ADC OAuth flow for project: {project_id}")
        logger.info("This will create Application Default Credentials for Gemini CLI OAuth mode")

        # Use gcloud auth application-default login to create ADC
        # This is required for Gemini CLI OAuth authentication
        result = subprocess.run(
            ["gcloud", "auth", "application-default", "login", "--project", project_id, "--quiet"],
            capture_output=True,
            text=True,
            timeout=120  # OAuth flow can take time
        )

        if result.returncode != 0:
            # If automatic browser open failed, provide manual URL
            if "browser" in result.stderr.lower() or "open" in result.stderr.lower():
                logger.warning("Browser didn't open automatically, providing manual URL...")

                # Get manual auth URL for ADC
                url_result = subprocess.run(
                    ["gcloud", "auth", "application-default", "login", "--project", project_id, "--no-launch-browser"],
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

            raise Exception(f"ADC OAuth authentication failed: {result.stderr}")

        # Get the authenticated account
        account = await get_active_account()

        logger.info(f"✓ Application Default Credentials created successfully")
        logger.info(f"  Account: {account}")
        logger.info(f"  Project: {project_id}")
        logger.info(f"  ADC file: ~/.config/gcloud/application_default_credentials.json")

        return {
            "authenticated": True,
            "account": account,
            "project_id": project_id,
            "method": "oauth_adc",
            "adc_created": True,
            "adc_path": "~/.config/gcloud/application_default_credentials.json"
        }

    except subprocess.TimeoutExpired:
        logger.error("OAuth authentication timed out")
        raise Exception("OAuth authentication timed out after 120 seconds")
    except Exception as e:
        logger.error(f"OAuth flow failed: {str(e)}")
        raise
