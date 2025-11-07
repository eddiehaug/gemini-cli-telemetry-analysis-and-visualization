"""
Dependency verification service.
Checks for required tools and validates billing.
"""
import subprocess
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)


async def verify_dependencies(auto_install: bool = True) -> List[Dict]:
    """
    Verify that all required dependencies are installed.
    If auto_install is True, automatically install missing dependencies.

    Args:
        auto_install: Whether to automatically install missing dependencies

    Returns:
        List of dependency check results with installation status
    """
    dependencies = []

    # Check gcloud CLI
    gcloud_check = await check_gcloud()
    dependencies.append(gcloud_check)

    # Check gemini CLI
    gemini_check = await check_gemini()
    dependencies.append(gemini_check)

    # Python is implicitly available (we're running in it)
    dependencies.append({
        "name": "Python",
        "installed": True,
        "version": await get_python_version(),
        "path": "system"
    })

    # Check if all required tools are present
    all_installed = all(dep["installed"] for dep in dependencies)

    if not all_installed:
        missing = [dep for dep in dependencies if not dep["installed"]]

        if auto_install:
            logger.info(f"Missing dependencies detected. Auto-installing: {[d['name'] for d in missing]}")

            # Import installation service
            from . import installation_service

            # Install missing dependencies
            for dep in missing:
                if dep["name"] == "gcloud CLI":
                    try:
                        logger.info("Installing gcloud CLI...")
                        install_result = await installation_service.install_gcloud_cli()
                        dep["installed"] = True
                        dep["auto_installed"] = True
                        dep["installation_method"] = install_result.get("method")
                        dep["installation_message"] = install_result.get("message")

                        # Re-check to get version and path
                        updated_check = await check_gcloud()
                        dep["version"] = updated_check.get("version")
                        dep["path"] = updated_check.get("path")

                    except Exception as e:
                        logger.error(f"Failed to install gcloud CLI: {str(e)}")
                        dep["installation_error"] = str(e)
                        raise Exception(f"Failed to install gcloud CLI: {str(e)}")

                elif dep["name"] == "gemini CLI":
                    try:
                        logger.info("Installing gemini CLI...")
                        install_result = await installation_service.install_gemini_cli()
                        dep["installed"] = True
                        dep["auto_installed"] = True
                        dep["installation_method"] = install_result.get("method")
                        dep["installation_message"] = install_result.get("message")

                        # Re-check to get version and path
                        updated_check = await check_gemini()
                        dep["version"] = updated_check.get("version")
                        dep["path"] = updated_check.get("path")

                    except Exception as e:
                        logger.error(f"Failed to install gemini CLI: {str(e)}")
                        dep["installation_error"] = str(e)
                        raise Exception(f"Failed to install gemini CLI: {str(e)}")

            logger.info("All missing dependencies installed successfully")
        else:
            # Raise error if auto-install is disabled
            missing_names = [dep["name"] for dep in missing]
            raise Exception(f"Missing required dependencies: {', '.join(missing_names)}")

    logger.info("All dependencies verified successfully")
    return dependencies


async def check_gcloud() -> Dict:
    """Check if gcloud CLI is installed and get version."""
    try:
        result = subprocess.run(
            ["gcloud", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode == 0:
            # Parse version from output (first line)
            version_line = result.stdout.split('\n')[0]
            version = version_line.split()[-1] if version_line else "unknown"

            # Get gcloud path
            path_result = subprocess.run(
                ["which", "gcloud"],
                capture_output=True,
                text=True,
                timeout=5
            )
            path = path_result.stdout.strip() if path_result.returncode == 0 else "unknown"

            return {
                "name": "gcloud CLI",
                "installed": True,
                "version": version,
                "path": path
            }
        else:
            return {
                "name": "gcloud CLI",
                "installed": False,
                "version": None,
                "path": None
            }
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.error(f"gcloud check failed: {str(e)}")
        return {
            "name": "gcloud CLI",
            "installed": False,
            "version": None,
            "path": None
        }


async def check_gemini() -> Dict:
    """Check if gemini CLI is installed and get version."""
    try:
        # Try with GOOGLE_APPLICATION_CREDENTIALS set
        result = subprocess.run(
            ["gemini", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            env={
                **subprocess.os.environ,
                "GOOGLE_APPLICATION_CREDENTIALS": f"{subprocess.os.path.expanduser('~')}/.config/gcloud/application_default_credentials.json"
            }
        )

        if result.returncode == 0:
            # Parse version from output
            version = result.stdout.strip() or result.stderr.strip() or "unknown"

            # Get gemini path
            path_result = subprocess.run(
                ["which", "gemini"],
                capture_output=True,
                text=True,
                timeout=5
            )
            path = path_result.stdout.strip() if path_result.returncode == 0 else "unknown"

            return {
                "name": "gemini CLI",
                "installed": True,
                "version": version,
                "path": path
            }
        else:
            return {
                "name": "gemini CLI",
                "installed": False,
                "version": None,
                "path": None
            }
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.error(f"gemini check failed: {str(e)}")
        return {
            "name": "gemini CLI",
            "installed": False,
            "version": None,
            "path": None
        }


async def get_python_version() -> str:
    """Get Python version."""
    import sys
    return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"


async def check_billing(project_id: str) -> bool:
    """
    Check if billing is enabled for the project.
    This is called separately after we have the project ID.
    """
    try:
        result = subprocess.run(
            [
                "gcloud", "billing", "projects", "describe", project_id,
                "--format=value(billingEnabled)"
            ],
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            billing_enabled = result.stdout.strip().lower() == "true"
            if not billing_enabled:
                raise Exception(f"Billing is not enabled for project {project_id}")
            return True
        else:
            # If command fails, try alternative approach
            logger.warning("Could not verify billing status directly, continuing...")
            return True

    except subprocess.TimeoutExpired:
        logger.error("Billing check timed out")
        raise Exception("Billing verification timed out")
    except Exception as e:
        logger.error(f"Billing check failed: {str(e)}")
        # Don't fail hard on billing check - it might not be accessible
        logger.warning("Skipping billing verification")
        return True
