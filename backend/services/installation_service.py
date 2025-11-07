"""
Installation service for gcloud CLI and gemini CLI.
Automatically installs missing dependencies based on the operating system.
"""
import subprocess
import platform
import os
import logging
from typing import Dict

logger = logging.getLogger(__name__)


async def install_gcloud_cli() -> Dict:
    """
    Install gcloud CLI based on the operating system.

    Returns:
        Dict with installation status and details
    """
    try:
        os_type = platform.system()
        logger.info(f"Installing gcloud CLI on {os_type}...")

        if os_type == "Darwin":  # macOS
            return await install_gcloud_macos()
        elif os_type == "Linux":
            return await install_gcloud_linux()
        elif os_type == "Windows":
            return await install_gcloud_windows()
        else:
            raise Exception(f"Unsupported operating system: {os_type}")

    except Exception as e:
        logger.error(f"gcloud CLI installation failed: {str(e)}")
        raise


async def install_gcloud_macos() -> Dict:
    """Install gcloud CLI on macOS using Homebrew or direct download."""
    try:
        # Check if Homebrew is available
        homebrew_check = subprocess.run(
            ["which", "brew"],
            capture_output=True,
            text=True
        )

        if homebrew_check.returncode == 0:
            # Use Homebrew (recommended method)
            logger.info("Installing gcloud CLI via Homebrew...")
            result = subprocess.run(
                ["brew", "install", "--cask", "google-cloud-sdk"],
                capture_output=True,
                text=True,
                timeout=300  # 5 minutes
            )

            if result.returncode == 0:
                logger.info("gcloud CLI installed successfully via Homebrew")
                return {
                    "installed": True,
                    "method": "homebrew",
                    "message": "gcloud CLI installed via Homebrew"
                }
            else:
                raise Exception(f"Homebrew installation failed: {result.stderr}")
        else:
            # Fallback to direct download
            logger.warning("Homebrew not found, using direct download method")
            return await install_gcloud_direct_download("Darwin")

    except subprocess.TimeoutExpired:
        raise Exception("gcloud CLI installation timed out (5 minutes)")
    except Exception as e:
        logger.error(f"macOS installation failed: {str(e)}")
        raise


async def install_gcloud_linux() -> Dict:
    """Install gcloud CLI on Linux."""
    try:
        # Check which package manager is available
        apt_check = subprocess.run(["which", "apt-get"], capture_output=True)
        yum_check = subprocess.run(["which", "yum"], capture_output=True)

        if apt_check.returncode == 0:
            # Debian/Ubuntu
            return await install_gcloud_debian()
        elif yum_check.returncode == 0:
            # RHEL/CentOS/Fedora
            return await install_gcloud_redhat()
        else:
            # Fallback to direct download
            return await install_gcloud_direct_download("Linux")

    except Exception as e:
        logger.error(f"Linux installation failed: {str(e)}")
        raise


async def install_gcloud_debian() -> Dict:
    """Install gcloud CLI on Debian/Ubuntu using apt."""
    try:
        logger.info("Installing gcloud CLI on Debian/Ubuntu...")

        # Add Google Cloud repository
        commands = [
            "echo 'deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main' | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list",
            "curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg",
            "sudo apt-get update",
            "sudo apt-get install -y google-cloud-cli"
        ]

        for cmd in commands:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode != 0:
                raise Exception(f"Command failed: {cmd}\nError: {result.stderr}")

        logger.info("gcloud CLI installed successfully via apt")
        return {
            "installed": True,
            "method": "apt",
            "message": "gcloud CLI installed via apt"
        }

    except Exception as e:
        logger.error(f"Debian installation failed: {str(e)}")
        raise


async def install_gcloud_redhat() -> Dict:
    """Install gcloud CLI on RHEL/CentOS/Fedora using yum."""
    try:
        logger.info("Installing gcloud CLI on RHEL/CentOS/Fedora...")

        commands = [
            "sudo tee -a /etc/yum.repos.d/google-cloud-sdk.repo << EOM\n[google-cloud-cli]\nname=Google Cloud CLI\nbaseurl=https://packages.cloud.google.com/yum/repos/cloud-sdk-el9-x86_64\nenabled=1\ngpgcheck=1\nrepo_gpgcheck=0\ngpgkey=https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg\nEOM",
            "sudo yum install -y google-cloud-cli"
        ]

        for cmd in commands:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=120
            )
            if result.returncode != 0:
                raise Exception(f"Command failed: {cmd}\nError: {result.stderr}")

        logger.info("gcloud CLI installed successfully via yum")
        return {
            "installed": True,
            "method": "yum",
            "message": "gcloud CLI installed via yum"
        }

    except Exception as e:
        logger.error(f"RedHat installation failed: {str(e)}")
        raise


async def install_gcloud_direct_download(os_type: str) -> Dict:
    """Install gcloud CLI via direct download (fallback method)."""
    try:
        logger.info(f"Installing gcloud CLI via direct download for {os_type}...")

        import tempfile
        import tarfile

        # Determine download URL and architecture
        if os_type == "Darwin":
            machine = platform.machine()
            if machine == "arm64":
                url = "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-darwin-arm.tar.gz"
            else:
                url = "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-darwin-x86_64.tar.gz"
        else:  # Linux
            url = "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-linux-x86_64.tar.gz"

        # Download to temp directory
        with tempfile.TemporaryDirectory() as temp_dir:
            archive_path = os.path.join(temp_dir, "google-cloud-cli.tar.gz")

            logger.info(f"Downloading from {url}...")
            subprocess.run(
                ["curl", "-o", archive_path, url],
                check=True,
                timeout=300
            )

            # Extract to home directory
            install_dir = os.path.expanduser("~/google-cloud-sdk")
            logger.info(f"Extracting to {install_dir}...")

            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(path=os.path.expanduser("~"))

            # Run install script
            install_script = os.path.join(install_dir, "install.sh")
            logger.info("Running installation script...")
            subprocess.run(
                [install_script, "--quiet"],
                timeout=120
            )

        logger.info("gcloud CLI installed successfully via direct download")
        return {
            "installed": True,
            "method": "direct_download",
            "message": f"gcloud CLI installed to {install_dir}",
            "note": "You may need to restart your shell or run: source ~/google-cloud-sdk/path.bash.inc"
        }

    except Exception as e:
        logger.error(f"Direct download installation failed: {str(e)}")
        raise


async def install_gcloud_windows() -> Dict:
    """Install gcloud CLI on Windows."""
    try:
        logger.info("Installing gcloud CLI on Windows...")

        import tempfile

        # Download installer
        url = "https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe"

        with tempfile.TemporaryDirectory() as temp_dir:
            installer_path = os.path.join(temp_dir, "GoogleCloudSDKInstaller.exe")

            logger.info(f"Downloading installer from {url}...")
            subprocess.run(
                ["curl", "-o", installer_path, url],
                check=True,
                timeout=300
            )

            # Run installer
            logger.info("Running installer...")
            result = subprocess.run(
                [installer_path, "/S"],  # Silent installation
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode == 0:
                logger.info("gcloud CLI installed successfully on Windows")
                return {
                    "installed": True,
                    "method": "windows_installer",
                    "message": "gcloud CLI installed on Windows"
                }
            else:
                raise Exception(f"Installer failed: {result.stderr}")

    except Exception as e:
        logger.error(f"Windows installation failed: {str(e)}")
        raise


async def install_gemini_cli() -> Dict:
    """
    Install gemini CLI using npm.

    Returns:
        Dict with installation status and details
    """
    try:
        logger.info("Installing gemini CLI via npm...")

        # Check if npm is available
        npm_check = subprocess.run(
            ["which", "npm"],
            capture_output=True,
            text=True
        )

        if npm_check.returncode != 0:
            raise Exception("npm not found. Please install Node.js (v18+) first: https://nodejs.org/")

        # Install globally via npm
        result = subprocess.run(
            ["npm", "install", "-g", "@google/gemini-cli"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes
        )

        if result.returncode == 0:
            logger.info("gemini CLI installed successfully via npm")
            return {
                "installed": True,
                "method": "npm",
                "message": "gemini CLI installed via npm",
                "version_check": await check_gemini_version()
            }
        else:
            raise Exception(f"npm installation failed: {result.stderr}")

    except subprocess.TimeoutExpired:
        raise Exception("gemini CLI installation timed out (5 minutes)")
    except Exception as e:
        logger.error(f"gemini CLI installation failed: {str(e)}")
        raise


async def check_gemini_version() -> str:
    """Check installed gemini CLI version."""
    try:
        result = subprocess.run(
            ["gemini", "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.stdout.strip() if result.returncode == 0 else "unknown"
    except:
        return "unknown"


async def verify_installation(tool_name: str) -> bool:
    """Verify that a tool was installed successfully."""
    try:
        result = subprocess.run(
            ["which", tool_name],
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except:
        return False
