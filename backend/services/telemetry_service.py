"""
Telemetry configuration service.
Configures Gemini CLI telemetry settings via settings.json file.
"""
import json
import os
import logging
from typing import Dict
from pathlib import Path

logger = logging.getLogger(__name__)

# Path to Gemini CLI settings file
GEMINI_SETTINGS_PATH = Path.home() / ".gemini" / "settings.json"


async def configure_telemetry(
    log_prompts: bool,
    inference_project_id: str,
    telemetry_project_id: str,
    auth_method: str = "oauth",
    gemini_region: str = None
) -> Dict:
    """
    Configure Gemini CLI telemetry settings by editing settings.json and shell profile.

    Args:
        log_prompts: Whether to log prompts and responses
        inference_project_id: GCP project ID for Gemini API inference
        telemetry_project_id: GCP project ID for telemetry data collection
        auth_method: Authentication method ('oauth' or 'vertex-ai')
        gemini_region: Region for Gemini API calls (required for Vertex AI)

    Returns:
        Dict with configuration status
    """
    try:
        # Validate Vertex AI requires region
        if auth_method == "vertex-ai" and not gemini_region:
            raise ValueError("Gemini region is required for Vertex AI authentication")

        logger.info(f"Configuring Gemini CLI telemetry (log_prompts={log_prompts}, inference={inference_project_id}, telemetry={telemetry_project_id}, auth={auth_method}, region={gemini_region})...")

        # Step 1: Read current settings
        settings = await read_gemini_settings()

        # Step 2: Update telemetry configuration
        if "telemetry" not in settings:
            settings["telemetry"] = {}

        settings["telemetry"]["enabled"] = True
        settings["telemetry"]["target"] = "gcp"

        # Explicitly set logPrompts (camelCase, not snake_case!)
        # Default is true, so we must explicitly set to false to disable prompt logging
        settings["telemetry"]["logPrompts"] = log_prompts

        # Step 3: Configure environment variables in settings.json
        await configure_environment_variables_in_settings(
            settings,
            inference_project_id,
            telemetry_project_id,
            auth_method,
            gemini_region
        )

        # Step 4: Write updated settings back
        await write_gemini_settings(settings)

        # Step 5: Configure environment variables in shell profile
        shell_result = await configure_environment_variables_in_shell(
            inference_project_id,
            telemetry_project_id,
            auth_method,
            gemini_region
        )

        logger.info("Telemetry configured successfully")
        return {
            "enabled": True,
            "log_prompts": log_prompts,
            "config": settings.get("telemetry", {}),
            "env_vars": settings.get("env", {}),
            "shell_profile": shell_result
        }

    except Exception as e:
        logger.error(f"Telemetry configuration failed: {str(e)}")
        raise


async def configure_environment_variables_in_settings(
    settings: Dict,
    inference_project_id: str,
    telemetry_project_id: str,
    auth_method: str = "oauth",
    gemini_region: str = None
) -> None:
    """
    Configure environment variables in settings.json.

    Args:
        settings: Current settings dictionary (modified in place)
        inference_project_id: GCP project ID for Gemini API inference (can be None/empty for same-project setup)
        telemetry_project_id: GCP project ID for telemetry data collection
        auth_method: Authentication method ('oauth' or 'vertex-ai')
        gemini_region: Region for Gemini API calls (required for Vertex AI headless mode)
    """
    try:
        if "env" not in settings:
            settings["env"] = {}

        # Handle case where inference_project_id is None or empty (use telemetry_project_id for both)
        if not inference_project_id:
            inference_project_id = telemetry_project_id

        # Logic: If same project, set only GOOGLE_CLOUD_PROJECT
        #        If different projects, set BOTH variables
        if inference_project_id == telemetry_project_id:
            # Same project: only GOOGLE_CLOUD_PROJECT
            settings["env"]["GOOGLE_CLOUD_PROJECT"] = telemetry_project_id
            # Remove OTLP_GOOGLE_CLOUD_PROJECT if it exists
            settings["env"].pop("OTLP_GOOGLE_CLOUD_PROJECT", None)
            logger.info(f"Using same project for inference and telemetry: {telemetry_project_id}")
        else:
            # Different projects: set BOTH variables
            settings["env"]["GOOGLE_CLOUD_PROJECT"] = inference_project_id
            settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] = telemetry_project_id
            logger.info(f"Using different projects - inference: {inference_project_id}, telemetry: {telemetry_project_id}")

        # CRITICAL: For Vertex AI, set GOOGLE_CLOUD_LOCATION (required for headless mode)
        if auth_method == "vertex-ai" and gemini_region:
            settings["env"]["GOOGLE_CLOUD_LOCATION"] = gemini_region
            logger.info(f"Set GOOGLE_CLOUD_LOCATION={gemini_region} for Vertex AI headless mode")
        else:
            # Remove GOOGLE_CLOUD_LOCATION if switching from Vertex AI to OAuth
            settings["env"].pop("GOOGLE_CLOUD_LOCATION", None)

    except Exception as e:
        logger.error(f"Failed to configure environment variables in settings: {str(e)}")
        raise


async def configure_environment_variables_in_shell(
    inference_project_id: str,
    telemetry_project_id: str,
    auth_method: str = "oauth",
    gemini_region: str = None
) -> Dict:
    """
    Configure environment variables in shell profile (~/.bashrc, ~/.zshrc, ~/.bash_profile).

    Args:
        inference_project_id: GCP project ID for Gemini API inference
        telemetry_project_id: GCP project ID for telemetry data collection
        auth_method: Authentication method ('oauth' or 'vertex-ai')
        gemini_region: Region for Gemini API calls (required for Vertex AI headless mode)

    Returns:
        Dict with shell configuration status
    """
    try:
        # Step 1: Detect user's shell
        shell_path = os.environ.get("SHELL", "")
        shell_name = Path(shell_path).name if shell_path else "bash"

        # Step 2: Determine profile file based on shell
        profile_file = None
        home = Path.home()

        if "zsh" in shell_name:
            profile_file = home / ".zshrc"
        elif "bash" in shell_name:
            # Check for .bashrc first, then .bash_profile
            bashrc = home / ".bashrc"
            bash_profile = home / ".bash_profile"
            profile_file = bashrc if bashrc.exists() else bash_profile
        else:
            # Default to .bashrc for unknown shells
            profile_file = home / ".bashrc"

        logger.info(f"Detected shell: {shell_name}, using profile: {profile_file}")

        # Step 3: Prepare export statements
        export_lines = []
        marker_start = "# >>> Gemini CLI Telemetry Configuration >>>"
        marker_end = "# <<< Gemini CLI Telemetry Configuration <<<"

        # Handle case where inference_project_id is None or empty (use telemetry_project_id for both)
        if not inference_project_id:
            inference_project_id = telemetry_project_id

        if inference_project_id == telemetry_project_id:
            # Same project: only GOOGLE_CLOUD_PROJECT
            export_lines.append(f'export GOOGLE_CLOUD_PROJECT="{telemetry_project_id}"')
        else:
            # Different projects: both variables
            export_lines.append(f'export GOOGLE_CLOUD_PROJECT="{inference_project_id}"')
            export_lines.append(f'export OTLP_GOOGLE_CLOUD_PROJECT="{telemetry_project_id}"')

        # CRITICAL: For Vertex AI, add GOOGLE_CLOUD_LOCATION (required for headless mode)
        if auth_method == "vertex-ai" and gemini_region:
            export_lines.append(f'export GOOGLE_CLOUD_LOCATION="{gemini_region}"')

        # Step 4: Read existing profile content
        if profile_file.exists():
            with open(profile_file, 'r') as f:
                profile_content = f.read()
        else:
            profile_content = ""
            logger.info(f"Profile file {profile_file} does not exist, will create it")

        # Step 5: Remove old Gemini CLI configuration block if it exists
        if marker_start in profile_content:
            # Find and remove old block
            start_idx = profile_content.find(marker_start)
            end_idx = profile_content.find(marker_end)
            if end_idx != -1:
                # Remove old block (including the end marker line)
                end_of_line = profile_content.find('\n', end_idx)
                if end_of_line != -1:
                    profile_content = profile_content[:start_idx] + profile_content[end_of_line + 1:]
                else:
                    profile_content = profile_content[:start_idx]
            logger.info("Removed existing Gemini CLI configuration block")

        # Step 6: Append new configuration block
        new_block = "\n" + marker_start + "\n"
        new_block += "\n".join(export_lines) + "\n"
        new_block += marker_end + "\n"

        # Ensure there's a newline at the end if content exists
        if profile_content and not profile_content.endswith('\n'):
            profile_content += '\n'

        profile_content += new_block

        # Step 7: Write updated profile
        with open(profile_file, 'w') as f:
            f.write(profile_content)

        logger.info(f"Successfully wrote environment variables to {profile_file}")

        return {
            "profile_file": str(profile_file),
            "shell": shell_name,
            "variables_set": list(settings_key for settings_key in ["GOOGLE_CLOUD_PROJECT", "OTLP_GOOGLE_CLOUD_PROJECT"] if any(settings_key in line for line in export_lines))
        }

    except Exception as e:
        logger.error(f"Failed to configure environment variables in shell profile: {str(e)}")
        # Don't raise - shell profile configuration is optional, settings.json is the primary method
        return {
            "profile_file": None,
            "shell": None,
            "error": str(e)
        }


async def read_gemini_settings() -> Dict:
    """Read Gemini CLI settings from settings.json."""
    try:
        if not GEMINI_SETTINGS_PATH.exists():
            logger.warning(f"Settings file not found at {GEMINI_SETTINGS_PATH}")
            return {}

        with open(GEMINI_SETTINGS_PATH, 'r') as f:
            settings = json.load(f)

        logger.info(f"Successfully read settings from {GEMINI_SETTINGS_PATH}")
        return settings

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse settings.json: {str(e)}")
        raise Exception(f"Invalid JSON in settings file: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to read settings: {str(e)}")
        raise


async def write_gemini_settings(settings: Dict) -> None:
    """Write Gemini CLI settings to settings.json."""
    try:
        # Ensure directory exists
        GEMINI_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Write with pretty formatting
        with open(GEMINI_SETTINGS_PATH, 'w') as f:
            json.dump(settings, f, indent=2)

        logger.info(f"Successfully wrote settings to {GEMINI_SETTINGS_PATH}")

    except Exception as e:
        logger.error(f"Failed to write settings: {str(e)}")
        raise


async def get_telemetry_config() -> Dict:
    """Get current telemetry configuration."""
    try:
        settings = await read_gemini_settings()
        return settings.get("telemetry", {})

    except Exception as e:
        logger.error(f"Failed to get telemetry config: {str(e)}")
        return {}


async def verify_telemetry_enabled() -> bool:
    """Verify that telemetry is enabled in settings."""
    try:
        config = await get_telemetry_config()
        return config.get("enabled", False)

    except Exception as e:
        logger.error(f"Failed to verify telemetry: {str(e)}")
        return False
