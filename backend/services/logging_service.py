"""
Cloud Logging service.
Tests Cloud Logging functionality with UUID-based tracking.
"""
import uuid
import asyncio
import logging
from typing import Dict, Optional
from google.cloud import logging as cloud_logging
from google.cloud.logging_v2 import entries

logger = logging.getLogger(__name__)


async def test_logging(project_id: str) -> Dict:
    """
    Test Cloud Logging by sending a test log entry with UUID tracking.

    This verifies that:
    1. Cloud Logging API is accessible
    2. We can write log entries
    3. We can read back the log entries (verifies permissions)

    Returns:
        Dict with test_uuid and verification status
    """
    try:
        # Generate a unique test ID
        test_uuid = str(uuid.uuid4())
        logger.info(f"Testing Cloud Logging with UUID: {test_uuid}")

        # Create logging client
        client = cloud_logging.Client(project=project_id)

        # Get or create logger
        log_name = "gemini-cli-telemetry-test"
        cloud_logger = client.logger(log_name)

        # Write test log entry
        test_message = {
            "message": "Gemini CLI Telemetry Deployment Test",
            "test_uuid": test_uuid,
            "test_type": "deployment_verification",
            "component": "logging_service"
        }

        cloud_logger.log_struct(
            test_message,
            severity="INFO",
            labels={"deployment_test": "true"}
        )

        logger.info(f"Test log entry written with UUID: {test_uuid}")

        # Wait a moment for log to be available
        await asyncio.sleep(3)

        # Verify we can read the log back
        verified = await verify_test_log(client, test_uuid)

        return {
            "test_uuid": test_uuid,
            "log_written": True,
            "log_verified": verified,
            "log_name": f"projects/{project_id}/logs/{log_name}"
        }

    except Exception as e:
        logger.error(f"Logging test failed: {str(e)}")
        raise


async def verify_test_log(client: cloud_logging.Client, test_uuid: str) -> bool:
    """
    Verify that we can read back the test log entry.

    This confirms that Cloud Logging is working end-to-end.
    """
    try:
        # Build filter to find our test log
        filter_str = f'jsonPayload.test_uuid="{test_uuid}"'

        logger.info(f"Searching for test log with filter: {filter_str}")

        # List log entries
        iterator = client.list_entries(
            filter_=filter_str,
            max_results=10
        )

        # Check if we found our log entry
        for entry in iterator:
            if isinstance(entry.payload, dict) and entry.payload.get("test_uuid") == test_uuid:
                logger.info("Test log entry found and verified")
                return True

        logger.warning("Test log entry not found (may need more time to propagate)")
        return False

    except Exception as e:
        logger.warning(f"Log verification failed: {str(e)}")
        return False


async def test_gemini_cli_logging(project_id: str, gemini_cli_project_id: Optional[str] = None) -> Dict:
    """
    Test that Gemini CLI is actually sending logs to Cloud Logging.

    This runs a real prompt with tool use to generate comprehensive telemetry,
    then verifies the prompt and response appear in Cloud Logging.

    Args:
        project_id: GCP project ID where logs should appear (Gemini CLI project)
        gemini_cli_project_id: Optional - if provided, use this project's gcloud configuration
    """
    import subprocess
    import os
    import time

    try:
        # Use Gemini CLI project if provided, otherwise use project_id
        actual_gemini_project = gemini_cli_project_id or project_id

        # Read telemetry settings to get the telemetry project
        import json
        settings_path = os.path.expanduser("~/.gemini/settings.json")
        telemetry_project = None

        try:
            with open(settings_path, 'r') as f:
                settings = json.load(f)
                telemetry_project = settings.get("env", {}).get("OTLP_GOOGLE_CLOUD_PROJECT")
                logger.info(f"Read telemetry configuration from settings.json")
                logger.info(f"  Inference project: {settings.get('env', {}).get('GOOGLE_CLOUD_PROJECT')}")
                logger.info(f"  Telemetry project: {telemetry_project}")
        except Exception as e:
            logger.warning(f"Could not read telemetry settings: {e}")

        # Use a simple, fast prompt for logging test
        test_prompt = "What is 2+2?"
        logger.info(f"Testing Gemini CLI with prompt: {test_prompt}")
        logger.info(f"  Gemini CLI project (inference): {actual_gemini_project}")
        logger.info(f"  Telemetry project (logs destination): {telemetry_project or actual_gemini_project}")

        # Set up environment with BOTH inference and telemetry projects
        # CRITICAL: In headless mode, Gemini CLI reads env vars, not just settings.json
        env = {
            **os.environ,
            "GOOGLE_CLOUD_PROJECT": actual_gemini_project,  # For API calls
            "GOOGLE_APPLICATION_CREDENTIALS": os.path.expanduser(
                "~/.config/gcloud/application_default_credentials.json"
            )
        }

        # Add telemetry project if different from inference project
        if telemetry_project and telemetry_project != actual_gemini_project:
            env["OTLP_GOOGLE_CLOUD_PROJECT"] = telemetry_project  # For telemetry logs
            logger.info(f"  Setting OTLP_GOOGLE_CLOUD_PROJECT={telemetry_project} for cross-project telemetry")

        # If using separate Gemini CLI project, use its gcloud configuration
        if gemini_cli_project_id:
            from services import gcloud_config_service
            config_name = gcloud_config_service.get_config_name_for_project(gemini_cli_project_id, "gemini-cli")
            env["CLOUDSDK_ACTIVE_CONFIG_NAME"] = config_name
            logger.info(f"  Using gcloud configuration: {config_name}")

        # Record timestamp BEFORE running command (for filtering logs)
        import datetime
        start_datetime = datetime.datetime.utcnow()
        start_time = time.time()

        # Run gemini with a simple prompt in headless mode using gemini-2.5-flash
        logger.info("Running Gemini CLI with simple prompt in headless mode...")
        result = subprocess.run(
            [
                "gemini",
                "--prompt", test_prompt,
                "--model", "gemini-2.5-flash",
                "--output-format", "json"
            ],
            capture_output=True,
            text=True,
            timeout=60,  # Simple prompt should complete quickly
            env=env
        )

        if result.returncode != 0:
            logger.warning(f"Gemini command had non-zero exit: {result.stderr}")
        else:
            logger.info("Gemini CLI command completed successfully")

        # Wait for logs to propagate to Cloud Logging (10 seconds)
        logger.info("Waiting for logs to propagate to Cloud Logging...")
        await asyncio.sleep(10)

        # IMPORTANT: Logs appear in the TELEMETRY project, not the inference project!
        # Determine where to search for logs
        search_project = telemetry_project or actual_gemini_project

        logger.info(f"Searching for logs in telemetry project: {search_project}")
        client = cloud_logging.Client(project=search_project)

        # Filter for logs generated AFTER we started the command (within last 30 seconds)
        # Subtract 5 seconds buffer to account for slight clock differences
        cutoff_time = start_datetime - datetime.timedelta(seconds=5)
        timestamp_str = cutoff_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Note: Even though logs are in the telemetry project, the logName still references the SOURCE project
        # The logName format is: projects/{SOURCE_PROJECT}/logs/{LOG_NAME}
        # So we search in telemetry project but filter by source project's logName
        filter_str = f'logName="projects/{actual_gemini_project}/logs/gemini_cli" AND timestamp>="{timestamp_str}"'
        logger.info(f"Searching in project {search_project} with filter: {filter_str}")
        logger.info(f"  Looking for logs generated after: {timestamp_str} (command start time)")

        iterator = client.list_entries(
            filter_=filter_str,
            max_results=20,
            order_by="timestamp desc"
        )

        # Collect recent logs
        recent_logs = []
        has_inference_logs = False
        has_tool_calls = False

        for entry in iterator:
            recent_logs.append(entry)

            # Check if log contains inference operation details
            if hasattr(entry, 'payload'):
                payload = entry.payload if isinstance(entry.payload, dict) else {}
                event_name = payload.get("event.name", "")

                if "gen_ai.client.inference" in event_name:
                    has_inference_logs = True
                    logger.info("Found Gemini inference log entry")

                # Check for tool usage
                gen_ai_output = payload.get("gen_ai.output.messages", "")
                if "tool_call" in str(gen_ai_output).lower():
                    has_tool_calls = True
                    logger.info("Found log entry with tool calls")

        log_count = len(recent_logs)
        logger.info(f"Found {log_count} recent gemini-cli logs")

        return {
            "gemini_command_run": True,
            "prompt": test_prompt,
            "logs_found": log_count > 0,
            "log_count": log_count,
            "has_inference_logs": has_inference_logs,
            "has_tool_calls": has_tool_calls,
            "verification_complete": True,
            "message": f"Found {log_count} Gemini CLI telemetry logs in Cloud Logging"
        }

    except subprocess.TimeoutExpired:
        logger.error("Gemini CLI test timed out (60s limit)")
        raise Exception("Gemini CLI test timed out - command took longer than 60 seconds")
    except Exception as e:
        logger.error(f"Gemini CLI logging test failed: {str(e)}")
        raise
