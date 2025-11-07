"""
Functional tests for Sink service.
Tests end-to-end workflows and ELT pipeline readiness.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import sink_service


class TestSinkAPIEndToEnd:
    """Functional test for Sink API endpoint"""

    @pytest.mark.asyncio
    async def test_create_sink_api_endpoint_simulation(self):
        """Simulate FastAPI endpoint call to create sink"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "create" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
                return mock_result
            elif "add-iam-policy-binding" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                # Simulate API endpoint call
                result = await sink_service.create_sink(
                    project_id=project_id,
                    topic_name=topic_name
                )

                # Verify API response structure
                assert "sink_name" in result
                assert "destination" in result
                assert "service_account" in result
                assert "topic_name" in result
                assert "filter" in result

                # Verify API response values
                assert result["sink_name"] == "gemini-cli-to-pubsub"
                assert result["topic_name"] == topic_name


class TestEndToEndWorkflow:
    """Functional tests for complete end-to-end workflows"""

    @pytest.mark.asyncio
    async def test_complete_sink_deployment_workflow(self):
        """Test complete workflow from creation to verification"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        # Track all steps executed
        workflow_log = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                workflow_log.append("list_sinks")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "create" in command:
                workflow_log.append("create_sink")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "describe" in command:
                workflow_log.append("describe_sink")
                mock_result = Mock()
                mock_result.returncode = 0
                if "value(writerIdentity)" in " ".join(command):
                    mock_result.stdout = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
                else:
                    sink_info = {
                        "destination": f"pubsub.googleapis.com/projects/{project_id}/topics/{topic_name}",
                        "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
                        "filter": f'logName="projects/{project_id}/logs/gemini_cli"'
                    }
                    mock_result.stdout = json.dumps(sink_info)
                return mock_result
            elif "add-iam-policy-binding" in command:
                workflow_log.append("grant_publisher")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "get-iam-policy" in command:
                workflow_log.append("verify_permissions")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "roles/pubsub.publisher\n"
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                # Step 1: Create sink
                create_result = await sink_service.create_sink(project_id, topic_name)

                # Step 2: Verify sink
                verify_result = await sink_service.verify_sink(project_id, "gemini-cli-to-pubsub")

                # Verify complete workflow
                assert "list_sinks" in workflow_log
                assert "create_sink" in workflow_log
                assert "grant_publisher" in workflow_log
                assert "verify_permissions" in workflow_log
                assert verify_result["verified"] is True

    @pytest.mark.asyncio
    async def test_sink_lifecycle_management(self):
        """Test complete sink lifecycle: create -> verify -> delete"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        workflow_log = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                workflow_log.append("listed")
                mock_result = Mock()
                mock_result.returncode = 0
                # First list: empty, second list: has sink
                if workflow_log.count("listed") > 1:
                    mock_result.stdout = "gemini-cli-to-pubsub\n"
                else:
                    mock_result.stdout = ""
                return mock_result
            elif "create" in command:
                workflow_log.append("created")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "describe" in command:
                workflow_log.append("described")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
                return mock_result
            elif "add-iam-policy-binding" in command:
                workflow_log.append("granted")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "delete" in command:
                workflow_log.append("deleted")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                # Create sink
                create_result = await sink_service.create_sink(project_id, topic_name)

                # List sinks
                sinks = await sink_service.list_sinks(project_id)

                # Delete sink
                await sink_service.delete_sink(project_id, "gemini-cli-to-pubsub")

                # Verify complete lifecycle
                assert "created" in workflow_log
                assert "granted" in workflow_log
                assert "deleted" in workflow_log
                assert len(sinks) > 0


class TestELTPipelineReadiness:
    """Functional tests for ELT pipeline readiness"""

    @pytest.mark.asyncio
    async def test_sink_destination_matches_elt_architecture(self):
        """Test that sink destination points to Pub/Sub topic"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            else:
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                result = await sink_service.create_sink(project_id, topic_name)

                # Verify ELT destination
                assert "pubsub.googleapis.com" in result["destination"]
                assert f"/topics/{topic_name}" in result["destination"]
                assert "bigquery.googleapis.com" not in result["destination"]

    @pytest.mark.asyncio
    async def test_sink_grants_publisher_not_bigquery_role(self):
        """Test that sink grants Pub/Sub Publisher role, not BigQuery role"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            with patch('asyncio.sleep'):
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

                # Verify Pub/Sub Publisher role, NOT BigQuery role
                command_str = " ".join(captured_command)
                assert "roles/pubsub.publisher" in command_str
                assert "roles/bigquery" not in command_str
                assert "pubsub" in command_str
                assert "bigquery" not in command_str

    @pytest.mark.asyncio
    async def test_sink_does_not_use_bigquery_specific_flags(self):
        """Test that sink creation does NOT use BigQuery-specific flags"""
        project_id = "test-project-123"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            command = args[0]
            if "create" in command:
                captured_command = command
            elif "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            elif "add-iam-policy-binding" in command:
                pass

            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            with patch('asyncio.sleep'):
                await sink_service.create_sink(project_id)

                # Verify NO BigQuery-specific flags
                assert "--use-partitioned-tables" not in captured_command

    @pytest.mark.asyncio
    async def test_sink_connects_to_dataflow_input(self):
        """Test that sink publishes to topic consumed by Dataflow"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            else:
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                result = await sink_service.create_sink(project_id, topic_name)

                # Verify sink publishes to the Dataflow input topic
                assert result["topic_name"] == "gemini-telemetry-topic"
                assert "/topics/gemini-telemetry-topic" in result["destination"]


class TestSinkNaming:
    """Functional tests for sink naming conventions"""

    @pytest.mark.asyncio
    async def test_sink_name_reflects_pubsub_destination(self):
        """Test that sink name reflects Pub/Sub destination"""
        project_id = "test-project-123"

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            else:
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                result = await sink_service.create_sink(project_id)

                # Verify sink name
                assert result["sink_name"] == "gemini-cli-to-pubsub"
                assert "bigquery" not in result["sink_name"].lower()


class TestIAMPropagation:
    """Functional tests for IAM propagation handling"""

    @pytest.mark.asyncio
    async def test_permission_granting_waits_for_iam_propagation(self):
        """Test that permission granting waits for IAM propagation"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        sleep_called = []

        async def mock_sleep(seconds):
            sleep_called.append(seconds)

        mock_result = Mock()
        mock_result.returncode = 0

        with patch('subprocess.run', return_value=mock_result):
            with patch('asyncio.sleep', side_effect=mock_sleep):
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

                # Verify IAM propagation wait
                assert 90 in sleep_called

    @pytest.mark.asyncio
    async def test_permission_granting_waits_even_if_already_exists(self):
        """Test that IAM wait happens even if permission already exists"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        sleep_called = []

        async def mock_sleep(seconds):
            sleep_called.append(seconds)

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Role already has member"

        with patch('subprocess.run', return_value=mock_result):
            with patch('asyncio.sleep', side_effect=mock_sleep):
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

                # Verify IAM propagation wait still happens
                assert 90 in sleep_called


class TestSinkVerificationChecks:
    """Functional tests for sink verification checks"""

    @pytest.mark.asyncio
    async def test_verification_rejects_non_pubsub_sinks(self):
        """Test that verification rejects sinks with non-Pub/Sub destinations"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-bigquery"

        destinations_to_test = [
            "bigquery.googleapis.com/projects/test-project-123/datasets/dataset",
            "storage.googleapis.com/test-bucket",
            "logging.googleapis.com/projects/test-project-123/locations/global/buckets/log-bucket"
        ]

        for destination in destinations_to_test:
            sink_info = {
                "destination": destination,
                "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
                "filter": 'logName="projects/test-project-123/logs/gemini_cli"'
            }

            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = json.dumps(sink_info)

            with patch('subprocess.run', return_value=mock_result):
                with pytest.raises(Exception) as exc_info:
                    await sink_service.verify_sink(project_id, sink_name)

                assert "Pub/Sub" in str(exc_info.value) or "pubsub" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_verification_checks_publisher_role(self):
        """Test that verification checks for Pub/Sub Publisher role"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        sink_info = {
            "destination": "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic",
            "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
            "filter": 'logName="projects/test-project-123/logs/gemini_cli"'
        }

        captured_commands = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            captured_commands.append(command)
            if "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = json.dumps(sink_info)
                return mock_result
            elif "get-iam-policy" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "roles/pubsub.publisher\n"
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await sink_service.verify_sink(project_id, sink_name)

            # Verify that IAM policy check was performed
            iam_check_found = False
            for command in captured_commands:
                if "get-iam-policy" in command:
                    iam_check_found = True
                    break

            assert iam_check_found
            assert result["has_permissions"] is True


class TestExclusionFilter:
    """Functional tests for diagnostic log exclusion filter"""

    @pytest.mark.asyncio
    async def test_sink_includes_diagnostic_exclusion_filter(self):
        """Test that sink creation includes diagnostic log exclusion"""
        project_id = "test-project-123"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            command = args[0]
            if "create" in command:
                captured_command = command
            elif "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            elif "add-iam-policy-binding" in command:
                pass

            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            with patch('asyncio.sleep'):
                await sink_service.create_sink(project_id)

                # Verify exclusion filter components
                command_str = " ".join(captured_command)
                assert "--exclusion=" in command_str
                assert "exclude-diagnostic-logs" in command_str
                assert "logging.googleapis.com/diagnostic" in command_str
