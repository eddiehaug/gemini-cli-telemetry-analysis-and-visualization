"""
Integration tests for Sink service.
Tests complete workflows with mocked subprocess commands.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import sink_service


class TestSinkCreationWorkflow:
    """Integration tests for sink creation workflow"""

    @pytest.mark.asyncio
    async def test_complete_sink_creation_workflow(self):
        """Test complete workflow from creation to permission granting"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "sinks" in command and "list" in command:
                workflow_steps.append("list_sinks")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "sinks" in command and "create" in command:
                workflow_steps.append("create_sink")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result
            elif "sinks" in command and "describe" in command:
                workflow_steps.append("get_service_account")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
                return mock_result
            elif "pubsub" in command and "add-iam-policy-binding" in command:
                workflow_steps.append("grant_publisher")
                mock_result = Mock()
                mock_result.returncode = 0
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):  # Skip IAM propagation wait
                result = await sink_service.create_sink(project_id, topic_name)

                # Verify all workflow steps executed
                assert "list_sinks" in workflow_steps
                assert "create_sink" in workflow_steps
                assert "get_service_account" in workflow_steps
                assert "grant_publisher" in workflow_steps

                # Verify result
                assert result["sink_name"] == "gemini-cli-to-pubsub"
                assert result["topic_name"] == topic_name

    @pytest.mark.asyncio
    async def test_sink_creation_builds_correct_command(self):
        """Test that sink creation builds correct gcloud command"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            command = args[0]
            if "sinks" in command and "create" in command:
                captured_command = command
            elif "sinks" in command and "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "describe" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "serviceAccount:test@project.iam.gserviceaccount.com"
                return mock_result
            elif "pubsub" in command:
                pass

            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            with patch('asyncio.sleep'):
                await sink_service.create_sink(project_id, topic_name)

                # Verify command structure
                assert "gcloud" in captured_command
                assert "logging" in captured_command
                assert "sinks" in captured_command
                assert "create" in captured_command
                assert "gemini-cli-to-pubsub" in captured_command
                assert f"pubsub.googleapis.com/projects/{project_id}/topics/{topic_name}" in captured_command
                # Verify NO BigQuery-specific flag
                assert "--use-partitioned-tables" not in captured_command


class TestPublisherPermissionGrantingWorkflow:
    """Integration tests for Pub/Sub Publisher permission granting"""

    @pytest.mark.asyncio
    async def test_complete_permission_granting_workflow(self):
        """Test complete workflow for granting publisher permissions"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("grant_permission")
            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with patch('asyncio.sleep'):
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

                assert "grant_permission" in workflow_steps

    @pytest.mark.asyncio
    async def test_permission_granting_builds_correct_command(self):
        """Test that permission granting builds correct gcloud command"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"

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

                # Verify command structure
                assert "gcloud" in captured_command
                assert "pubsub" in captured_command
                assert "topics" in captured_command
                assert "add-iam-policy-binding" in captured_command
                assert topic_name in captured_command
                assert "--member=serviceAccount:logging-sa@project.iam.gserviceaccount.com" in captured_command
                assert "--role=roles/pubsub.publisher" in captured_command


class TestSinkVerificationWorkflow:
    """Integration tests for sink verification workflow"""

    @pytest.mark.asyncio
    async def test_complete_verification_workflow(self):
        """Test complete sink verification workflow"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        workflow_steps = []

        sink_info = {
            "destination": "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic",
            "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
            "filter": 'logName="projects/test-project-123/logs/gemini_cli"'
        }

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "describe" in command:
                workflow_steps.append("describe_sink")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = json.dumps(sink_info)
                return mock_result
            elif "get-iam-policy" in command:
                workflow_steps.append("check_permissions")
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = "roles/pubsub.publisher\n"
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await sink_service.verify_sink(project_id, sink_name)

            # Verify workflow steps
            assert "describe_sink" in workflow_steps
            assert "check_permissions" in workflow_steps

            # Verify result
            assert result["verified"] is True
            assert result["destination_type"] == "pubsub"

    @pytest.mark.asyncio
    async def test_verification_detects_bigquery_destination(self):
        """Test that verification detects and rejects BigQuery destination"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-bigquery"

        sink_info = {
            "destination": "bigquery.googleapis.com/projects/test-project-123/datasets/dataset",
            "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
            "filter": 'logName="projects/test-project-123/logs/gemini_cli"'
        }

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(sink_info)

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.verify_sink(project_id, sink_name)

            assert "Pub/Sub" in str(exc_info.value)


class TestPermissionVerificationWorkflow:
    """Integration tests for permission verification workflow"""

    @pytest.mark.asyncio
    async def test_complete_permission_verification_workflow(self):
        """Test complete permission verification workflow"""
        project_id = "test-project-123"
        service_account = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
        destination = "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("check_topic_iam")
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "roles/pubsub.publisher\n"
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await sink_service.verify_service_account_permissions(project_id, service_account, destination)

            assert "check_topic_iam" in workflow_steps
            assert result is True

    @pytest.mark.asyncio
    async def test_permission_verification_builds_correct_command(self):
        """Test that permission verification builds correct gcloud command"""
        project_id = "test-project-123"
        service_account = "logging-sa@project.iam.gserviceaccount.com"
        destination = "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "roles/pubsub.publisher\n"
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await sink_service.verify_service_account_permissions(project_id, service_account, destination)

            # Verify command structure
            assert "gcloud" in captured_command
            assert "pubsub" in captured_command
            assert "topics" in captured_command
            assert "get-iam-policy" in captured_command
            assert "gemini-telemetry-topic" in captured_command


class TestSinkDeletionWorkflow:
    """Integration tests for sink deletion workflow"""

    @pytest.mark.asyncio
    async def test_complete_deletion_workflow(self):
        """Test complete sink deletion workflow"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("delete_sink")
            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            await sink_service.delete_sink(project_id, sink_name)

            assert "delete_sink" in workflow_steps

    @pytest.mark.asyncio
    async def test_deletion_builds_correct_command(self):
        """Test that deletion builds correct gcloud command"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await sink_service.delete_sink(project_id, sink_name)

            assert "gcloud" in captured_command
            assert "logging" in captured_command
            assert "sinks" in captured_command
            assert "delete" in captured_command
            assert sink_name in captured_command
            assert "--quiet" in captured_command


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_sink_creation_permission_denied(self):
        """Test handling of permission denied error during sink creation"""
        project_id = "test-project-123"

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "list" in command:
                mock_result = Mock()
                mock_result.returncode = 0
                mock_result.stdout = ""
                return mock_result
            elif "create" in command:
                mock_error = Mock()
                mock_error.returncode = 1
                mock_error.stderr = "Permission denied"
                return mock_error

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            with pytest.raises(Exception) as exc_info:
                await sink_service.create_sink(project_id)

            assert "Failed to create sink" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_permission_granting_api_error(self):
        """Test handling of API error during permission granting"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Topic not found"

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

            assert "Topic not found" in str(exc_info.value)


class TestListSinksWorkflow:
    """Integration tests for listing sinks workflow"""

    @pytest.mark.asyncio
    async def test_complete_listing_workflow(self):
        """Test complete sink listing workflow"""
        project_id = "test-project-123"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("list_sinks")
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = "sink1\nsink2\nsink3\n"
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await sink_service.list_sinks(project_id)

            assert "list_sinks" in workflow_steps
            assert len(result) == 3

    @pytest.mark.asyncio
    async def test_listing_builds_correct_command(self):
        """Test that listing builds correct gcloud command"""
        project_id = "test-project-123"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = ""
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await sink_service.list_sinks(project_id)

            assert "gcloud" in captured_command
            assert "logging" in captured_command
            assert "sinks" in captured_command
            assert "list" in captured_command
            # project_id is in --project=test-project-123
            assert any(project_id in arg for arg in captured_command)


class TestExclusionFilterHandling:
    """Integration tests for exclusion filter handling"""

    @pytest.mark.asyncio
    async def test_sink_creation_includes_exclusion_filter(self):
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
            elif "pubsub" in command:
                pass

            mock_result = Mock()
            mock_result.returncode = 0
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            with patch('asyncio.sleep'):
                await sink_service.create_sink(project_id)

                # Verify exclusion filter is present
                command_str = " ".join(captured_command)
                assert "--exclusion=" in command_str
                assert "exclude-diagnostic-logs" in command_str
                assert "logging.googleapis.com/diagnostic" in command_str
