"""
Unit tests for Sink service.
Tests individual functions with mocked dependencies.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import sink_service


class TestCreateSink:
    """Test create_sink function"""

    @pytest.mark.asyncio
    async def test_create_sink_success(self, monkeypatch):
        """Test successful sink creation to Pub/Sub"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"

        # Mock list_sinks to return empty list
        async def mock_list_sinks(pid):
            return []

        # Mock get_sink_service_account
        async def mock_get_sa(pid, sname):
            return "serviceAccount:logging-sink@project.iam.gserviceaccount.com"

        # Mock grant_pubsub_publisher
        async def mock_grant(pid, tname, sa):
            pass

        monkeypatch.setattr(sink_service, 'list_sinks', mock_list_sinks)
        monkeypatch.setattr(sink_service, 'get_sink_service_account', mock_get_sa)
        monkeypatch.setattr(sink_service, 'grant_pubsub_publisher', mock_grant)

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.create_sink(project_id, topic_name)

            # Verify result structure
            assert "sink_name" in result
            assert "destination" in result
            assert "service_account" in result
            assert "topic_name" in result
            assert "filter" in result

            # Verify values
            assert result["sink_name"] == "gemini-cli-to-pubsub"
            assert f"pubsub.googleapis.com/projects/{project_id}/topics/{topic_name}" in result["destination"]
            assert result["topic_name"] == topic_name

    @pytest.mark.asyncio
    async def test_create_sink_deletes_existing(self, monkeypatch):
        """Test that existing sink is deleted before creating new one"""
        project_id = "test-project-123"

        delete_called = []

        async def mock_list_sinks(pid):
            return ["gemini-cli-to-pubsub"]

        async def mock_delete_sink(pid, sname):
            delete_called.append(sname)

        async def mock_get_sa(pid, sname):
            return "serviceAccount:test@project.iam.gserviceaccount.com"

        async def mock_grant(pid, tname, sa):
            pass

        monkeypatch.setattr(sink_service, 'list_sinks', mock_list_sinks)
        monkeypatch.setattr(sink_service, 'delete_sink', mock_delete_sink)
        monkeypatch.setattr(sink_service, 'get_sink_service_account', mock_get_sa)
        monkeypatch.setattr(sink_service, 'grant_pubsub_publisher', mock_grant)

        mock_result = Mock()
        mock_result.returncode = 0

        with patch('subprocess.run', return_value=mock_result):
            await sink_service.create_sink(project_id)

            # Verify delete was called
            assert "gemini-cli-to-pubsub" in delete_called

    @pytest.mark.asyncio
    async def test_create_sink_command_failure(self, monkeypatch):
        """Test handling of sink creation command failure"""
        project_id = "test-project-123"

        async def mock_list_sinks(pid):
            return []

        monkeypatch.setattr(sink_service, 'list_sinks', mock_list_sinks)

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Permission denied"

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.create_sink(project_id)

            assert "Failed to create sink" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_create_sink_no_service_account(self, monkeypatch):
        """Test handling when service account cannot be retrieved"""
        project_id = "test-project-123"

        async def mock_list_sinks(pid):
            return []

        async def mock_get_sa(pid, sname):
            return ""  # Empty service account

        monkeypatch.setattr(sink_service, 'list_sinks', mock_list_sinks)
        monkeypatch.setattr(sink_service, 'get_sink_service_account', mock_get_sa)

        mock_result = Mock()
        mock_result.returncode = 0

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.create_sink(project_id)

            assert "service account" in str(exc_info.value).lower()


class TestGetSinkServiceAccount:
    """Test get_sink_service_account function"""

    @pytest.mark.asyncio
    async def test_get_service_account_success(self):
        """Test successful retrieval of sink service account"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"
        expected_sa = "serviceAccount:logging-123@project.iam.gserviceaccount.com"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = expected_sa + "\n"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.get_sink_service_account(project_id, sink_name)

            assert result == expected_sa

    @pytest.mark.asyncio
    async def test_get_service_account_failure(self):
        """Test handling when service account retrieval fails"""
        project_id = "test-project-123"
        sink_name = "nonexistent-sink"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Sink not found"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.get_sink_service_account(project_id, sink_name)

            assert result == ""


class TestGrantPubSubPublisher:
    """Test grant_pubsub_publisher function"""

    @pytest.mark.asyncio
    async def test_grant_publisher_success(self):
        """Test successful granting of Pub/Sub Publisher role"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stderr = ""

        with patch('subprocess.run', return_value=mock_result):
            with patch('asyncio.sleep'):  # Skip IAM propagation wait in tests
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

                # Should not raise exception

    @pytest.mark.asyncio
    async def test_grant_publisher_already_exists(self):
        """Test handling when permission already exists"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Role already has member"

        with patch('subprocess.run', return_value=mock_result):
            with patch('asyncio.sleep'):
                # Should not raise exception for already exists
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

    @pytest.mark.asyncio
    async def test_grant_publisher_permission_denied(self):
        """Test handling of permission denied error"""
        project_id = "test-project-123"
        topic_name = "gemini-telemetry-topic"
        service_account = "logging-sa@project.iam.gserviceaccount.com"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Permission denied"

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.grant_pubsub_publisher(project_id, topic_name, service_account)

            assert "Permission denied" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_grant_publisher_strips_service_account_prefix(self):
        """Test that serviceAccount: prefix is stripped from SA email"""
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

                # Verify serviceAccount: prefix was removed
                assert "--member=serviceAccount:logging-sa@project.iam.gserviceaccount.com" in captured_command


class TestVerifySink:
    """Test verify_sink function"""

    @pytest.mark.asyncio
    async def test_verify_sink_success(self, monkeypatch):
        """Test successful sink verification"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        sink_info = {
            "destination": "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic",
            "writerIdentity": "serviceAccount:logging-sa@project.iam.gserviceaccount.com",
            "filter": 'logName="projects/test-project-123/logs/gemini_cli"'
        }

        async def mock_verify_permissions(pid, sa, dest):
            return True

        monkeypatch.setattr(sink_service, 'verify_service_account_permissions', mock_verify_permissions)

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = json.dumps(sink_info)

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.verify_sink(project_id, sink_name)

            assert result["verified"] is True
            assert result["destination_type"] == "pubsub"
            assert result["has_permissions"] is True

    @pytest.mark.asyncio
    async def test_verify_sink_bigquery_destination_fails(self, monkeypatch):
        """Test that BigQuery destination causes verification to fail"""
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

            assert "should be Pub/Sub" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_verify_sink_not_found(self):
        """Test handling when sink does not exist"""
        project_id = "test-project-123"
        sink_name = "nonexistent-sink"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Sink not found"

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.verify_sink(project_id, sink_name)

            assert "not found" in str(exc_info.value).lower()


class TestVerifyServiceAccountPermissions:
    """Test verify_service_account_permissions function"""

    @pytest.mark.asyncio
    async def test_verify_permissions_success(self):
        """Test successful permission verification"""
        project_id = "test-project-123"
        service_account = "serviceAccount:logging-sa@project.iam.gserviceaccount.com"
        destination = "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "roles/pubsub.publisher\n"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.verify_service_account_permissions(project_id, service_account, destination)

            assert result is True

    @pytest.mark.asyncio
    async def test_verify_permissions_missing_role(self):
        """Test when service account is missing publisher role"""
        project_id = "test-project-123"
        service_account = "logging-sa@project.iam.gserviceaccount.com"
        destination = "pubsub.googleapis.com/projects/test-project-123/topics/gemini-telemetry-topic"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "roles/viewer\n"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.verify_service_account_permissions(project_id, service_account, destination)

            assert result is False

    @pytest.mark.asyncio
    async def test_verify_permissions_not_pubsub_destination(self):
        """Test with non-Pub/Sub destination"""
        project_id = "test-project-123"
        service_account = "logging-sa@project.iam.gserviceaccount.com"
        destination = "bigquery.googleapis.com/projects/test-project-123/datasets/dataset"

        result = await sink_service.verify_service_account_permissions(project_id, service_account, destination)

        assert result is False


class TestListSinks:
    """Test list_sinks function"""

    @pytest.mark.asyncio
    async def test_list_sinks_success(self):
        """Test successful listing of sinks"""
        project_id = "test-project-123"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = "sink1\nsink2\nsink3\n"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.list_sinks(project_id)

            assert len(result) == 3
            assert "sink1" in result
            assert "sink2" in result
            assert "sink3" in result

    @pytest.mark.asyncio
    async def test_list_sinks_empty(self):
        """Test listing when no sinks exist"""
        project_id = "test-project-123"

        mock_result = Mock()
        mock_result.returncode = 0
        mock_result.stdout = ""

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.list_sinks(project_id)

            assert len(result) == 0

    @pytest.mark.asyncio
    async def test_list_sinks_failure(self):
        """Test handling of listing failure"""
        project_id = "test-project-123"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Permission denied"

        with patch('subprocess.run', return_value=mock_result):
            result = await sink_service.list_sinks(project_id)

            assert result == []


class TestDeleteSink:
    """Test delete_sink function"""

    @pytest.mark.asyncio
    async def test_delete_sink_success(self):
        """Test successful sink deletion"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        mock_result = Mock()
        mock_result.returncode = 0

        with patch('subprocess.run', return_value=mock_result):
            # Should not raise exception
            await sink_service.delete_sink(project_id, sink_name)

    @pytest.mark.asyncio
    async def test_delete_sink_not_found(self):
        """Test deleting sink that doesn't exist"""
        project_id = "test-project-123"
        sink_name = "nonexistent-sink"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Sink not found"

        with patch('subprocess.run', return_value=mock_result):
            # Should not raise exception for not found
            await sink_service.delete_sink(project_id, sink_name)

    @pytest.mark.asyncio
    async def test_delete_sink_permission_denied(self):
        """Test handling of permission denied error"""
        project_id = "test-project-123"
        sink_name = "gemini-cli-to-pubsub"

        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Permission denied"

        with patch('subprocess.run', return_value=mock_result):
            with pytest.raises(Exception) as exc_info:
                await sink_service.delete_sink(project_id, sink_name)

            assert "Failed to delete sink" in str(exc_info.value)
