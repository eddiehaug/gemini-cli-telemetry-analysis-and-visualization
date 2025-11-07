"""
Unit tests for Step 12 end-to-end verification.
Tests verify_end_to_end function and all helper functions.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import asyncio

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import verification_service


class TestSendTestPrompts:
    """Test _send_test_prompts function"""

    @pytest.mark.asyncio
    async def test_send_test_prompts_success(self, monkeypatch):
        """Test successful sending of test prompts"""
        test_id = "test-uuid-123"

        async def mock_run_commands():
            return 5

        monkeypatch.setattr(verification_service, 'run_multiple_gemini_test_commands', mock_run_commands)

        result = await verification_service._send_test_prompts(test_id)

        assert result["success"] is True
        assert result["prompts_sent"] == 5
        assert result["test_id"] == test_id

    @pytest.mark.asyncio
    async def test_send_test_prompts_no_prompts_sent(self, monkeypatch):
        """Test when no prompts are sent"""
        test_id = "test-uuid-123"

        async def mock_run_commands():
            return 0

        monkeypatch.setattr(verification_service, 'run_multiple_gemini_test_commands', mock_run_commands)

        result = await verification_service._send_test_prompts(test_id)

        assert result["success"] is False
        assert result["prompts_sent"] == 0

    @pytest.mark.asyncio
    async def test_send_test_prompts_exception(self, monkeypatch):
        """Test exception handling in _send_test_prompts"""
        test_id = "test-uuid-123"

        async def mock_run_commands_error():
            raise Exception("Gemini CLI not found")

        monkeypatch.setattr(verification_service, 'run_multiple_gemini_test_commands', mock_run_commands_error)

        result = await verification_service._send_test_prompts(test_id)

        assert result["success"] is False
        assert "Gemini CLI not found" in result["error"]


class TestVerifyCloudLogging:
    """Test _verify_cloud_logging function"""

    @pytest.mark.asyncio
    async def test_verify_cloud_logging_success(self, monkeypatch):
        """Test successful Cloud Logging verification"""
        project_id = "test-project"
        test_id = "test-uuid-123"

        async def mock_check_logs(pid):
            return {"found": True, "count": 10}

        monkeypatch.setattr(verification_service, 'check_logs_in_cloud_logging_detailed', mock_check_logs)

        # Mock sleep to speed up test
        monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

        result = await verification_service._verify_cloud_logging(project_id, test_id)

        assert result["success"] is True
        assert result["log_count"] == 10

    @pytest.mark.asyncio
    async def test_verify_cloud_logging_no_logs(self, monkeypatch):
        """Test when no logs are found"""
        project_id = "test-project"
        test_id = "test-uuid-123"

        async def mock_check_logs(pid):
            return {"found": False, "count": 0}

        monkeypatch.setattr(verification_service, 'check_logs_in_cloud_logging_detailed', mock_check_logs)
        monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

        result = await verification_service._verify_cloud_logging(project_id, test_id)

        assert result["success"] is False
        assert result["log_count"] == 0


class TestVerifyPubSubMessages:
    """Test _verify_pubsub_messages function"""

    @pytest.mark.asyncio
    async def test_verify_pubsub_messages_found(self, monkeypatch):
        """Test when messages are found in Pub/Sub"""
        project_id = "test-project"
        test_id = "test-uuid-123"

        # Mock Pub/Sub client
        mock_subscriber = MagicMock()
        mock_response = MagicMock()
        mock_response.received_messages = [MagicMock(ack_id="ack1"), MagicMock(ack_id="ack2")]
        mock_subscriber.pull.return_value = mock_response
        mock_subscriber.subscription_path.return_value = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"

        # Mock google.cloud.pubsub_v1
        with patch('google.cloud.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
            monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

            result = await verification_service._verify_pubsub_messages(project_id, test_id)

            assert result["success"] is True
            assert result["messages_found"] == 2

    @pytest.mark.asyncio
    async def test_verify_pubsub_no_messages(self, monkeypatch):
        """Test when no messages are found"""
        project_id = "test-project"
        test_id = "test-uuid-123"

        mock_subscriber = MagicMock()
        mock_response = MagicMock()
        mock_response.received_messages = []
        mock_subscriber.pull.return_value = mock_response
        mock_subscriber.subscription_path.return_value = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"

        with patch('google.cloud.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
            monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

            result = await verification_service._verify_pubsub_messages(project_id, test_id)

            assert result["success"] is False
            assert result["messages_found"] == 0


class TestVerifyDataflowProcessing:
    """Test _verify_dataflow_processing function"""

    @pytest.mark.asyncio
    async def test_verify_dataflow_running(self, monkeypatch):
        """Test when Dataflow job is running"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        async def mock_verify_dataflow(pid, dataset, reg):
            return {
                "is_running": True,
                "job_id": "job-123",
                "state": "JOB_STATE_RUNNING"
            }

        from services import dataflow_service
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)

        result = await verification_service._verify_dataflow_processing(project_id, dataset_name, region)

        assert result["success"] is True
        assert result["job_id"] == "job-123"
        assert result["state"] == "JOB_STATE_RUNNING"

    @pytest.mark.asyncio
    async def test_verify_dataflow_not_running(self, monkeypatch):
        """Test when Dataflow job is not running"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        async def mock_verify_dataflow(pid, dataset, reg):
            return {
                "is_running": False,
                "job_id": None,
                "state": "JOB_STATE_FAILED"
            }

        from services import dataflow_service
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)

        result = await verification_service._verify_dataflow_processing(project_id, dataset_name, region)

        assert result["success"] is False
        assert result["state"] == "JOB_STATE_FAILED"


class TestWaitForBigQueryData:
    """Test _wait_for_bigquery_data function"""

    @pytest.mark.asyncio
    async def test_wait_for_bigquery_data_found_immediately(self, monkeypatch):
        """Test when data is found on first check"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        async def mock_check_table(pid, dataset):
            return {"has_data": True, "row_count": 5}

        monkeypatch.setattr(verification_service, '_check_bigquery_raw_table', mock_check_table)

        result = await verification_service._wait_for_bigquery_data(project_id, dataset_name, test_id, 60)

        assert result["success"] is True
        assert result["row_count"] == 5

    @pytest.mark.asyncio
    async def test_wait_for_bigquery_data_timeout(self, monkeypatch):
        """Test timeout when no data appears"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        async def mock_check_table(pid, dataset):
            return {"has_data": False, "row_count": 0}

        monkeypatch.setattr(verification_service, '_check_bigquery_raw_table', mock_check_table)
        monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

        result = await verification_service._wait_for_bigquery_data(project_id, dataset_name, test_id, 1)

        assert result["success"] is False
        assert "Timeout" in result["error"]


class TestVerifyJsonStringSchema:
    """Test _verify_json_string_schema function"""

    @pytest.mark.asyncio
    async def test_verify_json_schema_valid(self, monkeypatch):
        """Test valid JSON string schema"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.resource_json = '{"type": "global"}'
        mock_row.labels_json = '{"key": "value"}'
        mock_row.jsonPayload_json = '{"event": "test"}'

        mock_results = [mock_row]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._verify_json_string_schema(project_id, dataset_name, test_id)

            assert result["success"] is True
            assert result["valid_schema"] is True
            assert result["json_parseable"] is True

    @pytest.mark.asyncio
    async def test_verify_json_schema_invalid_json(self, monkeypatch):
        """Test invalid JSON in schema"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.resource_json = 'not valid json {'
        mock_row.labels_json = '{"key": "value"}'
        mock_row.jsonPayload_json = '{"event": "test"}'

        mock_results = [mock_row]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._verify_json_string_schema(project_id, dataset_name, test_id)

            assert result["success"] is False
            assert result["json_parseable"] is False


class TestQueryAnalyticsView:
    """Test _query_analytics_view function"""

    @pytest.mark.asyncio
    async def test_query_analytics_view_success(self, monkeypatch):
        """Test successful analytics view query"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.timestamp = "2025-01-01"
        mock_row.event_name = "gemini_cli.api_response"
        mock_row.resource = {"type": "global"}
        mock_row.labels = {"key": "value"}
        mock_row.payload = {"event": "test"}

        mock_results = [mock_row]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._query_analytics_view(project_id, dataset_name, test_id)

            assert result["success"] is True
            assert result["row_count"] == 1
            assert result["fields_extracted"] is True

    @pytest.mark.asyncio
    async def test_query_analytics_view_no_data(self, monkeypatch):
        """Test when analytics view returns no data"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        mock_client = MagicMock()
        mock_job = MagicMock()
        mock_job.result.return_value = []
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._query_analytics_view(project_id, dataset_name, test_id)

            assert result["success"] is False
            assert result["row_count"] == 0


class TestVerifyEndToEnd:
    """Test verify_end_to_end main function"""

    @pytest.mark.asyncio
    async def test_verify_end_to_end_all_steps_pass(self, monkeypatch):
        """Test when all verification steps pass"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Mock all helper functions to return success
        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging(pid, tid, timeout):
            return {"success": True, "log_count": 10}

        async def mock_pubsub(pid, tid, timeout):
            return {"success": True, "messages_found": 5}

        async def mock_dataflow(pid, dataset, reg):
            return {"success": True, "job_id": "job-123"}

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            return {"success": True, "row_count": 5}

        async def mock_schema(pid, dataset, tid):
            return {"success": True, "valid_schema": True}

        async def mock_analytics(pid, dataset, tid):
            return {"success": True, "row_count": 5}

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        assert result["success"] is True
        assert "test_id" in result
        assert len(result["steps"]) == 7
        assert result["pipeline_flow"] == "Gemini CLI → Cloud Logging → Pub/Sub → Dataflow → BigQuery"

    @pytest.mark.asyncio
    async def test_verify_end_to_end_one_step_fails(self, monkeypatch):
        """Test when one verification step fails"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Mock most functions to succeed, but dataflow to fail
        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging(pid, tid, timeout):
            return {"success": True, "log_count": 10}

        async def mock_pubsub(pid, tid, timeout):
            return {"success": True, "messages_found": 5}

        async def mock_dataflow_fail(pid, dataset, reg):
            return {"success": False, "error": "Job not running"}

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            return {"success": True, "row_count": 5}

        async def mock_schema(pid, dataset, tid):
            return {"success": True, "valid_schema": True}

        async def mock_analytics(pid, dataset, tid):
            return {"success": True, "row_count": 5}

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow_fail)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        assert result["success"] is False
        assert "dataflow" in result["steps"]
        assert result["steps"]["dataflow"]["success"] is False
