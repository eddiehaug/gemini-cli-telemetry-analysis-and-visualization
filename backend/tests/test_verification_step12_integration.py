"""
Integration tests for Step 12 end-to-end verification.
Tests complete workflows with mocked service dependencies.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import asyncio

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import verification_service


class TestEndToEndWorkflow:
    """Integration tests for complete E2E workflow"""

    @pytest.mark.asyncio
    async def test_complete_elt_pipeline_verification_workflow(self, monkeypatch):
        """Test complete successful ELT pipeline verification"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        workflow_steps = []

        # Track workflow execution order
        async def mock_send_prompts(tid):
            workflow_steps.append("send_prompts")
            return {"success": True, "prompts_sent": 5, "test_id": tid}

        async def mock_cloud_logging(pid, tid, timeout):
            workflow_steps.append("cloud_logging")
            return {"success": True, "log_count": 10, "test_id": tid}

        async def mock_pubsub(pid, tid, timeout):
            workflow_steps.append("pubsub")
            return {"success": True, "messages_found": 5, "test_id": tid}

        async def mock_dataflow(pid, dataset, reg):
            workflow_steps.append("dataflow")
            return {"success": True, "job_id": "job-abc123", "state": "JOB_STATE_RUNNING"}

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            workflow_steps.append("wait_bigquery")
            return {"success": True, "row_count": 8, "elapsed_seconds": 120}

        async def mock_schema(pid, dataset, tid):
            workflow_steps.append("schema_validation")
            return {"success": True, "valid_schema": True, "json_parseable": True}

        async def mock_analytics(pid, dataset, tid):
            workflow_steps.append("analytics_view")
            return {"success": True, "row_count": 8, "fields_extracted": True}

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        # Verify workflow executed in correct order
        assert workflow_steps == [
            "send_prompts",
            "cloud_logging",
            "pubsub",
            "dataflow",
            "wait_bigquery",
            "schema_validation",
            "analytics_view"
        ]

        # Verify overall result
        assert result["success"] is True
        assert "test_id" in result
        assert len(result["steps"]) == 7

        # Verify each step result
        assert result["steps"]["send_prompts"]["success"] is True
        assert result["steps"]["cloud_logging"]["success"] is True
        assert result["steps"]["pubsub"]["success"] is True
        assert result["steps"]["dataflow"]["success"] is True
        assert result["steps"]["bigquery_raw"]["success"] is True
        assert result["steps"]["schema_validation"]["success"] is True
        assert result["steps"]["analytics_view"]["success"] is True

    @pytest.mark.asyncio
    async def test_workflow_stops_gracefully_on_early_failure(self, monkeypatch):
        """Test that workflow continues even if early steps fail"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Cloud logging fails, but workflow continues
        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging_fail(pid, tid, timeout):
            return {"success": False, "log_count": 0, "error": "No logs found"}

        async def mock_pubsub(pid, tid, timeout):
            return {"success": False, "messages_found": 0}

        async def mock_dataflow(pid, dataset, reg):
            return {"success": True, "job_id": "job-123"}

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            return {"success": False, "row_count": 0, "error": "Timeout"}

        async def mock_schema(pid, dataset, tid):
            return {"success": False, "error": "No data"}

        async def mock_analytics(pid, dataset, tid):
            return {"success": False, "row_count": 0}

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging_fail)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        # Overall verification should fail
        assert result["success"] is False

        # But all steps should have been executed
        assert len(result["steps"]) == 7


class TestPubSubVerificationWorkflow:
    """Integration tests for Pub/Sub message verification"""

    @pytest.mark.asyncio
    async def test_pubsub_pull_and_acknowledge_workflow(self, monkeypatch):
        """Test complete Pub/Sub pull and acknowledgment workflow"""
        project_id = "test-project"
        test_id = "test-uuid-123"

        # Create mock messages
        mock_msg1 = MagicMock()
        mock_msg1.ack_id = "ack-id-1"
        mock_msg1.message.data = b'{"event": "test"}'

        mock_msg2 = MagicMock()
        mock_msg2.ack_id = "ack-id-2"
        mock_msg2.message.data = b'{"event": "test2"}'

        # Mock subscriber client
        mock_subscriber = MagicMock()
        mock_response = MagicMock()
        mock_response.received_messages = [mock_msg1, mock_msg2]
        mock_subscriber.pull.return_value = mock_response
        mock_subscriber.subscription_path.return_value = f"projects/{project_id}/subscriptions/gemini-telemetry-sub"

        # Track acknowledgment
        ack_called = False

        def mock_acknowledge(request):
            nonlocal ack_called
            ack_called = True
            assert len(request["ack_ids"]) == 2
            assert "ack-id-1" in request["ack_ids"]
            assert "ack-id-2" in request["ack_ids"]

        mock_subscriber.acknowledge = mock_acknowledge

        with patch('google.cloud.pubsub_v1.SubscriberClient', return_value=mock_subscriber):
            monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

            result = await verification_service._verify_pubsub_messages(project_id, test_id, timeout=180)

            assert result["success"] is True
            assert result["messages_found"] == 2
            assert ack_called is True


class TestBigQueryVerificationWorkflow:
    """Integration tests for BigQuery data verification"""

    @pytest.mark.asyncio
    async def test_bigquery_polling_finds_data_on_second_attempt(self, monkeypatch):
        """Test BigQuery polling retries until data is found"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        attempt_count = 0

        async def mock_check_table(pid, dataset):
            nonlocal attempt_count
            attempt_count += 1

            # Return no data on first attempt, data on second
            if attempt_count == 1:
                return {"has_data": False, "row_count": 0}
            else:
                return {"has_data": True, "row_count": 10}

        monkeypatch.setattr(verification_service, '_check_bigquery_raw_table', mock_check_table)
        monkeypatch.setattr(asyncio, 'sleep', AsyncMock())

        result = await verification_service._wait_for_bigquery_data(
            project_id, dataset_name, test_id, max_wait_seconds=120
        )

        assert result["success"] is True
        assert result["row_count"] == 10
        assert attempt_count == 2


class TestSchemaValidationWorkflow:
    """Integration tests for JSON schema validation"""

    @pytest.mark.asyncio
    async def test_schema_validation_with_real_json_parsing(self, monkeypatch):
        """Test schema validation with actual JSON parsing"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        # Create mock BigQuery row with JSON strings
        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.resource_json = '{"type":"global","labels":{"project_id":"test"}}'
        mock_row.labels_json = '{"user_email":"test@example.com"}'
        mock_row.jsonPayload_json = '{"event":{"name":"gemini_cli.api_response"},"gen_ai":{"request":{"model":"gemini-2.5-flash"}}}'
        mock_row.operation_json = None
        mock_row.httpRequest_json = None

        mock_results = [mock_row]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._verify_json_string_schema(project_id, dataset_name, test_id)

            assert result["success"] is True
            assert result["valid_schema"] is True
            assert result["all_fields_are_strings"] is True
            assert result["json_parseable"] is True


class TestAnalyticsViewWorkflow:
    """Integration tests for analytics view querying"""

    @pytest.mark.asyncio
    async def test_analytics_view_extracts_fields_correctly(self, monkeypatch):
        """Test that analytics view correctly extracts fields from JSON"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        # Create mock rows from analytics view
        mock_client = MagicMock()

        mock_row1 = MagicMock()
        mock_row1.timestamp = "2025-01-01T10:00:00"
        mock_row1.event_name = "gemini_cli.api_response"
        mock_row1.session_id = "session-123"
        mock_row1.model = "gemini-2.5-flash"
        mock_row1.input_tokens = 100
        mock_row1.output_tokens = 50
        mock_row1.total_tokens = 150
        mock_row1.resource = {"type": "global"}
        mock_row1.labels = {"user_email": "test@example.com"}
        mock_row1.payload = {"event": {"name": "gemini_cli.api_response"}}

        mock_row2 = MagicMock()
        mock_row2.timestamp = "2025-01-01T10:01:00"
        mock_row2.event_name = "gemini_cli.tool_call"
        mock_row2.session_id = "session-123"
        mock_row2.model = "gemini-2.5-flash"
        mock_row2.input_tokens = 120
        mock_row2.output_tokens = 80
        mock_row2.total_tokens = 200
        mock_row2.resource = {"type": "global"}
        mock_row2.labels = {"user_email": "test@example.com"}
        mock_row2.payload = {"function": {"name": "search"}}

        mock_results = [mock_row1, mock_row2]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._query_analytics_view(project_id, dataset_name, test_id)

            assert result["success"] is True
            assert result["row_count"] == 2
            assert result["fields_extracted"] is True


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_exception_in_middle_step_continues_workflow(self, monkeypatch):
        """Test that exception in one step doesn't crash entire workflow"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging(pid, tid, timeout):
            return {"success": True, "log_count": 10}

        async def mock_pubsub_exception(pid, tid, timeout):
            # This step raises an exception
            raise Exception("Pub/Sub API error")

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
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub_exception)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        # Should not raise exception, but workflow should fail
        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        assert result["success"] is False
        # Pub/Sub step should have error
        assert "error" in result["steps"]["pubsub"]


class TestRegionalSupport:
    """Integration tests for regional configuration"""

    @pytest.mark.asyncio
    async def test_region_passed_to_dataflow_verification(self, monkeypatch):
        """Test that region parameter is correctly passed to Dataflow verification"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_region = "europe-west1"

        dataflow_region_received = None

        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging(pid, tid, timeout):
            return {"success": True, "log_count": 10}

        async def mock_pubsub(pid, tid, timeout):
            return {"success": True, "messages_found": 5}

        async def mock_dataflow(pid, dataset, reg):
            nonlocal dataflow_region_received
            dataflow_region_received = reg
            return {"success": True, "job_id": "job-123", "state": "JOB_STATE_RUNNING"}

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

        result = await verification_service.verify_end_to_end(project_id, dataset_name, test_region)

        assert dataflow_region_received == test_region
        assert result["success"] is True
