"""
Functional tests for Step 12 end-to-end verification.
Tests API endpoints and complete end-to-end scenarios.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app
from services import verification_service


class TestEndToEndAPIEndpoint:
    """Functional tests for /api/verify-end-to-end endpoint"""

    def test_verify_end_to_end_api_success(self, monkeypatch):
        """Test successful E2E verification via API"""

        async def mock_verify_e2e(pid, dataset, region):
            return {
                "success": True,
                "test_id": "test-uuid-123",
                "steps": {
                    "send_prompts": {"success": True, "prompts_sent": 5},
                    "cloud_logging": {"success": True, "log_count": 10},
                    "pubsub": {"success": True, "messages_found": 5},
                    "dataflow": {"success": True, "job_id": "job-123"},
                    "bigquery_raw": {"success": True, "row_count": 8},
                    "schema_validation": {"success": True, "valid_schema": True},
                    "analytics_view": {"success": True, "row_count": 8}
                },
                "pipeline_flow": "Gemini CLI → Cloud Logging → Pub/Sub → Dataflow → BigQuery"
            }

        monkeypatch.setattr(verification_service, 'verify_end_to_end', mock_verify_e2e)

        client = TestClient(app)
        response = client.post(
            "/api/verify-end-to-end",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert data["data"]["success"] is True
        assert "test_id" in data["data"]
        assert len(data["data"]["steps"]) == 7

    def test_verify_end_to_end_api_partial_failure(self, monkeypatch):
        """Test API response when some steps fail"""

        async def mock_verify_e2e(pid, dataset, region):
            return {
                "success": False,
                "test_id": "test-uuid-456",
                "steps": {
                    "send_prompts": {"success": True, "prompts_sent": 5},
                    "cloud_logging": {"success": True, "log_count": 10},
                    "pubsub": {"success": False, "messages_found": 0, "error": "No messages"},
                    "dataflow": {"success": False, "error": "Job not running"},
                    "bigquery_raw": {"success": False, "row_count": 0, "error": "Timeout"},
                    "schema_validation": {"success": False, "error": "No data"},
                    "analytics_view": {"success": False, "row_count": 0}
                },
                "pipeline_flow": "Gemini CLI → Cloud Logging → Pub/Sub → Dataflow → BigQuery"
            }

        monkeypatch.setattr(verification_service, 'verify_end_to_end', mock_verify_e2e)

        client = TestClient(app)
        response = client.post(
            "/api/verify-end-to-end",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        # API should return success=False when E2E fails
        assert data["success"] is False
        assert data["data"]["success"] is False

    def test_verify_end_to_end_api_exception(self, monkeypatch):
        """Test API error handling"""

        async def mock_verify_e2e_error(pid, dataset, region):
            raise Exception("Service unavailable")

        monkeypatch.setattr(verification_service, 'verify_end_to_end', mock_verify_e2e_error)

        client = TestClient(app)
        response = client.post(
            "/api/verify-end-to-end",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["success"] is False
        assert "error" in data
        assert "Service unavailable" in data["error"]


class TestCompleteELTPipelineScenarios:
    """Functional tests for complete ELT pipeline scenarios"""

    @pytest.mark.asyncio
    async def test_successful_pipeline_all_components_working(self, monkeypatch):
        """Test scenario where all pipeline components are working correctly"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Mock all steps to return success with realistic data
        async def mock_send_prompts(tid):
            return {
                "success": True,
                "prompts_sent": 5,
                "test_id": tid,
                "message": "Sent 5 test prompts"
            }

        async def mock_cloud_logging(pid, tid, timeout):
            return {
                "success": True,
                "log_count": 15,
                "test_id": tid,
                "message": "Found 15 logs in Cloud Logging"
            }

        async def mock_pubsub(pid, tid, timeout):
            return {
                "success": True,
                "messages_found": 12,
                "test_id": tid,
                "message": "Found 12 messages in Pub/Sub"
            }

        async def mock_dataflow(pid, dataset, reg):
            return {
                "success": True,
                "job_id": "gemini-telemetry-pipeline-123",
                "state": "JOB_STATE_RUNNING",
                "message": "Dataflow job state: JOB_STATE_RUNNING"
            }

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            return {
                "success": True,
                "row_count": 12,
                "elapsed_seconds": 180,
                "message": "Found 12 rows in gemini_raw_logs"
            }

        async def mock_schema(pid, dataset, tid):
            return {
                "success": True,
                "valid_schema": True,
                "all_fields_are_strings": True,
                "json_parseable": True,
                "message": "Schema validation passed"
            }

        async def mock_analytics(pid, dataset, tid):
            return {
                "success": True,
                "row_count": 12,
                "fields_extracted": True,
                "message": "Analytics view returned 12 rows"
            }

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        # Verify complete pipeline success
        assert result["success"] is True
        assert result["pipeline_flow"] == "Gemini CLI → Cloud Logging → Pub/Sub → Dataflow → BigQuery"

        # Verify all step results have realistic data
        assert result["steps"]["send_prompts"]["prompts_sent"] == 5
        assert result["steps"]["cloud_logging"]["log_count"] == 15
        assert result["steps"]["pubsub"]["messages_found"] == 12
        assert result["steps"]["dataflow"]["job_id"] == "gemini-telemetry-pipeline-123"
        assert result["steps"]["bigquery_raw"]["row_count"] == 12
        assert result["steps"]["schema_validation"]["json_parseable"] is True
        assert result["steps"]["analytics_view"]["fields_extracted"] is True

    @pytest.mark.asyncio
    async def test_pipeline_dataflow_not_processing(self, monkeypatch):
        """Test scenario where Dataflow is not processing"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # First 3 steps succeed, but Dataflow fails
        async def mock_send_prompts(tid):
            return {"success": True, "prompts_sent": 5}

        async def mock_cloud_logging(pid, tid, timeout):
            return {"success": True, "log_count": 10}

        async def mock_pubsub(pid, tid, timeout):
            return {"success": True, "messages_found": 8}

        async def mock_dataflow_fail(pid, dataset, reg):
            return {
                "success": False,
                "job_id": None,
                "state": "JOB_STATE_FAILED",
                "error": "Job is not running (state: JOB_STATE_FAILED)"
            }

        async def mock_wait_bq(pid, dataset, tid, max_wait):
            # No data because Dataflow isn't processing
            return {"success": False, "row_count": 0, "error": "Timeout waiting for BigQuery data"}

        async def mock_schema(pid, dataset, tid):
            return {"success": False, "error": "No data found for schema validation"}

        async def mock_analytics(pid, dataset, tid):
            return {"success": False, "row_count": 0, "error": "No data"}

        monkeypatch.setattr(verification_service, '_send_test_prompts', mock_send_prompts)
        monkeypatch.setattr(verification_service, '_verify_cloud_logging', mock_cloud_logging)
        monkeypatch.setattr(verification_service, '_verify_pubsub_messages', mock_pubsub)
        monkeypatch.setattr(verification_service, '_verify_dataflow_processing', mock_dataflow_fail)
        monkeypatch.setattr(verification_service, '_wait_for_bigquery_data', mock_wait_bq)
        monkeypatch.setattr(verification_service, '_verify_json_string_schema', mock_schema)
        monkeypatch.setattr(verification_service, '_query_analytics_view', mock_analytics)

        result = await verification_service.verify_end_to_end(project_id, dataset_name, region)

        # Pipeline should fail overall
        assert result["success"] is False

        # Early steps should succeed
        assert result["steps"]["send_prompts"]["success"] is True
        assert result["steps"]["cloud_logging"]["success"] is True
        assert result["steps"]["pubsub"]["success"] is True

        # Dataflow and downstream should fail
        assert result["steps"]["dataflow"]["success"] is False
        assert result["steps"]["bigquery_raw"]["success"] is False
        assert result["steps"]["schema_validation"]["success"] is False


class TestDataValidationScenarios:
    """Functional tests for data validation scenarios"""

    @pytest.mark.asyncio
    async def test_schema_validation_detects_invalid_json(self, monkeypatch):
        """Test that schema validation correctly detects invalid JSON"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        # Create mock with invalid JSON
        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.resource_json = 'invalid json {'  # Malformed JSON
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

    @pytest.mark.asyncio
    async def test_analytics_view_handles_null_fields_gracefully(self, monkeypatch):
        """Test that analytics view handles null fields gracefully"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        # Create mock rows with some null fields
        mock_client = MagicMock()
        mock_row = MagicMock()
        mock_row.timestamp = "2025-01-01"
        mock_row.event_name = "gemini_cli.api_response"
        mock_row.session_id = None  # Null field
        mock_row.model = "gemini-2.5-flash"
        mock_row.input_tokens = None  # Null field
        mock_row.output_tokens = 50
        mock_row.total_tokens = 50
        mock_row.resource = {"type": "global"}  # Not null
        mock_row.labels = None  # Null field
        mock_row.payload = {"event": "test"}  # Not null

        mock_results = [mock_row]
        mock_job = MagicMock()
        mock_job.result.return_value = mock_results
        mock_client.query.return_value = mock_job

        with patch('google.cloud.bigquery.Client', return_value=mock_client):
            result = await verification_service._query_analytics_view(project_id, dataset_name, test_id)

            # Should still succeed - at least some fields are extracted
            assert result["success"] is True
            assert result["row_count"] == 1
            assert result["fields_extracted"] is True


class TestTimeoutAndRetryScenarios:
    """Functional tests for timeout and retry scenarios"""

    @pytest.mark.asyncio
    async def test_bigquery_wait_with_multiple_retries(self, monkeypatch):
        """Test BigQuery polling with multiple retries before finding data"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        test_id = "test-uuid-123"

        check_count = 0

        async def mock_check_table(pid, dataset):
            nonlocal check_count
            check_count += 1

            # Return no data for first 3 checks, then return data
            if check_count < 3:
                return {"has_data": False, "row_count": 0}
            else:
                return {"has_data": True, "row_count": 15}

        monkeypatch.setattr(verification_service, '_check_bigquery_raw_table', mock_check_table)

        # Mock asyncio.sleep to return immediately
        original_sleep = verification_service.asyncio.sleep

        async def mock_sleep(seconds):
            pass  # Return immediately instead of waiting

        monkeypatch.setattr(verification_service.asyncio, 'sleep', mock_sleep)

        result = await verification_service._wait_for_bigquery_data(
            project_id, dataset_name, test_id, max_wait_seconds=200
        )

        assert result["success"] is True
        assert result["row_count"] == 15
        assert check_count == 3  # Should have checked 3 times


class TestEndToEndDataStructure:
    """Functional tests for E2E verification data structure"""

    @pytest.mark.asyncio
    async def test_verify_end_to_end_returns_complete_structure(self, monkeypatch):
        """Test that E2E verification returns all expected fields"""
        project_id = "test-project"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Mock all steps
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

        # Verify all required fields exist
        required_fields = ["success", "test_id", "steps", "pipeline_flow"]
        for field in required_fields:
            assert field in result, f"Missing required field: {field}"

        # Verify all step fields exist
        required_steps = [
            "send_prompts",
            "cloud_logging",
            "pubsub",
            "dataflow",
            "bigquery_raw",
            "schema_validation",
            "analytics_view"
        ]
        for step in required_steps:
            assert step in result["steps"], f"Missing required step: {step}"

        # Verify data types
        assert isinstance(result["success"], bool)
        assert isinstance(result["test_id"], str)
        assert isinstance(result["steps"], dict)
        assert isinstance(result["pipeline_flow"], str)

        # Verify pipeline flow string
        assert "Gemini CLI" in result["pipeline_flow"]
        assert "Cloud Logging" in result["pipeline_flow"]
        assert "Pub/Sub" in result["pipeline_flow"]
        assert "Dataflow" in result["pipeline_flow"]
        assert "BigQuery" in result["pipeline_flow"]
