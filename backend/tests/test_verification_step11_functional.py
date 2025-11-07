"""
Functional tests for Step 11 verification.
Tests API endpoints and end-to-end verification scenarios.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from fastapi.testclient import TestClient

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app
from services import dataflow_service, verification_service


class TestVerifyDataflowAPIEndpoint:
    """Functional tests for /api/verify-dataflow endpoint"""

    def test_verify_dataflow_endpoint_success(self, monkeypatch):
        """Test successful Dataflow verification via API"""

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "job_found": True,
                "job_id": "job-123",
                "state": "JOB_STATE_RUNNING",
                "is_running": True,
                "input_subscription": "projects/test/subscriptions/gemini-telemetry-sub",
                "output_table": "test:dataset.gemini_raw_logs",
                "udf_path": "gs://test-dataflow/transform.js",
                "configuration_correct": True,
                "issues": []
            }

        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)

        client = TestClient(app)
        response = client.post(
            "/api/verify-dataflow",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert data["data"]["job_found"] is True
        assert data["data"]["is_running"] is True
        assert "Dataflow pipeline verification completed" in data["message"]

    def test_verify_dataflow_endpoint_job_not_running(self, monkeypatch):
        """Test API response when Dataflow job is not running"""

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "job_found": True,
                "job_id": "job-failed",
                "state": "JOB_STATE_FAILED",
                "is_running": False,
                "input_subscription": None,
                "output_table": None,
                "udf_path": None,
                "configuration_correct": False,
                "issues": ["Job is not running (state: JOB_STATE_FAILED)"]
            }

        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)

        client = TestClient(app)
        response = client.post(
            "/api/verify-dataflow",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        # Success should be False when job is not running
        assert data["success"] is False
        assert data["data"]["is_running"] is False
        assert len(data["data"]["issues"]) > 0

    def test_verify_dataflow_endpoint_exception(self, monkeypatch):
        """Test API error handling"""

        async def mock_verify_dataflow_error(pid, dataset, region):
            raise Exception("API connection timeout")

        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow_error)

        client = TestClient(app)
        response = client.post(
            "/api/verify-dataflow",
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
        assert "API connection timeout" in data["error"]


class TestVerifyELTPipelineAPIEndpoint:
    """Functional tests for /api/verify-elt-pipeline endpoint"""

    def test_verify_elt_pipeline_endpoint_success(self, monkeypatch):
        """Test successful ELT pipeline verification via API"""

        async def mock_verify_elt(pid, dataset, region):
            return {
                "pipeline_ready": True,
                "pubsub_topic_exists": True,
                "pubsub_subscription_exists": True,
                "sink_configured": True,
                "dataflow_running": True,
                "gcs_bucket_exists": True,
                "udf_exists": True,
                "bigquery_table_exists": True,
                "issues": [],
                "details": {
                    "pubsub": {"topic_exists": True, "subscription_exists": True},
                    "sink": {"verified": True},
                    "dataflow": {"is_running": True},
                    "gcs": {"bucket_exists": True, "udf_exists": True},
                    "bigquery": {"table_exists": True}
                }
            }

        monkeypatch.setattr(verification_service, 'verify_elt_pipeline', mock_verify_elt)

        client = TestClient(app)
        response = client.post(
            "/api/verify-elt-pipeline",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        assert data["success"] is True
        assert data["data"]["pipeline_ready"] is True
        assert data["data"]["pubsub_topic_exists"] is True
        assert data["data"]["dataflow_running"] is True
        assert "ELT pipeline verification completed" in data["message"]

    def test_verify_elt_pipeline_endpoint_partial_failure(self, monkeypatch):
        """Test API response when some pipeline components fail"""

        async def mock_verify_elt(pid, dataset, region):
            return {
                "pipeline_ready": False,
                "pubsub_topic_exists": False,
                "pubsub_subscription_exists": False,
                "sink_configured": True,
                "dataflow_running": True,
                "gcs_bucket_exists": True,
                "udf_exists": True,
                "bigquery_table_exists": True,
                "issues": [
                    "Pub/Sub topic 'gemini-telemetry-topic' not found",
                    "Pub/Sub subscription 'gemini-telemetry-sub' not found"
                ],
                "details": {}
            }

        monkeypatch.setattr(verification_service, 'verify_elt_pipeline', mock_verify_elt)

        client = TestClient(app)
        response = client.post(
            "/api/verify-elt-pipeline",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        assert response.status_code == 200
        data = response.json()

        # Success should be False when pipeline is not ready
        assert data["success"] is False
        assert data["data"]["pipeline_ready"] is False
        assert len(data["data"]["issues"]) > 0

    def test_verify_elt_pipeline_endpoint_exception(self, monkeypatch):
        """Test API error handling"""

        async def mock_verify_elt_error(pid, dataset, region):
            raise Exception("Service unavailable")

        monkeypatch.setattr(verification_service, 'verify_elt_pipeline', mock_verify_elt_error)

        client = TestClient(app)
        response = client.post(
            "/api/verify-elt-pipeline",
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


class TestEndToEndVerificationScenarios:
    """Functional tests for end-to-end verification scenarios"""

    @pytest.mark.asyncio
    async def test_complete_pipeline_verification_success_scenario(self, monkeypatch):
        """Test complete success scenario: all components verified"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Mock all services to return success
        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {
                "verified": True,
                "sink_name": sink_name,
                "destination": f"pubsub.googleapis.com/projects/{pid}/topics/gemini-telemetry-topic"
            }

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "job_found": True,
                "job_id": "job-running-123",
                "state": "JOB_STATE_RUNNING",
                "is_running": True,
                "configuration_correct": True,
                "issues": []
            }

        async def mock_verify_bucket(pid, bucket):
            return True

        async def mock_verify_file(pid, bucket, file):
            return True

        async def mock_verify_table(pid, dataset, table):
            return True

        from services import pubsub_service, sink_service, dataflow_service, gcs_service, bigquery_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub)
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        # Execute verification
        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Verify all components pass
        assert result["pipeline_ready"] is True
        assert result["pubsub_topic_exists"] is True
        assert result["pubsub_subscription_exists"] is True
        assert result["sink_configured"] is True
        assert result["dataflow_running"] is True
        assert result["gcs_bucket_exists"] is True
        assert result["udf_exists"] is True
        assert result["bigquery_table_exists"] is True
        assert len(result["issues"]) == 0

    @pytest.mark.asyncio
    async def test_pipeline_verification_missing_infrastructure_scenario(self, monkeypatch):
        """Test scenario where infrastructure is not deployed yet"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # All verifications fail - nothing deployed
        async def mock_verify_topic(pid, topic):
            return False

        async def mock_verify_sub(pid, sub):
            return False

        async def mock_verify_sink(pid, sink_name):
            raise Exception("Sink not found")

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "job_found": False,
                "is_running": False,
                "issues": ["Job 'gemini-telemetry-pipeline' not found in active jobs"]
            }

        async def mock_verify_bucket(pid, bucket):
            return False

        async def mock_verify_file(pid, bucket, file):
            return False

        async def mock_verify_table(pid, dataset, table):
            return False

        from services import pubsub_service, sink_service, dataflow_service, gcs_service, bigquery_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub)
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        # Execute verification
        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Verify all components fail
        assert result["pipeline_ready"] is False
        assert result["pubsub_topic_exists"] is False
        assert result["pubsub_subscription_exists"] is False
        assert result["sink_configured"] is False
        assert result["dataflow_running"] is False
        assert result["gcs_bucket_exists"] is False
        assert result["udf_exists"] is False
        assert result["bigquery_table_exists"] is False

        # Should have multiple issues
        assert len(result["issues"]) > 5

    @pytest.mark.asyncio
    async def test_pipeline_verification_dataflow_stopped_scenario(self, monkeypatch):
        """Test scenario where Dataflow job exists but is stopped"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Infrastructure exists but Dataflow is stopped
        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "job_found": True,
                "job_id": "job-stopped-123",
                "state": "JOB_STATE_CANCELLED",
                "is_running": False,
                "issues": ["Job is not running (state: JOB_STATE_CANCELLED)"]
            }

        async def mock_verify_bucket(pid, bucket):
            return True

        async def mock_verify_file(pid, bucket, file):
            return True

        async def mock_verify_table(pid, dataset, table):
            return True

        from services import pubsub_service, sink_service, dataflow_service, gcs_service, bigquery_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub)
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        # Execute verification
        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Pipeline not ready due to stopped Dataflow
        assert result["pipeline_ready"] is False
        assert result["dataflow_running"] is False

        # But other components should pass
        assert result["pubsub_topic_exists"] is True
        assert result["sink_configured"] is True
        assert result["gcs_bucket_exists"] is True
        assert result["bigquery_table_exists"] is True

        # Should have Dataflow-specific issue
        assert any("Dataflow" in issue for issue in result["issues"])
        assert any("not running" in issue for issue in result["issues"])


class TestVerificationDataStructure:
    """Functional tests for verification data structure and completeness"""

    @pytest.mark.asyncio
    async def test_dataflow_verification_returns_complete_structure(self, monkeypatch):
        """Test that Dataflow verification returns all expected fields"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-123",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {}
                    }
                ],
                "count": 1
            }

        async def mock_get_status(pid, jid, region):
            return {
                "job_id": jid,
                "state": "JOB_STATE_RUNNING"
            }

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        # Verify all required fields exist
        required_fields = [
            "job_found",
            "job_id",
            "state",
            "is_running",
            "input_subscription",
            "output_table",
            "udf_path",
            "configuration_correct",
            "issues"
        ]

        for field in required_fields:
            assert field in result, f"Missing required field: {field}"

        # Verify data types
        assert isinstance(result["job_found"], bool)
        assert isinstance(result["is_running"], bool)
        assert isinstance(result["configuration_correct"], bool)
        assert isinstance(result["issues"], list)
        assert isinstance(result["job_id"], str) or result["job_id"] is None
        assert isinstance(result["state"], str) or result["state"] is None

    @pytest.mark.asyncio
    async def test_elt_verification_returns_complete_structure(self, monkeypatch):
        """Test that ELT verification returns all expected fields"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Mock all services
        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        async def mock_verify_dataflow(pid, dataset, region):
            return {"is_running": True, "issues": []}

        async def mock_verify_bucket(pid, bucket):
            return True

        async def mock_verify_file(pid, bucket, file):
            return True

        async def mock_verify_table(pid, dataset, table):
            return True

        from services import pubsub_service, sink_service, dataflow_service, gcs_service, bigquery_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub)
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Verify all required fields exist
        required_fields = [
            "pipeline_ready",
            "pubsub_topic_exists",
            "pubsub_subscription_exists",
            "sink_configured",
            "dataflow_running",
            "gcs_bucket_exists",
            "udf_exists",
            "bigquery_table_exists",
            "issues",
            "details"
        ]

        for field in required_fields:
            assert field in result, f"Missing required field: {field}"

        # Verify data types
        assert isinstance(result["pipeline_ready"], bool)
        assert isinstance(result["pubsub_topic_exists"], bool)
        assert isinstance(result["pubsub_subscription_exists"], bool)
        assert isinstance(result["sink_configured"], bool)
        assert isinstance(result["dataflow_running"], bool)
        assert isinstance(result["gcs_bucket_exists"], bool)
        assert isinstance(result["udf_exists"], bool)
        assert isinstance(result["bigquery_table_exists"], bool)
        assert isinstance(result["issues"], list)
        assert isinstance(result["details"], dict)
