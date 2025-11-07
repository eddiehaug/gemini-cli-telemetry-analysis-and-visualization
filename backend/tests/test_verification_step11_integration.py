"""
Integration tests for Step 11 verification functions.
Tests complete workflows with mocked service dependencies.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import json

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import dataflow_service, verification_service


class TestVerifyDataflowPipelineWorkflow:
    """Integration tests for verify_dataflow_pipeline workflow"""

    @pytest.mark.asyncio
    async def test_complete_verification_workflow_success(self, monkeypatch):
        """Test complete successful verification workflow"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        workflow_steps = []

        async def mock_list_jobs(pid, region, status_filter=None):
            workflow_steps.append("list_jobs_called")
            return {
                "jobs": [
                    {
                        "id": "job-abc123",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {
                            "tempStoragePrefix": f"gs://{pid}-dataflow/temp"
                        }
                    }
                ],
                "count": 1
            }

        async def mock_get_status(pid, jid, region):
            workflow_steps.append("get_status_called")
            return {
                "job_id": jid,
                "state": "JOB_STATE_RUNNING",
                "type": "JOB_TYPE_STREAMING"
            }

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        # Verify complete workflow executed
        assert "list_jobs_called" in workflow_steps
        assert "get_status_called" in workflow_steps

        # Verify result structure
        assert result["job_found"] is True
        assert result["is_running"] is True
        assert result["job_id"] == "job-abc123"
        assert result["state"] == "JOB_STATE_RUNNING"
        assert result["configuration_correct"] is True
        assert len(result["issues"]) == 0

        # Verify expected values are set
        assert "gemini-telemetry-sub" in result["input_subscription"]
        assert "gemini_raw_logs" in result["output_table"]
        assert "transform.js" in result["udf_path"]

    @pytest.mark.asyncio
    async def test_verification_workflow_job_not_running(self, monkeypatch):
        """Test workflow when job exists but is not running"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-failed",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_FAILED",
                        "environment": {}
                    }
                ],
                "count": 1
            }

        async def mock_get_status(pid, jid, region):
            return {
                "job_id": jid,
                "state": "JOB_STATE_FAILED"
            }

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        assert result["job_found"] is True
        assert result["is_running"] is False
        assert result["state"] == "JOB_STATE_FAILED"
        assert any("not running" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verification_workflow_multiple_jobs(self, monkeypatch):
        """Test workflow when multiple jobs exist - should find correct one"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-other",
                        "name": "other-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {}
                    },
                    {
                        "id": "job-correct",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {"tempStoragePrefix": f"gs://{pid}-dataflow/temp"}
                    }
                ],
                "count": 2
            }

        async def mock_get_status(pid, jid, region):
            return {
                "job_id": jid,
                "state": "JOB_STATE_RUNNING"
            }

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        # Should find the correct job by name
        assert result["job_found"] is True
        assert result["job_id"] == "job-correct"
        assert result["is_running"] is True


class TestVerifyELTPipelineWorkflow:
    """Integration tests for verify_elt_pipeline workflow"""

    @pytest.mark.asyncio
    async def test_complete_elt_verification_workflow_success(self, monkeypatch):
        """Test complete successful ELT pipeline verification"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        workflow_steps = []

        # Mock Pub/Sub service
        async def mock_verify_topic(pid, topic):
            workflow_steps.append("verify_topic")
            return True

        async def mock_verify_sub(pid, sub):
            workflow_steps.append("verify_subscription")
            return True

        # Mock Sink service
        async def mock_verify_sink(pid, sink_name):
            workflow_steps.append("verify_sink")
            return {"verified": True, "destination": f"pubsub.googleapis.com/projects/{pid}/topics/gemini-telemetry-topic"}

        # Mock Dataflow service
        async def mock_verify_dataflow(pid, dataset, region):
            workflow_steps.append("verify_dataflow")
            return {
                "is_running": True,
                "job_found": True,
                "job_id": "job-123",
                "issues": []
            }

        # Mock GCS service
        async def mock_verify_bucket(pid, bucket):
            workflow_steps.append("verify_bucket")
            return True

        async def mock_verify_file(pid, bucket, file):
            workflow_steps.append("verify_udf")
            return True

        # Mock BigQuery service
        async def mock_verify_table(pid, dataset, table):
            workflow_steps.append("verify_table")
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

        # Verify all workflow steps executed in order
        assert "verify_topic" in workflow_steps
        assert "verify_subscription" in workflow_steps
        assert "verify_sink" in workflow_steps
        assert "verify_dataflow" in workflow_steps
        assert "verify_bucket" in workflow_steps
        assert "verify_udf" in workflow_steps
        assert "verify_table" in workflow_steps

        # Verify result
        assert result["pipeline_ready"] is True
        assert result["pubsub_topic_exists"] is True
        assert result["pubsub_subscription_exists"] is True
        assert result["sink_configured"] is True
        assert result["dataflow_running"] is True
        assert result["gcs_bucket_exists"] is True
        assert result["udf_exists"] is True
        assert result["bigquery_table_exists"] is True
        assert len(result["issues"]) == 0

        # Verify details are populated
        assert "pubsub" in result["details"]
        assert "sink" in result["details"]
        assert "dataflow" in result["details"]
        assert "gcs" in result["details"]
        assert "bigquery" in result["details"]

    @pytest.mark.asyncio
    async def test_elt_verification_partial_failure(self, monkeypatch):
        """Test ELT verification when some components fail"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Pub/Sub fails
        async def mock_verify_topic(pid, topic):
            return False

        async def mock_verify_sub(pid, sub):
            return False

        # Rest succeed
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

        # Pipeline should not be ready
        assert result["pipeline_ready"] is False

        # Pub/Sub components should be false
        assert result["pubsub_topic_exists"] is False
        assert result["pubsub_subscription_exists"] is False

        # But other components should be true
        assert result["sink_configured"] is True
        assert result["dataflow_running"] is True
        assert result["gcs_bucket_exists"] is True

        # Should have issues reported
        assert len(result["issues"]) >= 2
        assert any("topic" in issue.lower() for issue in result["issues"])
        assert any("subscription" in issue.lower() for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_elt_verification_dataflow_issues_propagated(self, monkeypatch):
        """Test that Dataflow issues are properly propagated"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        # Dataflow returns issues
        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "is_running": False,
                "issues": [
                    "Job not found in active jobs",
                    "Expected job 'gemini-telemetry-pipeline' does not exist"
                ]
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

        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Pipeline should not be ready
        assert result["pipeline_ready"] is False
        assert result["dataflow_running"] is False

        # Dataflow issues should be in main issues list with prefix
        assert any("Dataflow:" in issue for issue in result["issues"])
        assert any("Job not found" in issue for issue in result["issues"])


class TestVerificationErrorHandling:
    """Integration tests for error handling in verification"""

    @pytest.mark.asyncio
    async def test_dataflow_verification_handles_exception(self, monkeypatch):
        """Test that exceptions in list_dataflow_jobs are handled gracefully"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs_error(pid, region, status_filter=None):
            raise Exception("API connection failed")

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs_error)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        # Should return graceful failure
        assert result["job_found"] is False
        assert result["is_running"] is False
        assert "API connection failed" in result["issues"][0]

    @pytest.mark.asyncio
    async def test_elt_verification_handles_sink_exception(self, monkeypatch):
        """Test that sink verification exceptions are handled gracefully"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        # Sink raises exception
        async def mock_verify_sink_error(pid, sink_name):
            raise Exception("Sink API timeout")

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
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink_error)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Pipeline should not be ready due to sink failure
        assert result["pipeline_ready"] is False
        assert result["sink_configured"] is False

        # Error should be in issues
        assert any("Sink API timeout" in issue for issue in result["issues"])

        # Details should contain error
        assert "error" in result["details"]["sink"]


class TestVerificationRegionalSupport:
    """Integration tests for regional Dataflow support"""

    @pytest.mark.asyncio
    async def test_dataflow_verification_different_regions(self, monkeypatch):
        """Test verification works with different regions"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        regions_tested = []

        async def mock_list_jobs(pid, region, status_filter=None):
            regions_tested.append(region)
            return {
                "jobs": [
                    {
                        "id": f"job-{region}",
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

        # Test different regions
        for region in ["us-central1", "europe-west1", "asia-east1"]:
            result = await dataflow_service.verify_dataflow_pipeline(
                project_id, dataset_name, region=region
            )

            assert result["job_found"] is True
            assert result["is_running"] is True

        # Verify all regions were tested
        assert "us-central1" in regions_tested
        assert "europe-west1" in regions_tested
        assert "asia-east1" in regions_tested

    @pytest.mark.asyncio
    async def test_elt_verification_regional_configuration(self, monkeypatch):
        """Test ELT verification passes region to Dataflow verification"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        test_region = "europe-west4"

        dataflow_region_received = None

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        async def mock_verify_dataflow(pid, dataset, region):
            nonlocal dataflow_region_received
            dataflow_region_received = region
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

        result = await verification_service.verify_elt_pipeline(
            project_id, dataset_name, region=test_region
        )

        # Verify region was passed to Dataflow verification
        assert dataflow_region_received == test_region
        assert result["pipeline_ready"] is True
