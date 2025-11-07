"""
Unit tests for Step 11 verification functions.
Tests verify_dataflow_pipeline and verify_elt_pipeline functions.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import dataflow_service, verification_service


class TestVerifyDataflowPipeline:
    """Test verify_dataflow_pipeline function"""

    @pytest.mark.asyncio
    async def test_verify_pipeline_job_found_and_running(self, monkeypatch):
        """Test successful verification when job is found and running"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-123",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {"tempStoragePrefix": "gs://test-project-123-dataflow/temp"}
                    }
                ],
                "count": 1
            }

        async def mock_get_status(pid, jid, region):
            return {
                "job_id": "job-123",
                "state": "JOB_STATE_RUNNING"
            }

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        assert result["job_found"] is True
        assert result["is_running"] is True
        assert result["job_id"] == "job-123"
        assert result["state"] == "JOB_STATE_RUNNING"
        assert len(result["issues"]) == 0

    @pytest.mark.asyncio
    async def test_verify_pipeline_job_not_found(self, monkeypatch):
        """Test when job is not found"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {"jobs": [], "count": 0}

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        assert result["job_found"] is False
        assert result["is_running"] is False
        assert "not found in active jobs" in result["issues"][0]

    @pytest.mark.asyncio
    async def test_verify_pipeline_job_not_running(self, monkeypatch):
        """Test when job exists but is not running"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-123",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_FAILED",
                        "environment": {}
                    }
                ],
                "count": 1
            }

        async def mock_get_status(pid, jid, region):
            return {"job_id": "job-123", "state": "JOB_STATE_FAILED"}

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs)
        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        assert result["job_found"] is True
        assert result["is_running"] is False
        assert any("not running" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_exception_handling(self, monkeypatch):
        """Test exception handling during verification"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs_error(pid, region, status_filter=None):
            raise Exception("API error")

        monkeypatch.setattr(dataflow_service, 'list_dataflow_jobs', mock_list_jobs_error)

        result = await dataflow_service.verify_dataflow_pipeline(project_id, dataset_name)

        assert result["job_found"] is False
        assert result["is_running"] is False
        assert "API error" in result["issues"][0]

    @pytest.mark.asyncio
    async def test_verify_pipeline_configuration_mismatch(self, monkeypatch):
        """Test detection of configuration mismatches"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-123",
                        "name": "gemini-telemetry-pipeline",
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {
                            # Wrong project in temp location
                            "tempStoragePrefix": "gs://wrong-project-dataflow/temp"
                        }
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

        assert result["job_found"] is True
        assert result["is_running"] is True
        assert result["configuration_correct"] is False
        assert any("Temp location doesn't match project" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_custom_job_name(self, monkeypatch):
        """Test verification with custom job name"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        custom_job_name = "custom-pipeline-name"

        async def mock_list_jobs(pid, region, status_filter=None):
            return {
                "jobs": [
                    {
                        "id": "job-custom",
                        "name": custom_job_name,
                        "currentState": "JOB_STATE_RUNNING",
                        "environment": {"tempStoragePrefix": f"gs://{pid}-dataflow/temp"}
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

        result = await dataflow_service.verify_dataflow_pipeline(
            project_id, dataset_name, job_name=custom_job_name
        )

        assert result["job_found"] is True
        assert result["job_id"] == "job-custom"
        assert result["is_running"] is True


class TestVerifyELTPipeline:
    """Test verify_elt_pipeline function"""

    @pytest.mark.asyncio
    async def test_verify_pipeline_all_components_ready(self, monkeypatch):
        """Test when all ELT components are ready"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Mock all service verification functions
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

        # Import services after defining mocks
        from services import pubsub_service, sink_service, dataflow_service, gcs_service, bigquery_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub)
        monkeypatch.setattr(sink_service, 'verify_sink', mock_verify_sink)
        monkeypatch.setattr(dataflow_service, 'verify_dataflow_pipeline', mock_verify_dataflow)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)
        monkeypatch.setattr(bigquery_service, 'verify_table_exists', mock_verify_table)

        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

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
    async def test_verify_pipeline_missing_pubsub(self, monkeypatch):
        """Test when Pub/Sub resources are missing"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return False

        async def mock_verify_sub(pid, sub):
            return False

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

        assert result["pipeline_ready"] is False
        assert result["pubsub_topic_exists"] is False
        assert result["pubsub_subscription_exists"] is False
        assert any("topic" in issue.lower() for issue in result["issues"])
        assert any("subscription" in issue.lower() for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_dataflow_not_running(self, monkeypatch):
        """Test when Dataflow is not running"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "is_running": False,
                "issues": ["Job not found in active jobs"]
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

        assert result["pipeline_ready"] is False
        assert result["dataflow_running"] is False
        assert any("Dataflow" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_sink_error(self, monkeypatch):
        """Test when sink verification fails"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink_error(pid, sink_name):
            raise Exception("Sink not found")

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

        assert result["pipeline_ready"] is False
        assert result["sink_configured"] is False
        assert any("Sink" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_missing_gcs_resources(self, monkeypatch):
        """Test when GCS resources are missing"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {"verified": True}

        async def mock_verify_dataflow(pid, dataset, region):
            return {"is_running": True, "issues": []}

        async def mock_verify_bucket(pid, bucket):
            return False

        async def mock_verify_file(pid, bucket, file):
            return False

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

        assert result["pipeline_ready"] is False
        assert result["gcs_bucket_exists"] is False
        assert result["udf_exists"] is False
        assert any("bucket" in issue.lower() for issue in result["issues"])
        assert any("UDF" in issue or "transform.js" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_complete_exception(self, monkeypatch):
        """Test handling of complete pipeline verification exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # First verification call succeeds
        async def mock_verify_topic(pid, topic):
            return True

        # Second verification call raises exception (simulating service failure)
        async def mock_verify_sub_error(pid, sub):
            raise Exception("Network timeout")

        from services import pubsub_service

        monkeypatch.setattr(pubsub_service, 'verify_topic_exists', mock_verify_topic)
        monkeypatch.setattr(pubsub_service, 'verify_subscription_exists', mock_verify_sub_error)

        # Should not crash, should return error state
        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        assert result["pipeline_ready"] is False
        assert any("ELT pipeline verification failed" in issue for issue in result["issues"])

    @pytest.mark.asyncio
    async def test_verify_pipeline_details_structure(self, monkeypatch):
        """Test that verification details have expected structure"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        async def mock_verify_topic(pid, topic):
            return True

        async def mock_verify_sub(pid, sub):
            return True

        async def mock_verify_sink(pid, sink_name):
            return {
                "verified": True,
                "destination": "pubsub.googleapis.com/...",
                "service_account": "sink@project.iam.gserviceaccount.com"
            }

        async def mock_verify_dataflow(pid, dataset, region):
            return {
                "is_running": True,
                "job_id": "job-123",
                "state": "JOB_STATE_RUNNING",
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

        result = await verification_service.verify_elt_pipeline(project_id, dataset_name)

        # Verify details have expected structure
        assert "details" in result
        assert "pubsub" in result["details"]
        assert "sink" in result["details"]
        assert "dataflow" in result["details"]
        assert "gcs" in result["details"]
        assert "bigquery" in result["details"]

        # Verify details contain expected data
        assert result["details"]["pubsub"]["topic_exists"] is True
        assert result["details"]["sink"]["verified"] is True
        assert result["details"]["dataflow"]["job_id"] == "job-123"
        assert result["details"]["gcs"]["bucket_exists"] is True
