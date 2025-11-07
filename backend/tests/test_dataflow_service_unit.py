"""
Unit tests for Dataflow service.
Tests individual functions with mocked dependencies.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import dataflow_service


class TestStartDataflowJob:
    """Test start_dataflow_job function"""

    @pytest.mark.asyncio
    async def test_start_job_success(self):
        """Test successful Dataflow job start"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"
        job_name = "gemini-telemetry-pipeline"

        # Mock successful gcloud command
        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": "2025-01-06_12_34_56-1234567890",
            "name": job_name
        })
        mock_result.stderr = ""

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region
            )

            # Verify result structure
            assert "job_id" in result
            assert "job_name" in result
            assert "region" in result
            assert "status" in result
            assert "console_url" in result
            assert "parameters" in result

            # Verify values
            assert result["job_id"] == "2025-01-06_12_34_56-1234567890"
            assert result["job_name"] == job_name
            assert result["region"] == region
            assert result["status"] == "running"

    @pytest.mark.asyncio
    async def test_start_job_with_custom_job_name(self):
        """Test starting job with custom job name"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        custom_job_name = "custom-pipeline-name"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": "2025-01-06_12_34_56-1234567890",
            "name": custom_job_name
        })

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(
                project_id=project_id,
                dataset_name=dataset_name,
                job_name=custom_job_name
            )

            assert result["job_name"] == custom_job_name

    @pytest.mark.asyncio
    async def test_start_job_command_failure(self):
        """Test handling of gcloud command failure"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        # Mock command failure
        mock_error = subprocess.CalledProcessError(
            returncode=1,
            cmd="gcloud dataflow jobs run",
            stderr="Failed to start job: Permission denied"
        )

        with patch('subprocess.run', side_effect=mock_error):
            with pytest.raises(Exception) as exc_info:
                await dataflow_service.start_dataflow_job(project_id, dataset_name)

            assert "Failed to start Dataflow job" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_start_job_parameters_correct(self):
        """Test that job parameters are correctly formatted"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = json.dumps({"id": "job-123"})

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name, region)

            # Verify command structure
            assert "gcloud" in captured_command
            assert "dataflow" in captured_command
            assert "jobs" in captured_command
            assert "run" in captured_command
            assert project_id in captured_command

            # Verify parameters
            params = result["parameters"]
            assert params["inputSubscription"] == f"projects/{project_id}/subscriptions/gemini-telemetry-sub"
            assert params["outputTableSpec"] == f"{project_id}:{dataset_name}.gemini_raw_logs"
            assert params["javascriptTextTransformGcsPath"] == f"gs://{project_id}-dataflow/transform.js"
            assert params["stagingLocation"] == f"gs://{project_id}-dataflow/temp"


class TestGetJobStatus:
    """Test get_job_status function"""

    @pytest.mark.asyncio
    async def test_get_status_success(self):
        """Test successful job status retrieval"""
        project_id = "test-project-123"
        job_id = "2025-01-06_12_34_56-1234567890"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": job_id,
            "currentState": "JOB_STATE_RUNNING",
            "createTime": "2025-01-06T12:34:56Z",
            "currentStateTime": "2025-01-06T12:35:00Z",
            "type": "JOB_TYPE_STREAMING"
        })

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.get_job_status(project_id, job_id, region)

            assert result["job_id"] == job_id
            assert result["state"] == "JOB_STATE_RUNNING"
            assert "create_time" in result
            assert "current_state_time" in result
            assert result["type"] == "JOB_TYPE_STREAMING"

    @pytest.mark.asyncio
    async def test_get_status_failed_state(self):
        """Test retrieving status of failed job"""
        project_id = "test-project-123"
        job_id = "job-123"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": job_id,
            "currentState": "JOB_STATE_FAILED"
        })

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.get_job_status(project_id, job_id)

            assert result["state"] == "JOB_STATE_FAILED"

    @pytest.mark.asyncio
    async def test_get_status_command_failure(self):
        """Test handling of gcloud describe command failure"""
        project_id = "test-project-123"
        job_id = "nonexistent-job"

        mock_error = subprocess.CalledProcessError(
            returncode=1,
            cmd="gcloud dataflow jobs describe",
            stderr="Job not found"
        )

        with patch('subprocess.run', side_effect=mock_error):
            with pytest.raises(Exception) as exc_info:
                await dataflow_service.get_job_status(project_id, job_id)

            assert "Failed to get job status" in str(exc_info.value)


class TestVerifyJobRunning:
    """Test verify_job_running function"""

    @pytest.mark.asyncio
    async def test_verify_job_running_success(self, monkeypatch):
        """Test verification when job is running"""
        project_id = "test-project-123"
        job_id = "job-123"

        async def mock_get_status(pid, jid, reg):
            return {"state": "JOB_STATE_RUNNING"}

        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_job_running(project_id, job_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_verify_job_failed_state(self, monkeypatch):
        """Test verification when job is in failed state"""
        project_id = "test-project-123"
        job_id = "job-123"

        async def mock_get_status(pid, jid, reg):
            return {"state": "JOB_STATE_FAILED"}

        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_job_running(project_id, job_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_verify_job_timeout(self, monkeypatch):
        """Test verification timeout when job doesn't reach running state"""
        project_id = "test-project-123"
        job_id = "job-123"

        async def mock_get_status(pid, jid, reg):
            return {"state": "JOB_STATE_PENDING"}

        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        # Use short timeout for testing
        result = await dataflow_service.verify_job_running(
            project_id, job_id, max_wait_seconds=5
        )

        assert result is False


class TestStopDataflowJob:
    """Test stop_dataflow_job function"""

    @pytest.mark.asyncio
    async def test_stop_job_success(self):
        """Test successful job cancellation"""
        project_id = "test-project-123"
        job_id = "job-123"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = ""
        mock_result.stderr = ""

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.stop_dataflow_job(project_id, job_id, region)

            assert result["job_id"] == job_id
            assert result["status"] == "cancelled"
            assert "message" in result

    @pytest.mark.asyncio
    async def test_stop_job_failure(self):
        """Test handling of job cancellation failure"""
        project_id = "test-project-123"
        job_id = "job-123"

        mock_error = subprocess.CalledProcessError(
            returncode=1,
            cmd="gcloud dataflow jobs cancel",
            stderr="Cannot cancel job"
        )

        with patch('subprocess.run', side_effect=mock_error):
            with pytest.raises(Exception) as exc_info:
                await dataflow_service.stop_dataflow_job(project_id, job_id)

            assert "Failed to cancel job" in str(exc_info.value)


class TestListDataflowJobs:
    """Test list_dataflow_jobs function"""

    @pytest.mark.asyncio
    async def test_list_jobs_success(self):
        """Test successful job listing"""
        project_id = "test-project-123"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = json.dumps([
            {"id": "job-1", "name": "pipeline-1", "currentState": "JOB_STATE_RUNNING"},
            {"id": "job-2", "name": "pipeline-2", "currentState": "JOB_STATE_DONE"}
        ])

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.list_dataflow_jobs(project_id, region)

            assert "jobs" in result
            assert "count" in result
            assert result["count"] == 2
            assert len(result["jobs"]) == 2

    @pytest.mark.asyncio
    async def test_list_jobs_with_filter(self):
        """Test job listing with status filter"""
        project_id = "test-project-123"
        status_filter = "active"

        mock_result = Mock()
        mock_result.stdout = json.dumps([
            {"id": "job-1", "name": "pipeline-1", "currentState": "JOB_STATE_RUNNING"}
        ])

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.list_dataflow_jobs(
                project_id,
                status_filter=status_filter
            )

            # Verify filter was included in command
            assert "--status" in captured_command
            assert status_filter in captured_command

    @pytest.mark.asyncio
    async def test_list_jobs_empty(self):
        """Test listing when no jobs exist"""
        project_id = "test-project-123"

        mock_result = Mock()
        mock_result.stdout = json.dumps([])

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.list_dataflow_jobs(project_id)

            assert result["count"] == 0
            assert len(result["jobs"]) == 0


class TestExtractJobIdFromOutput:
    """Test _extract_job_id_from_output helper function"""

    def test_extract_job_id_success(self):
        """Test successful job ID extraction"""
        output = "Job created: 2025-01-06_12_34_56-1234567890123456789"

        job_id = dataflow_service._extract_job_id_from_output(output)

        assert job_id == "2025-01-06_12_34_56-1234567890123456789"

    def test_extract_job_id_from_complex_output(self):
        """Test extraction from complex output"""
        output = """
        Creating Dataflow job...
        Job ID: 2025-01-06_12_34_56-1234567890
        View job at: https://console.cloud.google.com/dataflow/...
        """

        job_id = dataflow_service._extract_job_id_from_output(output)

        assert job_id == "2025-01-06_12_34_56-1234567890"

    def test_extract_job_id_not_found(self):
        """Test when job ID cannot be found"""
        output = "Error: Failed to create job"

        job_id = dataflow_service._extract_job_id_from_output(output)

        assert job_id is None
