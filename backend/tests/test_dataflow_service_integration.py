"""
Integration tests for Dataflow service.
Tests complete workflows with mocked subprocess commands.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import dataflow_service


class TestJobStartWorkflow:
    """Integration tests for job start workflow"""

    @pytest.mark.asyncio
    async def test_complete_job_start_workflow(self):
        """Test complete workflow from job submission to ID extraction"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"

        # Track workflow steps
        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("job_submitted")
            mock_result = Mock()
            mock_result.stdout = json.dumps({
                "id": "2025-01-06_12_34_56-1234567890",
                "name": "gemini-telemetry-pipeline"
            })
            mock_result.stderr = ""
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name, region)

            # Verify workflow completed
            assert "job_submitted" in workflow_steps
            assert result["job_id"] == "2025-01-06_12_34_56-1234567890"
            assert result["status"] == "running"

    @pytest.mark.asyncio
    async def test_job_start_builds_correct_gcloud_command(self):
        """Test that gcloud command is built correctly"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "europe-west1"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await dataflow_service.start_dataflow_job(project_id, dataset_name, region)

            # Verify command structure
            assert captured_command[0] == "gcloud"
            assert captured_command[1] == "dataflow"
            assert captured_command[2] == "jobs"
            assert captured_command[3] == "run"
            assert "--region" in captured_command
            assert region in captured_command
            assert "--project" in captured_command
            assert project_id in captured_command


class TestJobStatusWorkflow:
    """Integration tests for job status workflow"""

    @pytest.mark.asyncio
    async def test_complete_status_check_workflow(self):
        """Test complete workflow for checking job status"""
        project_id = "test-project-123"
        job_id = "job-123"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("status_checked")
            mock_result = Mock()
            mock_result.stdout = json.dumps({
                "id": job_id,
                "currentState": "JOB_STATE_RUNNING",
                "createTime": "2025-01-06T12:34:56Z",
                "currentStateTime": "2025-01-06T12:35:00Z",
                "type": "JOB_TYPE_STREAMING"
            })
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await dataflow_service.get_job_status(project_id, job_id)

            assert "status_checked" in workflow_steps
            assert result["job_id"] == job_id
            assert result["state"] == "JOB_STATE_RUNNING"

    @pytest.mark.asyncio
    async def test_status_check_builds_correct_command(self):
        """Test that status check builds correct gcloud command"""
        project_id = "test-project-123"
        job_id = "job-123"
        region = "asia-east1"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": job_id, "currentState": "JOB_STATE_RUNNING"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await dataflow_service.get_job_status(project_id, job_id, region)

            assert "gcloud" in captured_command
            assert "dataflow" in captured_command
            assert "jobs" in captured_command
            assert "describe" in captured_command
            assert job_id in captured_command
            assert "--region" in captured_command
            assert region in captured_command


class TestJobVerificationWorkflow:
    """Integration tests for job verification workflow"""

    @pytest.mark.asyncio
    async def test_complete_verification_workflow(self, monkeypatch):
        """Test complete job verification workflow"""
        project_id = "test-project-123"
        job_id = "job-123"

        workflow_steps = []

        async def mock_get_status(pid, jid, reg):
            workflow_steps.append("status_polled")
            return {"state": "JOB_STATE_RUNNING"}

        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_job_running(project_id, job_id, max_wait_seconds=5)

        # Verify workflow completed
        assert "status_polled" in workflow_steps
        assert result is True

    @pytest.mark.asyncio
    async def test_verification_polling_logic(self, monkeypatch):
        """Test that verification polls until job is running"""
        project_id = "test-project-123"
        job_id = "job-123"

        poll_count = 0

        async def mock_get_status(pid, jid, reg):
            nonlocal poll_count
            poll_count += 1
            # Return PENDING first time, RUNNING second time
            if poll_count == 1:
                return {"state": "JOB_STATE_PENDING"}
            else:
                return {"state": "JOB_STATE_RUNNING"}

        monkeypatch.setattr(dataflow_service, 'get_job_status', mock_get_status)

        result = await dataflow_service.verify_job_running(project_id, job_id, max_wait_seconds=30)

        # Verify multiple polls occurred
        assert poll_count >= 2
        assert result is True


class TestJobCancellationWorkflow:
    """Integration tests for job cancellation workflow"""

    @pytest.mark.asyncio
    async def test_complete_cancellation_workflow(self):
        """Test complete job cancellation workflow"""
        project_id = "test-project-123"
        job_id = "job-123"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("job_cancelled")
            mock_result = Mock()
            mock_result.stdout = ""
            mock_result.stderr = ""
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await dataflow_service.stop_dataflow_job(project_id, job_id)

            assert "job_cancelled" in workflow_steps
            assert result["status"] == "cancelled"
            assert result["job_id"] == job_id

    @pytest.mark.asyncio
    async def test_cancellation_builds_correct_command(self):
        """Test that cancellation builds correct gcloud command"""
        project_id = "test-project-123"
        job_id = "job-123"
        region = "us-west1"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = ""
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            await dataflow_service.stop_dataflow_job(project_id, job_id, region)

            assert "gcloud" in captured_command
            assert "dataflow" in captured_command
            assert "jobs" in captured_command
            assert "cancel" in captured_command
            assert job_id in captured_command
            assert "--region" in captured_command
            assert region in captured_command


class TestJobListingWorkflow:
    """Integration tests for job listing workflow"""

    @pytest.mark.asyncio
    async def test_complete_listing_workflow(self):
        """Test complete job listing workflow"""
        project_id = "test-project-123"

        workflow_steps = []

        def mock_subprocess_run(*args, **kwargs):
            workflow_steps.append("jobs_listed")
            mock_result = Mock()
            mock_result.stdout = json.dumps([
                {"id": "job-1", "currentState": "JOB_STATE_RUNNING"},
                {"id": "job-2", "currentState": "JOB_STATE_DONE"}
            ])
            return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            result = await dataflow_service.list_dataflow_jobs(project_id)

            assert "jobs_listed" in workflow_steps
            assert result["count"] == 2
            assert len(result["jobs"]) == 2

    @pytest.mark.asyncio
    async def test_listing_with_filter_workflow(self):
        """Test listing workflow with status filter"""
        project_id = "test-project-123"
        status_filter = "active"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps([{"id": "job-1"}])
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.list_dataflow_jobs(project_id, status_filter=status_filter)

            # Verify filter was applied
            assert "--status" in captured_command
            assert status_filter in captured_command
            assert result["count"] == 1


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_job_start_permission_denied(self):
        """Test handling of permission denied error"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        mock_error = subprocess.CalledProcessError(
            returncode=1,
            cmd="gcloud dataflow jobs run",
            stderr="ERROR: (gcloud.dataflow.jobs.run) User does not have permission"
        )

        with patch('subprocess.run', side_effect=mock_error):
            with pytest.raises(Exception) as exc_info:
                await dataflow_service.start_dataflow_job(project_id, dataset_name)

            assert "Failed to start Dataflow job" in str(exc_info.value)
            assert "permission" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_job_status_not_found(self):
        """Test handling of job not found error"""
        project_id = "test-project-123"
        job_id = "nonexistent-job"

        mock_error = subprocess.CalledProcessError(
            returncode=1,
            cmd="gcloud dataflow jobs describe",
            stderr="ERROR: Job not found"
        )

        with patch('subprocess.run', side_effect=mock_error):
            with pytest.raises(Exception) as exc_info:
                await dataflow_service.get_job_status(project_id, job_id)

            assert "Failed to get job status" in str(exc_info.value)


class TestJobIdExtraction:
    """Integration tests for job ID extraction"""

    @pytest.mark.asyncio
    async def test_job_id_extraction_from_json(self):
        """Test job ID extraction from JSON output"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": "2025-01-06_12_34_56-1234567890",
            "name": "test-pipeline"
        })

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            assert result["job_id"] == "2025-01-06_12_34_56-1234567890"

    @pytest.mark.asyncio
    async def test_job_id_extraction_from_stderr(self):
        """Test job ID extraction from stderr when not in stdout"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        mock_result = Mock()
        mock_result.stdout = json.dumps({})  # No ID in stdout
        mock_result.stderr = "Job created: 2025-01-06_12_34_56-9876543210"

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            assert result["job_id"] == "2025-01-06_12_34_56-9876543210"
