"""
Functional tests for Dataflow service.
Tests end-to-end workflows and ELT pipeline readiness.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch, AsyncMock
import subprocess

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import dataflow_service


class TestDataflowAPIEndToEnd:
    """Functional test for Dataflow API endpoint"""

    @pytest.mark.asyncio
    async def test_start_dataflow_api_endpoint_simulation(self):
        """Simulate FastAPI endpoint call to start Dataflow"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": "2025-01-06_12_34_56-1234567890",
            "name": "gemini-telemetry-pipeline"
        })

        with patch('subprocess.run', return_value=mock_result):
            # Simulate API endpoint call
            result = await dataflow_service.start_dataflow_job(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region
            )

            # Verify API response structure
            assert "job_id" in result
            assert "job_name" in result
            assert "region" in result
            assert "status" in result
            assert "console_url" in result
            assert "parameters" in result

            # Verify API response values
            assert result["status"] == "running"
            assert result["region"] == region
            assert "console.cloud.google.com" in result["console_url"]


class TestEndToEndWorkflow:
    """Functional tests for complete end-to-end workflows"""

    @pytest.mark.asyncio
    async def test_complete_pipeline_deployment_workflow(self, monkeypatch):
        """Test complete workflow from job start to verification"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"

        # Track all steps executed
        workflow_log = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "run" in command:
                workflow_log.append("job_started")
                mock_result = Mock()
                mock_result.stdout = json.dumps({
                    "id": "2025-01-06_12_34_56-1234567890"
                })
                return mock_result
            elif "describe" in command:
                workflow_log.append("status_checked")
                mock_result = Mock()
                mock_result.stdout = json.dumps({
                    "id": "2025-01-06_12_34_56-1234567890",
                    "currentState": "JOB_STATE_RUNNING"
                })
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            # Step 1: Start job
            start_result = await dataflow_service.start_dataflow_job(
                project_id, dataset_name, region
            )

            # Step 2: Verify job is running
            verify_result = await dataflow_service.verify_job_running(
                project_id, start_result["job_id"], region, max_wait_seconds=5
            )

            # Verify complete workflow
            assert "job_started" in workflow_log
            assert "status_checked" in workflow_log
            assert verify_result is True

    @pytest.mark.asyncio
    async def test_job_lifecycle_management(self, monkeypatch):
        """Test complete job lifecycle: start -> monitor -> stop"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        workflow_log = []

        def mock_subprocess_run(*args, **kwargs):
            command = args[0]
            if "run" in command:
                workflow_log.append("started")
                mock_result = Mock()
                mock_result.stdout = json.dumps({"id": "job-123"})
                return mock_result
            elif "describe" in command:
                workflow_log.append("monitored")
                mock_result = Mock()
                mock_result.stdout = json.dumps({
                    "id": "job-123",
                    "currentState": "JOB_STATE_RUNNING"
                })
                return mock_result
            elif "cancel" in command:
                workflow_log.append("stopped")
                mock_result = Mock()
                mock_result.stdout = ""
                return mock_result

        with patch('subprocess.run', side_effect=mock_subprocess_run):
            # Start job
            start_result = await dataflow_service.start_dataflow_job(project_id, dataset_name)
            job_id = start_result["job_id"]

            # Monitor job
            status_result = await dataflow_service.get_job_status(project_id, job_id)

            # Stop job
            stop_result = await dataflow_service.stop_dataflow_job(project_id, job_id)

            # Verify complete lifecycle
            assert "started" in workflow_log
            assert "monitored" in workflow_log
            assert "stopped" in workflow_log
            assert stop_result["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_multi_job_management(self):
        """Test managing multiple Dataflow jobs"""
        project_id = "test-project-123"

        mock_result = Mock()
        mock_result.stdout = json.dumps([
            {"id": "job-1", "name": "pipeline-1", "currentState": "JOB_STATE_RUNNING"},
            {"id": "job-2", "name": "pipeline-2", "currentState": "JOB_STATE_RUNNING"},
            {"id": "job-3", "name": "pipeline-3", "currentState": "JOB_STATE_DONE"}
        ])

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.list_dataflow_jobs(project_id)

            # Verify multi-job handling
            assert result["count"] == 3
            assert len(result["jobs"]) == 3


class TestELTPipelineReadiness:
    """Functional tests for ELT pipeline readiness"""

    @pytest.mark.asyncio
    async def test_pipeline_parameters_match_elt_architecture(self):
        """Test that pipeline parameters match ELT architecture requirements"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        mock_result = Mock()
        mock_result.stdout = json.dumps({"id": "job-123"})

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            params = result["parameters"]

            # Verify ELT pipeline parameters
            assert params["inputSubscription"].startswith("projects/")
            assert "/subscriptions/gemini-telemetry-sub" in params["inputSubscription"]
            assert params["outputTableSpec"].endswith(".gemini_raw_logs")
            assert "transform.js" in params["javascriptTextTransformGcsPath"]
            assert params["stagingLocation"].startswith("gs://")

    @pytest.mark.asyncio
    async def test_pipeline_connects_to_pubsub(self):
        """Test that pipeline is configured to read from Pub/Sub"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            # Verify Pub/Sub subscription is in parameters
            params_index = captured_command.index("--parameters")
            parameters = captured_command[params_index + 1]

            assert "inputSubscription=projects/" in parameters
            assert "/subscriptions/gemini-telemetry-sub" in parameters

    @pytest.mark.asyncio
    async def test_pipeline_uses_udf_for_transformation(self):
        """Test that pipeline is configured to use JavaScript UDF"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            # Verify UDF configuration
            params_index = captured_command.index("--parameters")
            parameters = captured_command[params_index + 1]

            assert "javascriptTextTransformGcsPath=gs://" in parameters
            assert "/transform.js" in parameters
            assert "javascriptTextTransformFunctionName=transform" in parameters

    @pytest.mark.asyncio
    async def test_pipeline_writes_to_bigquery_raw_table(self):
        """Test that pipeline writes to gemini_raw_logs table"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            # Verify BigQuery output table
            params_index = captured_command.index("--parameters")
            parameters = captured_command[params_index + 1]

            assert f"outputTableSpec={project_id}:{dataset_name}.gemini_raw_logs" in parameters


class TestRegionConfiguration:
    """Functional tests for region configuration"""

    @pytest.mark.asyncio
    async def test_custom_region_propagates_correctly(self):
        """Test that custom region is used throughout pipeline"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        custom_region = "europe-west1"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(
                project_id, dataset_name, custom_region
            )

            # Verify custom region in command
            assert "--region" in captured_command
            region_index = captured_command.index("--region")
            assert captured_command[region_index + 1] == custom_region

            # Verify custom region in result
            assert result["region"] == custom_region

            # Verify template location uses custom region
            template_index = captured_command.index("--gcs-location")
            template_location = captured_command[template_index + 1]
            assert f"dataflow-templates-{custom_region}" in template_location

    @pytest.mark.asyncio
    async def test_default_region_is_us_central1(self):
        """Test that default region is us-central1"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            # Verify default region
            region_index = captured_command.index("--region")
            assert captured_command[region_index + 1] == "us-central1"


class TestConsoleURLGeneration:
    """Functional tests for console URL generation"""

    @pytest.mark.asyncio
    async def test_console_url_structure(self):
        """Test that console URL is correctly formatted"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        region = "us-central1"

        mock_result = Mock()
        mock_result.stdout = json.dumps({
            "id": "2025-01-06_12_34_56-1234567890"
        })

        with patch('subprocess.run', return_value=mock_result):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name, region)

            console_url = result["console_url"]

            # Verify URL structure
            assert console_url.startswith("https://console.cloud.google.com/dataflow/jobs/")
            assert region in console_url
            assert "2025-01-06_12_34_56-1234567890" in console_url
            assert f"project={project_id}" in console_url

    @pytest.mark.asyncio
    async def test_console_url_with_different_regions(self):
        """Test console URL generation with different regions"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        regions = ["us-central1", "europe-west1", "asia-east1"]

        for region in regions:
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})

            with patch('subprocess.run', return_value=mock_result):
                result = await dataflow_service.start_dataflow_job(
                    project_id, dataset_name, region
                )

                console_url = result["console_url"]
                assert region in console_url


class TestJobNaming:
    """Functional tests for job naming conventions"""

    @pytest.mark.asyncio
    async def test_default_job_name(self):
        """Test that default job name is gemini-telemetry-pipeline"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(project_id, dataset_name)

            # Verify default job name in command
            assert "gemini-telemetry-pipeline" in captured_command
            assert result["job_name"] == "gemini-telemetry-pipeline"

    @pytest.mark.asyncio
    async def test_custom_job_name(self):
        """Test that custom job names are accepted"""
        project_id = "test-project-123"
        dataset_name = "gemini_cli_telemetry"
        custom_name = "custom-pipeline-name"

        captured_command = None

        def capture_command(*args, **kwargs):
            nonlocal captured_command
            captured_command = args[0]
            mock_result = Mock()
            mock_result.stdout = json.dumps({"id": "job-123"})
            return mock_result

        with patch('subprocess.run', side_effect=capture_command):
            result = await dataflow_service.start_dataflow_job(
                project_id, dataset_name, job_name=custom_name
            )

            # Verify custom job name
            assert custom_name in captured_command
            assert result["job_name"] == custom_name
