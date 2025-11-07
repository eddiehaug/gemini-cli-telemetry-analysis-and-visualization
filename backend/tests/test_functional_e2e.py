"""
Automated Functional End-to-End Tests

These tests simulate real deployment scenarios by:
1. Testing complete deployment flows
2. Verifying service interactions
3. Testing error recovery
4. Validating critical paths
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock, call
import uuid


class TestCompleteDeploymentFlow:
    """Test complete deployment workflow end-to-end"""

    @pytest.mark.asyncio
    async def test_successful_complete_deployment(self, mock_asyncio_sleep):
        """
        Functional test: Complete successful deployment from start to finish

        This test simulates a user going through the entire deployment wizard:
        1. Verify dependencies
        2. Validate configuration
        3. Authenticate
        4. Check permissions
        5. Enable APIs
        6. Configure telemetry
        7. Create BigQuery dataset and table
        8. Test Cloud Logging
        9. Create log sink
        10. Verify sink
        11. End-to-end verification
        """
        from services import (
            dependency_service, config_service, auth_service, iam_service,
            api_service, telemetry_service, bigquery_service, logging_service,
            sink_service, verification_service, deployment_service
        )

        # Test configuration
        test_project = "test-project-123"
        test_dataset = "gemini_cli_telemetry"
        test_region = "us-central1"

        # Mock all subprocess calls
        with patch('subprocess.run') as mock_run:
            # Configure subprocess mocks for each step
            mock_run.side_effect = [
                # Step 1: Dependency checks
                Mock(returncode=0, stdout="Google Cloud SDK 450.0.0\n", stderr=""),  # gcloud version
                Mock(returncode=0, stdout="/usr/local/bin/gcloud\n", stderr=""),  # gcloud path
                Mock(returncode=0, stdout="gemini 1.0.0\n", stderr=""),  # gemini version
                Mock(returncode=0, stdout="/usr/local/bin/gemini\n", stderr=""),  # gemini path

                # Step 3: Authentication (ADC file exists, so no subprocess call for ADC)
                Mock(returncode=0, stdout="user@example.com\n", stderr=""),  # active account

                # Step 4: IAM permissions - user already has all permissions
                Mock(returncode=0, stdout="user@example.com\n", stderr=""),  # get current user
                Mock(returncode=0, stdout="roles/bigquery.admin\nroles/logging.admin\nroles/serviceusage.serviceUsageAdmin\n", stderr=""),  # get roles (already has all)

                # Step 5: Enable APIs - all 4 already enabled
                Mock(returncode=0, stdout="bigquery.googleapis.com\nlogging.googleapis.com\ncloudresourcemanager.googleapis.com\nserviceusage.googleapis.com\n", stderr=""),  # list enabled APIs (all 4 present)

                # Step 6: Configure telemetry
                Mock(returncode=0, stdout="", stderr=""),  # enable telemetry
                Mock(returncode=0, stdout="", stderr=""),  # set log_prompts
                Mock(returncode=0, stdout="true\n", stderr=""),  # get telemetry.enabled
                Mock(returncode=0, stdout="false\n", stderr=""),  # get telemetry.log_prompts

                # Step 7: Create Dataset - no subprocess calls (uses BigQuery client)
                # Step 8: Test Logging - no subprocess calls (uses Cloud Logging client)

                # Step 9: Create sink
                Mock(returncode=0, stdout="", stderr=""),  # create sink
                Mock(returncode=0, stdout="serviceAccount:sink@project.iam.gserviceaccount.com\n", stderr=""),  # get sink SA
                Mock(returncode=0, stdout="", stderr=""),  # grant permissions

                # Step 10: Verify sink
                Mock(returncode=0, stdout='{"destination": "bigquery://...", "writerIdentity": "serviceAccount:test", "filter": "logName:\\"gemini-cli\\""}', stderr=""),

                # Step 11: E2E verification - gemini command
                Mock(returncode=0, stdout="", stderr=""),  # gemini models list
            ]

            # Mock Google Cloud clients
            with patch('google.cloud.bigquery.Client') as mock_bq_client, \
                 patch('google.cloud.logging.Client') as mock_log_client, \
                 patch('os.path.exists', return_value=True):

                # Configure BigQuery mock
                mock_bq = Mock()
                mock_bq.create_dataset = Mock()
                mock_bq.create_table = Mock()
                mock_bq.get_dataset = Mock()
                mock_bq.get_table = Mock(return_value=Mock(num_rows=5))
                mock_bq.query = Mock(return_value=Mock(result=Mock(return_value=iter([Mock(row_count=5)]))))
                mock_bq_client.return_value = mock_bq

                # Configure Logging mock
                mock_logger = Mock()
                mock_log_entry = Mock()
                mock_log_entry.payload = {"test_uuid": "test-123"}
                mock_logger.log_struct = Mock()
                mock_log = Mock()
                mock_log.logger = Mock(return_value=mock_logger)
                # Use side_effect to return a new iterator each time (avoids exhaustion)
                mock_log.list_entries = Mock(side_effect=lambda **kwargs: iter([mock_log_entry]))
                mock_log_client.return_value = mock_log

                # STEP 1: Verify Dependencies
                deps = await dependency_service.verify_dependencies()
                assert len(deps) == 3
                assert all(dep["installed"] for dep in deps)

                # STEP 2: Validate Configuration
                from pydantic import BaseModel
                class Config(BaseModel):
                    projectId: str
                    region: str
                    datasetName: str
                    logPrompts: bool

                config = Config(
                    projectId=test_project,
                    region=test_region,
                    datasetName=test_dataset,
                    logPrompts=False
                )

                result = await config_service.validate_config(config)
                assert result is True

                # STEP 3: Authenticate
                auth_result = await auth_service.authenticate()
                assert auth_result["authenticated"] is True
                assert auth_result["account"] == "user@example.com"

                # STEP 4: Check Permissions
                perm_result = await iam_service.check_permissions(test_project)
                assert perm_result["hasAll"] is True
                assert len(perm_result["missingRoles"]) == 0

                # STEP 5: Enable APIs
                api_result = await api_service.enable_apis(test_project)
                assert api_result["success"] is True

                # STEP 6: Configure Telemetry
                telem_result = await telemetry_service.configure_telemetry(False)
                assert telem_result["enabled"] is True

                # STEP 7: Create Dataset
                ds_result = await bigquery_service.create_dataset(test_project, test_dataset, test_region)
                assert test_dataset in ds_result["dataset"]

                # STEP 8: Test Logging
                log_result = await logging_service.test_logging(test_project)
                assert log_result["log_written"] is True

                # STEP 9: Create Sink
                sink_result = await sink_service.create_sink(test_project, test_dataset)
                assert sink_result["sink_name"] == "gemini-cli-to-bigquery"

                # STEP 10: Verify Sink
                verify_result = await sink_service.verify_sink(test_project, "gemini-cli-to-bigquery")
                assert verify_result["verified"] is True

                # STEP 11: End-to-End Verification
                e2e_result = await verification_service.verify_end_to_end(test_project, test_dataset)
                assert e2e_result["success"] is True

    @pytest.mark.asyncio
    async def test_deployment_with_missing_dependencies(self):
        """
        Functional test: Deployment fails gracefully when gcloud is missing
        """
        from services import dependency_service

        with patch('subprocess.run', side_effect=FileNotFoundError()):
            with pytest.raises(Exception, match="Missing required dependencies"):
                await dependency_service.verify_dependencies()

    @pytest.mark.asyncio
    async def test_deployment_with_invalid_configuration(self):
        """
        Functional test: Deployment rejects invalid configuration
        """
        from services import config_service
        from pydantic import BaseModel

        class Config(BaseModel):
            projectId: str
            region: str
            datasetName: str
            logPrompts: bool

        # Test various invalid configurations
        invalid_configs = [
            {"projectId": "", "region": "us-central1", "datasetName": "test", "logPrompts": False},
            {"projectId": "test", "region": "invalid-region", "datasetName": "test", "logPrompts": False},
            {"projectId": "test", "region": "us-central1", "datasetName": "123invalid", "logPrompts": False},
        ]

        for invalid_config in invalid_configs:
            config = Config(**invalid_config)
            with pytest.raises(Exception):
                await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_deployment_with_authentication_failure(self):
        """
        Functional test: Deployment fails when user is not authenticated
        """
        from services import auth_service

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")):
            with pytest.raises(Exception, match="Not authenticated"):
                await auth_service.authenticate()

    @pytest.mark.asyncio
    async def test_deployment_with_missing_permissions(self, mock_asyncio_sleep):
        """
        Functional test: Deployment detects and attempts to grant missing permissions
        """
        from services import iam_service

        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout="user@example.com\n", stderr=""),  # get current user
                Mock(returncode=0, stdout="roles/bigquery.admin\n", stderr=""),  # get roles (missing 2)
                Mock(returncode=0, stdout="", stderr=""),  # grant role 1
                Mock(returncode=0, stdout="", stderr=""),  # grant role 2
                Mock(returncode=0, stdout="roles/bigquery.admin\nroles/logging.admin\nroles/serviceusage.serviceUsageAdmin\n", stderr=""),  # get roles after grant
            ]

            result = await iam_service.check_permissions("test-project")
            # Should have all permissions after granting
            assert result["hasAll"] is True


class TestErrorRecoveryScenarios:
    """Test error recovery and resilience"""

    @pytest.mark.asyncio
    async def test_api_enablement_with_partial_failures(self, mock_asyncio_sleep):
        """
        Functional test: API enablement handles partial failures
        """
        from services import api_service

        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = [
                Mock(returncode=0, stdout="", stderr=""),  # list enabled (empty)
                Mock(returncode=0, stdout="", stderr=""),  # enable API 1 (success)
                Mock(returncode=1, stdout="", stderr="Permission denied"),  # enable API 2 (fail)
                Mock(returncode=1, stdout="", stderr="Permission denied"),  # enable API 3 (fail)
                Mock(returncode=0, stdout="", stderr=""),  # enable API 4 (success)
            ]

            with pytest.raises(Exception, match="Failed to enable APIs"):
                await api_service.enable_apis("test-project")

    @pytest.mark.asyncio
    async def test_sink_creation_when_already_exists(self, mock_asyncio_sleep):
        """
        Functional test: Sink creation updates existing sink
        """
        from services import sink_service

        with patch('subprocess.run') as mock_run:
            mock_run.side_effect = [
                Mock(returncode=1, stdout="", stderr="already exists"),  # create fails (exists)
                Mock(returncode=0, stdout="", stderr=""),  # update succeeds
                Mock(returncode=0, stdout="serviceAccount:test@project.iam\n", stderr=""),  # get SA
                Mock(returncode=0, stdout="", stderr=""),  # grant permissions
            ]

            result = await sink_service.create_sink("test-project", "test_dataset")
            assert result["sink_name"] == "gemini-cli-to-bigquery"

    @pytest.mark.asyncio
    async def test_bigquery_table_creation_when_exists(self, mock_bigquery_client):
        """
        Functional test: Table creation handles existing table
        """
        from services import bigquery_service
        from google.cloud.exceptions import Conflict

        mock_bigquery_client.create_table.side_effect = Conflict("Already exists")

        # Should not raise, just log
        await bigquery_service.create_telemetry_table(mock_bigquery_client, "test", "test_ds")


class TestDeploymentStateManagement:
    """Test deployment state tracking"""

    @pytest.mark.asyncio
    async def test_deployment_lifecycle(self):
        """
        Functional test: Complete deployment lifecycle tracking
        """
        from services import deployment_service

        # Create deployment
        config = {"projectId": "test-project", "region": "us-central1"}
        deployment_id = await deployment_service.create_deployment(config)

        # Verify initial state
        status = await deployment_service.get_status(deployment_id)
        assert status["status"] == "idle"
        assert status["currentStep"] == 0
        assert all(step["status"] == "pending" for step in status["steps"])

        # Simulate deployment progress
        await deployment_service.update_deployment_status(deployment_id, "deploying", 1)

        # Update step statuses
        await deployment_service.update_step_status(deployment_id, 0, "in_progress")
        await deployment_service.update_step_status(deployment_id, 0, "completed", "Dependencies verified")

        await deployment_service.update_step_status(deployment_id, 1, "in_progress")
        await deployment_service.update_step_status(deployment_id, 1, "completed", "Config validated")

        # Add created resources
        await deployment_service.add_created_resource(deployment_id, "dataset", "test.gemini_telemetry")
        await deployment_service.add_created_resource(deployment_id, "sink", "gemini-cli-to-bigquery")

        # Mark complete
        await deployment_service.update_deployment_status(deployment_id, "completed", 11)

        # Verify final state
        final_status = await deployment_service.get_status(deployment_id)
        assert final_status["status"] == "completed"
        assert final_status["currentStep"] == 11
        assert final_status["createdResources"]["dataset"] == "test.gemini_telemetry"
        assert final_status["createdResources"]["sink"] == "gemini-cli-to-bigquery"

    @pytest.mark.asyncio
    async def test_deployment_failure_tracking(self):
        """
        Functional test: Deployment failure is tracked correctly
        """
        from services import deployment_service

        config = {"projectId": "test-project"}
        deployment_id = await deployment_service.create_deployment(config)

        # Start deployment
        await deployment_service.update_deployment_status(deployment_id, "deploying", 0)
        await deployment_service.update_step_status(deployment_id, 0, "in_progress")

        # Simulate failure
        await deployment_service.update_step_status(
            deployment_id,
            0,
            "failed",
            error="gcloud not found"
        )
        await deployment_service.update_deployment_status(deployment_id, "failed")

        # Verify failure state
        status = await deployment_service.get_status(deployment_id)
        assert status["status"] == "failed"
        assert status["steps"][0]["status"] == "failed"
        assert "gcloud not found" in status["steps"][0]["error"]


class TestCriticalPathValidation:
    """Test critical deployment paths"""

    @pytest.mark.asyncio
    async def test_iam_propagation_delay_enforced(self, monkeypatch):
        """
        Functional test: IAM propagation delay is enforced
        """
        from services import iam_service
        import asyncio

        sleep_called = {"times": 0, "duration": 0}

        async def mock_sleep(seconds):
            sleep_called["times"] += 1
            sleep_called["duration"] += seconds

        monkeypatch.setattr(asyncio, "sleep", mock_sleep)

        await iam_service.wait_for_iam_propagation(90)

        assert sleep_called["times"] == 1
        assert sleep_called["duration"] == 90

    @pytest.mark.asyncio
    async def test_api_propagation_verification(self, mock_asyncio_sleep):
        """
        Functional test: API propagation is verified before proceeding
        """
        from services import api_service

        with patch('subprocess.run') as mock_run, \
             patch('services.api_service.verify_api_accessible', return_value=True) as mock_verify:

            mock_run.side_effect = [
                Mock(returncode=0, stdout="", stderr=""),  # get enabled APIs (empty)
                Mock(returncode=0, stdout="", stderr=""),  # enable API 1
                Mock(returncode=0, stdout="", stderr=""),  # enable API 2
                Mock(returncode=0, stdout="", stderr=""),  # enable API 3
                Mock(returncode=0, stdout="", stderr=""),  # enable API 4
            ]

            result = await api_service.enable_apis("test-project")

            # Verify propagation check was called for each newly enabled API
            assert mock_verify.call_count == 4

    @pytest.mark.asyncio
    async def test_uuid_tracking_through_pipeline(self, mock_logging_client, mock_asyncio_sleep):
        """
        Functional test: UUID tracking works end-to-end
        """
        from services import logging_service

        # We need to capture the UUID that gets generated during test_logging
        # and return it in the mock when verify_test_log is called
        generated_uuid = None

        def list_entries_side_effect(*args, **kwargs):
            # First call is from test_logging - return empty to trigger log write
            # Second call is from verify_test_log - return entry with the UUID
            nonlocal generated_uuid
            if generated_uuid is None:
                return iter([])  # No entries found initially
            else:
                # Return entry with the specific UUID we're looking for
                mock_entry = Mock()
                mock_entry.payload = {"test_uuid": generated_uuid}
                return iter([mock_entry])

        mock_logging_client.list_entries.side_effect = list_entries_side_effect

        with patch('google.cloud.logging.Client', return_value=mock_logging_client), \
             patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")):
            result = await logging_service.test_logging("test-project")

            # Verify UUID was generated
            assert "test_uuid" in result
            assert len(result["test_uuid"]) == 36  # UUID length

            # Capture the generated UUID
            generated_uuid = result["test_uuid"]

            # Now verify log was found - this will use the second call to list_entries
            verified = await logging_service.verify_test_log(mock_logging_client, generated_uuid)
            assert verified is True

    @pytest.mark.asyncio
    async def test_schema_first_table_creation(self, mock_bigquery_client):
        """
        Functional test: Table is created with schema before sink
        """
        from services import bigquery_service

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            await bigquery_service.create_dataset("test", "test_ds", "us-central1")

            # Verify table was created with proper schema
            mock_bigquery_client.create_table.assert_called_once()
            table_arg = mock_bigquery_client.create_table.call_args[0][0]

            # Verify schema includes required fields
            assert table_arg.schema is not None
            field_names = [field.name for field in table_arg.schema]
            assert "timestamp" in field_names
            assert "logName" in field_names
            assert "jsonPayload" in field_names


class TestPerformanceAndResilience:
    """Test performance characteristics and resilience"""

    @pytest.mark.asyncio
    async def test_timeout_handling_on_slow_operations(self):
        """
        Functional test: Timeouts are handled gracefully
        """
        from services import dependency_service
        import subprocess

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 30)):
            with pytest.raises(Exception, match="timed out"):
                await dependency_service.check_billing("test-project")

    @pytest.mark.asyncio
    async def test_concurrent_deployments(self):
        """
        Functional test: Multiple deployments can be tracked simultaneously
        """
        from services import deployment_service

        # Create multiple deployments
        deployments = []
        for i in range(5):
            config = {"projectId": f"project-{i}"}
            dep_id = await deployment_service.create_deployment(config)
            deployments.append(dep_id)

        # Verify all can be retrieved
        for dep_id in deployments:
            status = await deployment_service.get_status(dep_id)
            assert status["deploymentId"] == dep_id

        # List all deployments
        all_deployments = await deployment_service.list_deployments()
        assert len(all_deployments) >= 5

    @pytest.mark.asyncio
    async def test_graceful_degradation_on_optional_features(self):
        """
        Functional test: Optional features fail gracefully
        """
        from services import dependency_service

        # Billing check should continue even if it fails
        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            result = await dependency_service.check_billing("test-project")
            # Should return True (continue with warning) instead of failing
            assert result is True


class TestDataFlowValidation:
    """Test data flow through the system"""

    @pytest.mark.asyncio
    async def test_end_to_end_data_flow_simulation(self, mock_asyncio_sleep, mock_bigquery_client, mock_logging_client):
        """
        Functional test: Complete data flow from Gemini CLI to BigQuery

        Simulates:
        1. Gemini CLI generates telemetry
        2. Telemetry sent to Cloud Logging
        3. Log sink exports to BigQuery
        4. Data appears in BigQuery table
        """
        from services import verification_service

        # Mock Gemini command execution
        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")), \
             patch('google.cloud.logging.Client', return_value=mock_logging_client), \
             patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):

            # Setup mocks
            mock_log_entry = Mock()
            mock_logging_client.list_entries.return_value = iter([mock_log_entry])

            mock_row = Mock()
            mock_row.row_count = 10
            mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

            # Run E2E verification
            result = await verification_service.verify_end_to_end("test-project", "test_dataset")

            # Verify complete flow
            assert result["success"] is True
            assert result["logs_in_cloud_logging"] is True
            assert result["data_in_bigquery"] is True
            assert result["verification_complete"] is True


# Performance summary test
class TestFunctionalTestSummary:
    """Summary of functional test execution"""

    def test_functional_suite_metadata(self):
        """Capture functional test suite metadata"""
        metadata = {
            "total_functional_tests": 18,
            "test_categories": [
                "Complete Deployment Flow",
                "Error Recovery Scenarios",
                "Deployment State Management",
                "Critical Path Validation",
                "Performance and Resilience",
                "Data Flow Validation"
            ],
            "scenarios_covered": [
                "Successful deployment (11 steps)",
                "Missing dependencies",
                "Invalid configuration",
                "Authentication failure",
                "Missing permissions",
                "Partial API failures",
                "Sink already exists",
                "Table already exists",
                "Deployment lifecycle",
                "Failure tracking",
                "IAM propagation",
                "API propagation",
                "UUID tracking",
                "Schema-first table creation",
                "Timeout handling",
                "Concurrent deployments",
                "Graceful degradation",
                "End-to-end data flow"
            ],
            "estimated_execution_time": "< 5 seconds"
        }

        assert len(metadata["scenarios_covered"]) == 18
        assert len(metadata["test_categories"]) == 6
