"""
Comprehensive unit tests for remaining services
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import uuid
from google.cloud.exceptions import NotFound, Conflict


# ============================================================================
# TELEMETRY SERVICE TESTS
# ============================================================================

class TestTelemetryService:
    """Test telemetry_service.py"""

    @pytest.mark.asyncio
    async def test_configure_telemetry_success(self):
        """Test successful telemetry configuration"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="", stderr=""),  # Enable telemetry
            Mock(returncode=0, stdout="", stderr=""),  # Set log_prompts
        ]), patch('services.telemetry_service.get_telemetry_config', return_value={"enabled": True, "log_prompts": True}):

            result = await telemetry_service.configure_telemetry(True)

            assert result["enabled"] is True
            assert result["log_prompts"] is True

    @pytest.mark.asyncio
    async def test_configure_telemetry_failure(self):
        """Test telemetry configuration failure"""
        from services import telemetry_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            with pytest.raises(Exception, match="Failed to enable telemetry"):
                await telemetry_service.configure_telemetry(False)

    @pytest.mark.asyncio
    async def test_get_telemetry_config(self):
        """Test getting telemetry config"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="true\n", stderr=""),
            Mock(returncode=0, stdout="false\n", stderr=""),
        ]):
            config = await telemetry_service.get_telemetry_config()

            assert config["enabled"] is True
            assert config["log_prompts"] is False

    @pytest.mark.asyncio
    async def test_verify_telemetry_enabled(self):
        """Test telemetry verification"""
        from services import telemetry_service

        with patch('services.telemetry_service.get_telemetry_config', return_value={"enabled": True}):
            result = await telemetry_service.verify_telemetry_enabled()
            assert result is True


# ============================================================================
# BIGQUERY SERVICE TESTS
# ============================================================================

class TestBigQueryService:
    """Test bigquery_service.py"""

    @pytest.mark.asyncio
    async def test_create_dataset_success(self, mock_bigquery_client):
        """Test successful dataset creation"""
        from services import bigquery_service

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client), \
             patch('services.bigquery_service.create_telemetry_table'):

            result = await bigquery_service.create_dataset("test-project", "test_dataset", "us-central1")

            assert "dataset" in result
            assert "test_dataset" in result["dataset"]
            assert result["location"] == "us-central1"

    @pytest.mark.asyncio
    async def test_create_dataset_already_exists(self, mock_bigquery_client):
        """Test when dataset already exists"""
        from services import bigquery_service

        mock_bigquery_client.create_dataset.side_effect = Conflict("Already exists")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client), \
             patch('services.bigquery_service.create_telemetry_table'):

            result = await bigquery_service.create_dataset("test-project", "test_dataset", "us-central1")
            assert "dataset" in result

    @pytest.mark.asyncio
    async def test_create_telemetry_table(self, mock_bigquery_client):
        """Test telemetry table creation"""
        from services import bigquery_service

        await bigquery_service.create_telemetry_table(mock_bigquery_client, "test-project", "test_dataset")

        mock_bigquery_client.create_table.assert_called_once()

    @pytest.mark.asyncio
    async def test_verify_dataset_exists_true(self, mock_bigquery_client):
        """Test dataset exists verification"""
        from services import bigquery_service

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_dataset_exists("test-project", "test_dataset")
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_dataset_exists_false(self, mock_bigquery_client):
        """Test dataset not found"""
        from services import bigquery_service

        mock_bigquery_client.get_dataset.side_effect = NotFound("Not found")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_dataset_exists("test-project", "test_dataset")
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_table_exists_true(self, mock_bigquery_client):
        """Test table exists verification"""
        from services import bigquery_service

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_table_exists("test-project", "test_dataset")
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_table_exists_false(self, mock_bigquery_client):
        """Test table not found"""
        from services import bigquery_service

        mock_bigquery_client.get_table.side_effect = NotFound("Not found")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_table_exists("test-project", "test_dataset")
            assert result is False


# ============================================================================
# LOGGING SERVICE TESTS
# ============================================================================

class TestLoggingService:
    """Test logging_service.py"""

    @pytest.mark.asyncio
    async def test_test_logging_success(self, mock_logging_client, mock_asyncio_sleep):
        """Test successful logging test"""
        from services import logging_service

        with patch('google.cloud.logging.Client', return_value=mock_logging_client), \
             patch('services.logging_service.verify_test_log', return_value=True):

            result = await logging_service.test_logging("test-project")

            assert "test_uuid" in result
            assert result["log_written"] is True
            assert result["log_verified"] is True

    @pytest.mark.asyncio
    async def test_verify_test_log_found(self, mock_logging_client):
        """Test log verification when log is found"""
        from services import logging_service

        test_uuid = str(uuid.uuid4())
        mock_entry = Mock()
        mock_entry.payload = {"test_uuid": test_uuid}

        mock_logging_client.list_entries.return_value = iter([mock_entry])

        result = await logging_service.verify_test_log(mock_logging_client, test_uuid)
        assert result is True

    @pytest.mark.asyncio
    async def test_verify_test_log_not_found(self, mock_logging_client):
        """Test log verification when log is not found"""
        from services import logging_service

        mock_logging_client.list_entries.return_value = iter([])

        result = await logging_service.verify_test_log(mock_logging_client, str(uuid.uuid4()))
        assert result is False

    @pytest.mark.asyncio
    async def test_gemini_cli_logging(self, mock_logging_client, mock_asyncio_sleep):
        """Test Gemini CLI logging check"""
        from services import logging_service

        mock_logging_client.list_entries.return_value = iter([Mock(), Mock()])

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")), \
             patch('google.cloud.logging.Client', return_value=mock_logging_client):

            result = await logging_service.test_gemini_cli_logging("test-project")

            assert result["gemini_command_run"] is True
            assert result["logs_found"] is True
            assert result["log_count"] == 2


# ============================================================================
# SINK SERVICE TESTS
# ============================================================================

class TestSinkService:
    """Test sink_service.py"""

    @pytest.mark.asyncio
    async def test_create_sink_success(self, mock_asyncio_sleep):
        """Test successful sink creation"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")), \
             patch('services.sink_service.get_sink_service_account', return_value="serviceAccount:sink@project.iam.gserviceaccount.com"), \
             patch('services.sink_service.grant_sink_permissions'):

            result = await sink_service.create_sink("test-project", "test_dataset")

            assert result["sink_name"] == "gemini-cli-to-bigquery"
            assert "bigquery.googleapis.com" in result["destination"]

    @pytest.mark.asyncio
    async def test_create_sink_already_exists(self, mock_asyncio_sleep):
        """Test sink creation when already exists"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="already exists")), \
             patch('services.sink_service.update_sink'), \
             patch('services.sink_service.get_sink_service_account', return_value=""), \
             patch('services.sink_service.grant_sink_permissions'):

            result = await sink_service.create_sink("test-project", "test_dataset")
            assert result["sink_name"] == "gemini-cli-to-bigquery"

    @pytest.mark.asyncio
    async def test_get_sink_service_account(self):
        """Test getting sink service account"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="serviceAccount:sink@project.iam.gserviceaccount.com\n",
            stderr=""
        )):
            account = await sink_service.get_sink_service_account("test-project", "test-sink")
            assert "sink@project" in account

    @pytest.mark.asyncio
    async def test_grant_sink_permissions(self, mock_asyncio_sleep):
        """Test granting sink permissions"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")):
            await sink_service.grant_sink_permissions("test-project", "test_dataset", "serviceAccount:test@project.iam")
            # Should complete without error

    @pytest.mark.asyncio
    async def test_verify_sink_success(self):
        """Test sink verification"""
        from services import sink_service

        sink_info = {
            "destination": "bigquery.googleapis.com/projects/test/datasets/test_dataset",
            "writerIdentity": "serviceAccount:test@project.iam",
            "filter": 'logName:"gemini-cli"'
        }

        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout=str(sink_info).replace("'", '"'),
            stderr=""
        )), patch('json.loads', return_value=sink_info):

            result = await sink_service.verify_sink("test-project", "test-sink")
            assert result["verified"] is True

    @pytest.mark.asyncio
    async def test_list_sinks(self):
        """Test listing sinks"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="sink1\nsink2\n",
            stderr=""
        )):
            sinks = await sink_service.list_sinks("test-project")
            assert len(sinks) == 2


# ============================================================================
# VERIFICATION SERVICE TESTS
# ============================================================================

class TestVerificationService:
    """Test verification_service.py"""

    @pytest.mark.asyncio
    async def test_verify_end_to_end_success(self, mock_asyncio_sleep):
        """Test successful E2E verification"""
        from services import verification_service

        with patch('services.verification_service.run_gemini_test_command', return_value=True), \
             patch('services.verification_service.check_logs_in_cloud_logging', return_value=True), \
             patch('services.verification_service.check_data_in_bigquery', return_value=True):

            result = await verification_service.verify_end_to_end("test-project", "test_dataset")

            assert result["success"] is True
            assert result["logs_in_cloud_logging"] is True
            assert result["data_in_bigquery"] is True

    @pytest.mark.asyncio
    async def test_run_gemini_test_command(self):
        """Test running Gemini test command"""
        from services import verification_service

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")):
            result = await verification_service.run_gemini_test_command()
            assert result is True

    @pytest.mark.asyncio
    async def test_check_logs_in_cloud_logging(self, mock_logging_client):
        """Test checking logs in Cloud Logging"""
        from services import verification_service

        mock_logging_client.list_entries.return_value = iter([Mock()])

        with patch('google.cloud.logging.Client', return_value=mock_logging_client):
            result = await verification_service.check_logs_in_cloud_logging("test-project")
            assert result is True

    @pytest.mark.asyncio
    async def test_check_data_in_bigquery(self, mock_bigquery_client):
        """Test checking data in BigQuery"""
        from services import verification_service

        mock_row = Mock()
        mock_row.row_count = 5
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await verification_service.check_data_in_bigquery("test-project", "test_dataset")
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_complete_setup(self):
        """Test complete setup verification"""
        from services import verification_service

        with patch('services.bigquery_service.verify_dataset_exists', return_value=True), \
             patch('services.bigquery_service.verify_table_exists', return_value=True), \
             patch('services.sink_service.verify_sink', return_value={"verified": True}), \
             patch('services.telemetry_service.verify_telemetry_enabled', return_value=True):

            result = await verification_service.verify_complete_setup("test-project", "test_dataset")

            assert result["all_verified"] is True


# ============================================================================
# DEPLOYMENT SERVICE TESTS
# ============================================================================

class TestDeploymentService:
    """Test deployment_service.py"""

    @pytest.mark.asyncio
    async def test_create_deployment(self):
        """Test deployment creation"""
        from services import deployment_service

        config = {"projectId": "test", "region": "us-central1"}
        deployment_id = await deployment_service.create_deployment(config)

        assert deployment_id is not None
        assert len(deployment_id) == 36  # UUID length

    @pytest.mark.asyncio
    async def test_get_status(self):
        """Test getting deployment status"""
        from services import deployment_service

        config = {"projectId": "test"}
        deployment_id = await deployment_service.create_deployment(config)

        status = await deployment_service.get_status(deployment_id)
        assert status["deploymentId"] == deployment_id
        assert status["status"] == "idle"

    @pytest.mark.asyncio
    async def test_get_status_not_found(self):
        """Test getting status for non-existent deployment"""
        from services import deployment_service

        with pytest.raises(Exception, match="not found"):
            await deployment_service.get_status("non-existent-id")

    @pytest.mark.asyncio
    async def test_update_deployment_status(self):
        """Test updating deployment status"""
        from services import deployment_service

        config = {"projectId": "test"}
        deployment_id = await deployment_service.create_deployment(config)

        await deployment_service.update_deployment_status(deployment_id, "deploying", 1)

        status = await deployment_service.get_status(deployment_id)
        assert status["status"] == "deploying"
        assert status["currentStep"] == 1

    @pytest.mark.asyncio
    async def test_update_step_status(self):
        """Test updating step status"""
        from services import deployment_service

        config = {"projectId": "test"}
        deployment_id = await deployment_service.create_deployment(config)

        await deployment_service.update_step_status(deployment_id, 0, "completed", "Success")

        status = await deployment_service.get_status(deployment_id)
        assert status["steps"][0]["status"] == "completed"
        assert status["steps"][0]["details"] == "Success"

    @pytest.mark.asyncio
    async def test_add_created_resource(self):
        """Test adding created resource"""
        from services import deployment_service

        config = {"projectId": "test"}
        deployment_id = await deployment_service.create_deployment(config)

        await deployment_service.add_created_resource(deployment_id, "dataset", "test_dataset")

        status = await deployment_service.get_status(deployment_id)
        assert status["createdResources"]["dataset"] == "test_dataset"

    @pytest.mark.asyncio
    async def test_list_deployments(self):
        """Test listing deployments"""
        from services import deployment_service

        await deployment_service.create_deployment({"projectId": "test1"})
        await deployment_service.create_deployment({"projectId": "test2"})

        deployments = await deployment_service.list_deployments()
        assert len(deployments) >= 2

    @pytest.mark.asyncio
    async def test_delete_deployment(self):
        """Test deleting deployment"""
        from services import deployment_service

        config = {"projectId": "test"}
        deployment_id = await deployment_service.create_deployment(config)

        result = await deployment_service.delete_deployment(deployment_id)
        assert result is True

        with pytest.raises(Exception):
            await deployment_service.get_status(deployment_id)
