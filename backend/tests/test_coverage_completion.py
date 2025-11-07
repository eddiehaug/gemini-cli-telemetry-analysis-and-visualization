"""
Additional tests to achieve 100% coverage
Covers all missing lines from services
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import subprocess
from google.cloud.exceptions import NotFound, Conflict


# ============================================================================
# API SERVICE - Missing Lines
# ============================================================================

class TestApiServiceAdditional:
    """Cover missing lines in api_service.py"""

    @pytest.mark.asyncio
    async def test_verify_api_accessible_exception(self):
        """Test API verification with exception"""
        from services import api_service

        with patch('services.api_service.verify_bigquery_api', side_effect=Exception("Error")):
            result = await api_service.verify_api_accessible("test", "bigquery.googleapis.com")
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_bigquery_api_exception(self):
        """Test BigQuery verification exception"""
        from services import api_service

        with patch('subprocess.run', side_effect=Exception("Connection error")):
            result = await api_service.verify_bigquery_api("test")
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_logging_api_exception(self):
        """Test Logging API verification exception"""
        from services import api_service

        with patch('subprocess.run', side_effect=Exception("Connection error")):
            result = await api_service.verify_logging_api("test")
            assert result is False


# ============================================================================
# BIGQUERY SERVICE - Missing Lines
# ============================================================================

class TestBigQueryServiceAdditional:
    """Cover missing lines in bigquery_service.py"""

    @pytest.mark.asyncio
    async def test_create_dataset_exception(self, mock_bigquery_client):
        """Test dataset creation exception"""
        from services import bigquery_service

        mock_bigquery_client.create_dataset.side_effect = Exception("Test error")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            with pytest.raises(Exception, match="Test error"):
                await bigquery_service.create_dataset("test", "test_ds", "us-central1")

    @pytest.mark.asyncio
    async def test_create_telemetry_table_exception(self, mock_bigquery_client):
        """Test table creation exception"""
        from services import bigquery_service

        mock_bigquery_client.create_table.side_effect = Exception("Table error")

        with pytest.raises(Exception, match="Table error"):
            await bigquery_service.create_telemetry_table(mock_bigquery_client, "test", "test_ds")

    @pytest.mark.asyncio
    async def test_verify_dataset_exception(self, mock_bigquery_client):
        """Test dataset verification exception"""
        from services import bigquery_service

        mock_bigquery_client.get_dataset.side_effect = Exception("Connection error")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_dataset_exists("test", "test_ds")
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_table_exception(self, mock_bigquery_client):
        """Test table verification exception"""
        from services import bigquery_service

        mock_bigquery_client.get_table.side_effect = Exception("Connection error")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await bigquery_service.verify_table_exists("test", "test_ds")
            assert result is False


# ============================================================================
# DEPLOYMENT SERVICE - Missing Lines
# ============================================================================

class TestDeploymentServiceAdditional:
    """Cover missing lines in deployment_service.py"""

    @pytest.mark.asyncio
    async def test_update_deployment_status_not_found(self):
        """Test updating non-existent deployment"""
        from services import deployment_service

        with pytest.raises(Exception, match="not found"):
            await deployment_service.update_deployment_status("non-existent", "deploying")

    @pytest.mark.asyncio
    async def test_update_step_status_not_found(self):
        """Test updating step for non-existent deployment"""
        from services import deployment_service

        with pytest.raises(Exception, match="not found"):
            await deployment_service.update_step_status("non-existent", 0, "completed")

    @pytest.mark.asyncio
    async def test_update_step_invalid_index(self):
        """Test updating invalid step index"""
        from services import deployment_service

        deployment_id = await deployment_service.create_deployment({"projectId": "test"})

        with pytest.raises(Exception, match="Invalid step index"):
            await deployment_service.update_step_status(deployment_id, 999, "completed")

    @pytest.mark.asyncio
    async def test_add_resource_not_found(self):
        """Test adding resource to non-existent deployment"""
        from services import deployment_service

        with pytest.raises(Exception, match="not found"):
            await deployment_service.add_created_resource("non-existent", "dataset", "test")

    @pytest.mark.asyncio
    async def test_delete_deployment_not_found(self):
        """Test deleting non-existent deployment"""
        from services import deployment_service

        result = await deployment_service.delete_deployment("non-existent")
        assert result is False


# ============================================================================
# IAM SERVICE - Missing Lines
# ============================================================================

class TestIamServiceAdditional:
    """Cover missing lines in iam_service.py"""

    @pytest.mark.asyncio
    async def test_check_permissions_exception(self):
        """Test permissions check exception"""
        from services import iam_service

        with patch('services.iam_service.get_current_user', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await iam_service.check_permissions("test-project")

    @pytest.mark.asyncio
    async def test_get_user_roles_exception(self):
        """Test get user roles exception"""
        from services import iam_service

        with patch('subprocess.run', side_effect=Exception("Connection error")):
            roles = await iam_service.get_user_roles("test", "user@example.com")
            assert roles == []


# ============================================================================
# LOGGING SERVICE - Missing Lines
# ============================================================================

class TestLoggingServiceAdditional:
    """Cover missing lines in logging_service.py"""

    @pytest.mark.asyncio
    async def test_test_logging_exception(self, mock_logging_client):
        """Test logging test exception"""
        from services import logging_service

        with patch('google.cloud.logging.Client', side_effect=Exception("Connection error")):
            with pytest.raises(Exception):
                await logging_service.test_logging("test-project")

    @pytest.mark.asyncio
    async def test_verify_test_log_exception(self, mock_logging_client):
        """Test log verification exception"""
        from services import logging_service

        mock_logging_client.list_entries.side_effect = Exception("Error")

        result = await logging_service.verify_test_log(mock_logging_client, "test-uuid")
        assert result is False

    @pytest.mark.asyncio
    async def test_gemini_cli_logging_timeout(self):
        """Test Gemini CLI logging timeout"""
        from services import logging_service

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gemini", 30)):
            with pytest.raises(Exception, match="timed out"):
                await logging_service.test_gemini_cli_logging("test-project")

    @pytest.mark.asyncio
    async def test_gemini_cli_logging_exception(self):
        """Test Gemini CLI logging exception"""
        from services import logging_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await logging_service.test_gemini_cli_logging("test-project")


# ============================================================================
# SINK SERVICE - Missing Lines (Most gaps here)
# ============================================================================

class TestSinkServiceAdditional:
    """Cover missing lines in sink_service.py"""

    @pytest.mark.asyncio
    async def test_create_sink_timeout(self, mock_asyncio_sleep):
        """Test sink creation timeout"""
        from services import sink_service

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 60)):
            with pytest.raises(Exception, match="timed out"):
                await sink_service.create_sink("test", "test_ds")

    @pytest.mark.asyncio
    async def test_create_sink_exception(self, mock_asyncio_sleep):
        """Test sink creation exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await sink_service.create_sink("test", "test_ds")

    @pytest.mark.asyncio
    async def test_update_sink_success(self):
        """Test sink update success"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=0, stdout="", stderr="")):
            await sink_service.update_sink("test", "test-sink", "dest", "filter")

    @pytest.mark.asyncio
    async def test_update_sink_failure(self):
        """Test sink update failure"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            with pytest.raises(Exception, match="Failed to update"):
                await sink_service.update_sink("test", "test-sink", "dest", "filter")

    @pytest.mark.asyncio
    async def test_update_sink_exception(self):
        """Test sink update exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await sink_service.update_sink("test", "test-sink", "dest", "filter")

    @pytest.mark.asyncio
    async def test_get_sink_service_account_failure(self):
        """Test sink service account retrieval failure"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            account = await sink_service.get_sink_service_account("test", "test-sink")
            assert account == ""

    @pytest.mark.asyncio
    async def test_get_sink_service_account_exception(self):
        """Test sink service account exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            account = await sink_service.get_sink_service_account("test", "test-sink")
            assert account == ""

    @pytest.mark.asyncio
    async def test_grant_sink_permissions_failure(self, mock_asyncio_sleep):
        """Test granting permissions failure"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            # Should not raise, just log warning
            await sink_service.grant_sink_permissions("test", "test_ds", "serviceAccount:test@test.iam")

    @pytest.mark.asyncio
    async def test_grant_sink_permissions_exception(self, mock_asyncio_sleep):
        """Test granting permissions exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            # Should not raise, just log warning
            await sink_service.grant_sink_permissions("test", "test_ds", "test@test.iam")

    @pytest.mark.asyncio
    async def test_verify_sink_failure(self):
        """Test sink verification failure"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="not found")):
            with pytest.raises(Exception, match="not found"):
                await sink_service.verify_sink("test", "test-sink")

    @pytest.mark.asyncio
    async def test_verify_sink_exception(self):
        """Test sink verification exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await sink_service.verify_sink("test", "test-sink")

    @pytest.mark.asyncio
    async def test_list_sinks_failure(self):
        """Test list sinks failure"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            sinks = await sink_service.list_sinks("test")
            assert sinks == []

    @pytest.mark.asyncio
    async def test_list_sinks_exception(self):
        """Test list sinks exception"""
        from services import sink_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            sinks = await sink_service.list_sinks("test")
            assert sinks == []


# ============================================================================
# TELEMETRY SERVICE - Missing Lines
# ============================================================================

class TestTelemetryServiceAdditional:
    """Cover missing lines in telemetry_service.py"""

    @pytest.mark.asyncio
    async def test_configure_telemetry_log_prompts_failure(self):
        """Test telemetry log_prompts config failure"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="", stderr=""),  # Enable telemetry succeeds
            Mock(returncode=1, stdout="", stderr="error"),  # Set log_prompts fails
        ]):
            with pytest.raises(Exception, match="Failed to configure log_prompts"):
                await telemetry_service.configure_telemetry(True)

    @pytest.mark.asyncio
    async def test_configure_telemetry_timeout(self):
        """Test telemetry configuration timeout"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gemini", 30)):
            with pytest.raises(Exception, match="timed out"):
                await telemetry_service.configure_telemetry(False)

    @pytest.mark.asyncio
    async def test_configure_telemetry_exception(self):
        """Test telemetry configuration exception"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            with pytest.raises(Exception):
                await telemetry_service.configure_telemetry(False)

    @pytest.mark.asyncio
    async def test_get_telemetry_config_exception(self):
        """Test get config exception"""
        from services import telemetry_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            config = await telemetry_service.get_telemetry_config()
            assert isinstance(config, dict)

    @pytest.mark.asyncio
    async def test_verify_telemetry_enabled_exception(self):
        """Test verify telemetry exception"""
        from services import telemetry_service

        with patch('services.telemetry_service.get_telemetry_config', side_effect=Exception("Error")):
            result = await telemetry_service.verify_telemetry_enabled()
            assert result is False


# ============================================================================
# VERIFICATION SERVICE - Missing Lines (Most gaps here)
# ============================================================================

class TestVerificationServiceAdditional:
    """Cover missing lines in verification_service.py"""

    @pytest.mark.asyncio
    async def test_verify_end_to_end_exception(self):
        """Test E2E verification exception"""
        from services import verification_service

        with patch('services.verification_service.run_gemini_test_command', side_effect=Exception("Error")):
            with pytest.raises(Exception):
                await verification_service.verify_end_to_end("test", "test_ds")

    @pytest.mark.asyncio
    async def test_run_gemini_test_command_timeout(self):
        """Test Gemini command timeout"""
        from services import verification_service

        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gemini", 30)):
            result = await verification_service.run_gemini_test_command()
            assert result is False

    @pytest.mark.asyncio
    async def test_run_gemini_test_command_exception(self):
        """Test Gemini command exception"""
        from services import verification_service

        with patch('subprocess.run', side_effect=Exception("Test error")):
            result = await verification_service.run_gemini_test_command()
            assert result is False

    @pytest.mark.asyncio
    async def test_check_logs_no_logs_found(self, mock_logging_client):
        """Test checking logs when none found"""
        from services import verification_service

        mock_logging_client.list_entries.return_value = iter([])

        with patch('google.cloud.logging.Client', return_value=mock_logging_client):
            result = await verification_service.check_logs_in_cloud_logging("test")
            assert result is False

    @pytest.mark.asyncio
    async def test_check_logs_exception(self, mock_logging_client):
        """Test check logs exception"""
        from services import verification_service

        with patch('google.cloud.logging.Client', side_effect=Exception("Error")):
            result = await verification_service.check_logs_in_cloud_logging("test")
            assert result is False

    @pytest.mark.asyncio
    async def test_check_data_bigquery_no_data(self, mock_bigquery_client):
        """Test BigQuery check with no data"""
        from services import verification_service

        mock_row = Mock()
        mock_row.row_count = 0
        mock_bigquery_client.query.return_value.result.return_value = iter([mock_row])
        mock_bigquery_client.get_table.return_value = Mock(num_rows=0)

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await verification_service.check_data_in_bigquery("test", "test_ds")
            assert result is True  # Table exists

    @pytest.mark.asyncio
    async def test_check_data_bigquery_exception(self, mock_bigquery_client):
        """Test BigQuery check exception"""
        from services import verification_service

        mock_bigquery_client.query.side_effect = Exception("Query error")
        mock_bigquery_client.get_table.return_value = Mock()

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await verification_service.check_data_in_bigquery("test", "test_ds")
            assert result is True

    @pytest.mark.asyncio
    async def test_check_data_bigquery_all_fail(self, mock_bigquery_client):
        """Test BigQuery check all failures"""
        from services import verification_service

        mock_bigquery_client.query.side_effect = Exception("Query error")
        mock_bigquery_client.get_table.side_effect = Exception("Table error")

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await verification_service.check_data_in_bigquery("test", "test_ds")
            assert result is False

    @pytest.mark.asyncio
    async def test_check_table_exists_exception(self, mock_bigquery_client):
        """Test check table exception"""
        from services import verification_service

        mock_bigquery_client.get_table.side_effect = Exception("Error")

        result = await verification_service.check_table_exists(mock_bigquery_client, "test", "test_ds")
        assert result is False

    @pytest.mark.asyncio
    async def test_verify_complete_setup_sink_exception(self):
        """Test complete setup with sink exception"""
        from services import verification_service

        with patch('services.bigquery_service.verify_dataset_exists', return_value=True), \
             patch('services.bigquery_service.verify_table_exists', return_value=True), \
             patch('services.sink_service.verify_sink', side_effect=Exception("Sink error")), \
             patch('services.telemetry_service.verify_telemetry_enabled', return_value=True):

            result = await verification_service.verify_complete_setup("test", "test_ds")
            assert result["sink_exists"] is False

    @pytest.mark.asyncio
    async def test_verify_complete_setup_exception(self):
        """Test complete setup exception"""
        from services import verification_service

        with patch('services.bigquery_service.verify_dataset_exists', side_effect=Exception("Error")):
            with pytest.raises(Exception):
                await verification_service.verify_complete_setup("test", "test_ds")


# ============================================================================
# DEPENDENCY SERVICE - Edge Case
# ============================================================================

class TestDependencyServiceEdge:
    """Cover edge case in dependency service"""

    @pytest.mark.asyncio
    async def test_check_billing_exception_fallthrough(self):
        """Test billing check exception that continues"""
        from services import dependency_service

        with patch('subprocess.run', side_effect=Exception("Unexpected error")):
            # Should not raise, should return True and log warning
            result = await dependency_service.check_billing("test-project")
            assert result is True
