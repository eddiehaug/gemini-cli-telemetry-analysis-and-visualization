"""
Final tests to achieve 100% coverage on all services
Targets specific missing lines
"""
import pytest
from unittest.mock import Mock, patch
from google.cloud import bigquery


# Final tests for remaining gaps
class TestFinalCoverage:
    """Hit all remaining uncovered lines"""

    # ========================================================================
    # dependency_service.py line 131 - gemini CLI non-zero returncode
    # ========================================================================
    @pytest.mark.asyncio
    async def test_gemini_cli_non_zero_returncode(self):
        """Test gemini CLI with non-zero return code"""
        from services import dependency_service

        with patch('subprocess.run', side_effect=[
            Mock(returncode=127, stdout="", stderr="command not found"),  # gemini version fails
        ]):
            result = await dependency_service.check_gemini()

            assert result["name"] == "gemini CLI"
            assert result["installed"] is False

    # ========================================================================
    # bigquery_service.py line 97 - table creation Conflict exception
    # ========================================================================
    @pytest.mark.asyncio
    async def test_create_telemetry_table_conflict(self, mock_bigquery_client):
        """Test table creation when already exists"""
        from services import bigquery_service
        from google.cloud.exceptions import Conflict

        mock_bigquery_client.create_table.side_effect = Conflict("Already exists")

        # Should not raise, just log
        await bigquery_service.create_telemetry_table(mock_bigquery_client, "test", "test_ds")

    # ========================================================================
    # api_service.py lines 60-61, 99-101, 127-129 - exception returns
    # ========================================================================
    @pytest.mark.asyncio
    async def test_get_enabled_apis_exception(self):
        """Test get enabled APIs exception"""
        from services import api_service

        with patch('subprocess.run', side_effect=Exception("Network error")):
            apis = await api_service.get_enabled_apis("test")
            assert apis == []

    @pytest.mark.asyncio
    async def test_enable_api_exception(self):
        """Test enable API exception"""
        from services import api_service

        with patch('subprocess.run', side_effect=Exception("Permission denied")):
            result = await api_service.enable_api("test", "bigquery.googleapis.com")
            assert result is False

    @pytest.mark.asyncio
    async def test_verify_api_accessible_generic_exception(self):
        """Test verify API accessible exception"""
        from services import api_service

        with patch('services.api_service.get_enabled_apis', side_effect=Exception("Error")):
            result = await api_service.verify_api_accessible("test", "other.googleapis.com")
            assert result is False

    # ========================================================================
    # deployment_service.py line 127 - update step invalid index edge
    # ========================================================================
    @pytest.mark.asyncio
    async def test_update_step_index_negative(self):
        """Test updating step with negative index"""
        from services import deployment_service

        deployment_id = await deployment_service.create_deployment({"projectId": "test"})

        with pytest.raises(Exception, match="Invalid step index"):
            await deployment_service.update_step_status(deployment_id, -1, "completed")

    # ========================================================================
    # logging_service.py line 137 - gemini command non-zero exit
    # ========================================================================
    @pytest.mark.asyncio
    async def test_gemini_cli_logging_non_zero_exit(self, mock_logging_client, mock_asyncio_sleep):
        """Test Gemini CLI logging with non-zero exit"""
        from services import logging_service

        mock_logging_client.list_entries.return_value = iter([Mock()])

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")), \
             patch('google.cloud.logging.Client', return_value=mock_logging_client):

            result = await logging_service.test_gemini_cli_logging("test")
            # Should still complete despite non-zero exit
            assert result["gemini_command_run"] is True

    # ========================================================================
    # sink_service.py lines 53-54 - sink creation failure paths
    # ========================================================================
    @pytest.mark.asyncio
    async def test_create_sink_not_already_exists_error(self, mock_asyncio_sleep):
        """Test sink creation with error that's not 'already exists'"""
        from services import sink_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="permission denied")):
            with pytest.raises(Exception, match="Failed to create sink"):
                await sink_service.create_sink("test", "test_ds")

    # ========================================================================
    # verification_service.py lines 66-70, 102-103, 175, 183-184
    # ========================================================================
    @pytest.mark.asyncio
    async def test_verify_end_to_end_logs_not_in_cloud_logging(self, mock_asyncio_sleep):
        """Test E2E when logs not in cloud logging"""
        from services import verification_service

        with patch('services.verification_service.run_gemini_test_command', return_value=True), \
             patch('services.verification_service.check_logs_in_cloud_logging', return_value=False), \
             patch('services.verification_service.check_data_in_bigquery', return_value=False):

            result = await verification_service.verify_end_to_end("test", "test_ds")

            assert result["success"] is False
            assert result["logs_in_cloud_logging"] is False

    @pytest.mark.asyncio
    async def test_verify_end_to_end_logs_but_no_data(self, mock_asyncio_sleep):
        """Test E2E when logs present but no data in BQ"""
        from services import verification_service

        with patch('services.verification_service.run_gemini_test_command', return_value=True), \
             patch('services.verification_service.check_logs_in_cloud_logging', return_value=True), \
             patch('services.verification_service.check_data_in_bigquery', return_value=False):

            result = await verification_service.verify_end_to_end("test", "test_ds")

            assert result["success"] is False
            assert result["data_in_bigquery"] is False

    @pytest.mark.asyncio
    async def test_run_gemini_command_non_zero_return(self):
        """Test Gemini command with non-zero return"""
        from services import verification_service

        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            result = await verification_service.run_gemini_test_command()
            # Should still return True as command ran
            assert result is True

    @pytest.mark.asyncio
    async def test_check_data_no_results(self, mock_bigquery_client):
        """Test check data in BigQuery with no results"""
        from services import verification_service

        # Empty iterator (no rows returned from query)
        # This means the for loop on line 164 doesn't execute
        # So it falls through to line 175 and returns False
        mock_bigquery_client.query.return_value.result.return_value = iter([])

        with patch('google.cloud.bigquery.Client', return_value=mock_bigquery_client):
            result = await verification_service.check_data_in_bigquery("test", "test_ds")
            # When query returns no rows, function returns False
            assert result is False
