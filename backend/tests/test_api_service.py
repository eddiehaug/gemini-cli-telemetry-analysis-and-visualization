"""
Unit tests for api_service.py
"""
import pytest
from unittest.mock import Mock, patch
from services import api_service


class TestEnableApis:
    """Test enable_apis function"""

    @pytest.mark.asyncio
    async def test_all_apis_already_enabled(self, mock_asyncio_sleep):
        """Test when all APIs are already enabled"""
        with patch('services.api_service.get_enabled_apis', return_value=api_service.REQUIRED_APIS):
            result = await api_service.enable_apis("test-project")

            assert result["success"] is True
            assert len(result["enabled"]) == len(api_service.REQUIRED_APIS)
            assert len(result["failed"]) == 0

    @pytest.mark.asyncio
    async def test_enable_new_apis(self, mock_asyncio_sleep):
        """Test enabling new APIs"""
        with patch('services.api_service.get_enabled_apis', return_value=[]), \
             patch('services.api_service.enable_api', return_value=True), \
             patch('services.api_service.verify_api_accessible', return_value=True):

            result = await api_service.enable_apis("test-project")

            assert result["success"] is True
            assert len(result["enabled"]) == len(api_service.REQUIRED_APIS)

    @pytest.mark.asyncio
    async def test_api_enable_failure(self, mock_asyncio_sleep):
        """Test when API enablement fails"""
        with patch('services.api_service.get_enabled_apis', return_value=[]), \
             patch('services.api_service.enable_api', return_value=False):

            with pytest.raises(Exception, match="Failed to enable APIs"):
                await api_service.enable_apis("test-project")


class TestGetEnabledApis:
    """Test get_enabled_apis function"""

    @pytest.mark.asyncio
    async def test_get_enabled_apis_success(self):
        """Test successful API list retrieval"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="bigquery.googleapis.com\nlogging.googleapis.com\n",
            stderr=""
        )):
            apis = await api_service.get_enabled_apis("test-project")

            assert len(apis) == 2
            assert "bigquery.googleapis.com" in apis
            assert "logging.googleapis.com" in apis

    @pytest.mark.asyncio
    async def test_get_enabled_apis_empty(self):
        """Test when no APIs are enabled"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="",
            stderr=""
        )):
            apis = await api_service.get_enabled_apis("test-project")
            assert apis == []

    @pytest.mark.asyncio
    async def test_get_enabled_apis_failure(self):
        """Test when command fails"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="error"
        )):
            apis = await api_service.get_enabled_apis("test-project")
            assert apis == []


class TestEnableApi:
    """Test enable_api function"""

    @pytest.mark.asyncio
    async def test_enable_api_success(self):
        """Test successful API enablement"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="Operation finished successfully",
            stderr=""
        )):
            result = await api_service.enable_api("test-project", "bigquery.googleapis.com")
            assert result is True

    @pytest.mark.asyncio
    async def test_enable_api_failure(self):
        """Test API enablement failure"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="Permission denied"
        )):
            result = await api_service.enable_api("test-project", "bigquery.googleapis.com")
            assert result is False

    @pytest.mark.asyncio
    async def test_enable_api_timeout(self):
        """Test API enablement timeout"""
        import subprocess
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 60)):
            result = await api_service.enable_api("test-project", "bigquery.googleapis.com")
            assert result is False


class TestVerifyApiAccessible:
    """Test verify_api_accessible function"""

    @pytest.mark.asyncio
    async def test_verify_bigquery_api(self):
        """Test BigQuery API verification"""
        with patch('services.api_service.verify_bigquery_api', return_value=True):
            result = await api_service.verify_api_accessible("test-project", "bigquery.googleapis.com")
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_logging_api(self):
        """Test Logging API verification"""
        with patch('services.api_service.verify_logging_api', return_value=True):
            result = await api_service.verify_api_accessible("test-project", "logging.googleapis.com")
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_other_api(self):
        """Test other API verification (checks enabled list)"""
        with patch('services.api_service.get_enabled_apis', return_value=["serviceusage.googleapis.com"]):
            result = await api_service.verify_api_accessible("test-project", "serviceusage.googleapis.com")
            assert result is True


class TestVerifyBigQueryApi:
    """Test verify_bigquery_api function"""

    @pytest.mark.asyncio
    async def test_bigquery_api_accessible(self):
        """Test when BigQuery API is accessible"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="Listed 0 datasets",
            stderr=""
        )):
            result = await api_service.verify_bigquery_api("test-project")
            assert result is True

    @pytest.mark.asyncio
    async def test_bigquery_api_not_accessible(self):
        """Test when BigQuery API is not accessible"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="API not enabled"
        )):
            result = await api_service.verify_bigquery_api("test-project")
            assert result is False


class TestVerifyLoggingApi:
    """Test verify_logging_api function"""

    @pytest.mark.asyncio
    async def test_logging_api_accessible(self):
        """Test when Logging API is accessible"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="projects/test/logs/syslog",
            stderr=""
        )):
            result = await api_service.verify_logging_api("test-project")
            assert result is True

    @pytest.mark.asyncio
    async def test_logging_api_not_accessible(self):
        """Test when Logging API is not accessible"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="API not enabled"
        )):
            result = await api_service.verify_logging_api("test-project")
            assert result is False
