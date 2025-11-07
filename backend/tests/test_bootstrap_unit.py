"""
Unit tests for Bootstrap helper function
Target: Test check_compute_api_enabled() helper function
Coverage Target: 100%
"""
import pytest
from unittest.mock import patch, AsyncMock
import sys
import os

# Add parent directory to path to import main
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import check_compute_api_enabled


class TestCheckComputeApiEnabled:
    """Unit tests for check_compute_api_enabled function"""

    @pytest.mark.asyncio
    @patch('services.api_service.get_enabled_apis')
    async def test_compute_api_enabled_success(self, mock_get_apis):
        """Test success when Compute API is enabled"""
        mock_get_apis.return_value = [
            "compute.googleapis.com",
            "bigquery.googleapis.com"
        ]

        result = await check_compute_api_enabled("test-project")
        assert result is True

        # Verify function was called with correct project
        mock_get_apis.assert_called_once_with("test-project")

    @pytest.mark.asyncio
    @patch('services.api_service.get_enabled_apis')
    async def test_compute_api_not_enabled_raises_exception(self, mock_get_apis):
        """Test exception when Compute API not enabled"""
        mock_get_apis.return_value = ["bigquery.googleapis.com"]

        with pytest.raises(Exception) as exc_info:
            await check_compute_api_enabled("test-project")

        # Verify error message contains key information
        error_msg = str(exc_info.value)
        assert "Compute Engine API is not enabled" in error_msg
        assert "test-project" in error_msg
        assert "landing zone" in error_msg
        assert "https://cloud.google.com/vpc/docs/vpc" in error_msg

    @pytest.mark.asyncio
    @patch('services.api_service.get_enabled_apis')
    async def test_compute_api_empty_list_raises_exception(self, mock_get_apis):
        """Test exception when no APIs enabled"""
        mock_get_apis.return_value = []

        with pytest.raises(Exception) as exc_info:
            await check_compute_api_enabled("brand-new-project")

        error_msg = str(exc_info.value)
        assert "Compute Engine API is not enabled" in error_msg
        assert "brand-new-project" in error_msg

    @pytest.mark.asyncio
    @patch('services.api_service.get_enabled_apis')
    async def test_compute_api_check_handles_api_service_error(self, mock_get_apis):
        """Test error handling when API service fails"""
        mock_get_apis.side_effect = Exception("API service error")

        with pytest.raises(Exception) as exc_info:
            await check_compute_api_enabled("test-project")

        assert "API service error" in str(exc_info.value)
