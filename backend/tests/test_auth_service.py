"""
Unit tests for auth_service.py
"""
import pytest
from unittest.mock import Mock, patch
import subprocess
from services import auth_service


class TestAuthenticate:
    """Test authenticate function"""

    @pytest.mark.asyncio
    async def test_already_authenticated(self):
        """Test when user is already authenticated"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="user@example.com\n",
            stderr=""
        )), patch('os.path.exists', return_value=True):
            result = await auth_service.authenticate()

            assert result["authenticated"] is True
            assert result["account"] == "user@example.com"
            assert result["method"] == "gcloud"

    @pytest.mark.asyncio
    async def test_not_authenticated(self):
        """Test when user is not authenticated"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="",
            stderr=""
        )):
            with pytest.raises(Exception, match="Not authenticated"):
                await auth_service.authenticate()

    @pytest.mark.asyncio
    async def test_authentication_timeout(self):
        """Test when authentication check times out"""
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 30)):
            with pytest.raises(Exception, match="timed out"):
                await auth_service.authenticate()

    @pytest.mark.asyncio
    async def test_adc_missing_creates_warning(self):
        """Test when ADC is missing"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="user@example.com\n",
            stderr=""
        )), patch('os.path.exists', return_value=False), \
           patch('services.auth_service.setup_application_default_credentials') as mock_setup:

            await auth_service.authenticate()
            mock_setup.assert_called_once()


class TestSetupApplicationDefaultCredentials:
    """Test setup_application_default_credentials function"""

    @pytest.mark.asyncio
    async def test_adc_already_configured(self):
        """Test when ADC is already configured"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="ya29.token...",
            stderr=""
        )):
            result = await auth_service.setup_application_default_credentials()
            assert result is True

    @pytest.mark.asyncio
    async def test_adc_not_configured(self):
        """Test when ADC is not configured"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="not configured"
        )):
            with pytest.raises(Exception, match="Application default credentials not configured"):
                await auth_service.setup_application_default_credentials()

    @pytest.mark.asyncio
    async def test_adc_setup_timeout(self):
        """Test when ADC setup times out"""
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 30)):
            with pytest.raises(Exception, match="timed out"):
                await auth_service.setup_application_default_credentials()


class TestGetActiveAccount:
    """Test get_active_account function"""

    @pytest.mark.asyncio
    async def test_get_active_account_success(self):
        """Test successful account retrieval"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="user@example.com\n",
            stderr=""
        )):
            account = await auth_service.get_active_account()
            assert account == "user@example.com"

    @pytest.mark.asyncio
    async def test_get_active_account_no_account(self):
        """Test when no account is active"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="",
            stderr=""
        )):
            with pytest.raises(Exception, match="No active account"):
                await auth_service.get_active_account()

    @pytest.mark.asyncio
    async def test_get_active_account_multiple_accounts(self):
        """Test with multiple accounts (should return first)"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="user1@example.com\nuser2@example.com\n",
            stderr=""
        )):
            account = await auth_service.get_active_account()
            assert account == "user1@example.com"

    @pytest.mark.asyncio
    async def test_get_active_account_error(self):
        """Test error handling"""
        with patch('subprocess.run', side_effect=Exception("Command failed")):
            with pytest.raises(Exception, match="Command failed"):
                await auth_service.get_active_account()
