"""
Unit tests for Auth Service OAuth Flow
Target: Test authenticate_oauth_flow() function
Coverage Target: >85%
"""
import pytest
import subprocess
from unittest.mock import patch, Mock, AsyncMock
import sys
import os

# Add parent directory to path to import services
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import auth_service


class TestAuthenticateOAuthFlow:
    """Unit tests for OAuth authentication flow"""

    @pytest.mark.asyncio
    @patch('services.auth_service.get_active_account')
    @patch('services.auth_service.subprocess.run')
    async def test_oauth_flow_success(self, mock_run, mock_get_account):
        """Test successful OAuth flow"""
        mock_run.return_value = Mock(returncode=0, stdout="Success", stderr="")
        mock_get_account.return_value = "user@example.com"

        result = await auth_service.authenticate_oauth_flow("test-project")

        assert result["authenticated"] is True
        assert result["account"] == "user@example.com"
        assert result["project_id"] == "test-project"
        assert result["method"] == "oauth"

        # Verify gcloud was called with correct arguments
        call_args = mock_run.call_args[0][0]
        assert "gcloud" in call_args
        assert "auth" in call_args
        assert "login" in call_args
        assert "--project" in call_args
        assert "test-project" in call_args

    @pytest.mark.asyncio
    @patch('services.auth_service.subprocess.run')
    async def test_oauth_flow_browser_open_failure(self, mock_run):
        """Test fallback when browser doesn't open"""
        # First call fails with browser error, second provides manual URL
        mock_run.side_effect = [
            Mock(returncode=1, stdout="", stderr="Failed to open browser"),
            Mock(
                returncode=0,
                stdout="Go to the following link: https://accounts.google.com/o/oauth2/auth?token=abc",
                stderr=""
            )
        ]

        with pytest.raises(Exception) as exc_info:
            await auth_service.authenticate_oauth_flow("test-project")

        error_msg = str(exc_info.value)
        assert "MANUAL_AUTH_REQUIRED" in error_msg
        assert "https://accounts.google.com" in error_msg
        assert "Open this URL in your browser" in error_msg

    @pytest.mark.asyncio
    @patch('services.auth_service.subprocess.run')
    async def test_oauth_flow_timeout(self, mock_run):
        """Test OAuth timeout handling"""
        mock_run.side_effect = subprocess.TimeoutExpired("gcloud", 120)

        with pytest.raises(Exception) as exc_info:
            await auth_service.authenticate_oauth_flow("test-project")

        assert "OAuth authentication timed out" in str(exc_info.value)
        assert "120 seconds" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('services.auth_service.subprocess.run')
    async def test_oauth_flow_authentication_failure(self, mock_run):
        """Test OAuth authentication failure"""
        mock_run.return_value = Mock(returncode=1, stdout="", stderr="Invalid credentials")

        with pytest.raises(Exception) as exc_info:
            await auth_service.authenticate_oauth_flow("test-project")

        error_msg = str(exc_info.value)
        assert "OAuth authentication failed" in error_msg
        assert "Invalid credentials" in error_msg

    @pytest.mark.asyncio
    @patch('services.auth_service.get_active_account')
    @patch('services.auth_service.subprocess.run')
    async def test_oauth_flow_with_project_context(self, mock_run, mock_get_account):
        """Test OAuth uses correct project context"""
        mock_run.return_value = Mock(returncode=0, stdout="Success", stderr="")
        mock_get_account.return_value = "user@example.com"

        await auth_service.authenticate_oauth_flow("my-special-project")

        # Verify gcloud was called with correct project
        call_args = mock_run.call_args[0][0]
        assert "my-special-project" in call_args
        assert "--project" in call_args

        # Verify it was called with brief flag
        assert "--brief" in call_args
