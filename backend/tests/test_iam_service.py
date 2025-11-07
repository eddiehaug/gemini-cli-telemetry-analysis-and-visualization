"""
Unit tests for iam_service.py
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import subprocess
from services import iam_service


class TestCheckPermissions:
    """Test check_permissions function"""

    @pytest.mark.asyncio
    async def test_all_permissions_granted(self, mock_asyncio_sleep):
        """Test when all required permissions are present"""
        with patch('services.iam_service.get_current_user', return_value="user@example.com"), \
             patch('services.iam_service.get_user_roles', return_value=[
                 "roles/bigquery.admin",
                 "roles/logging.admin",
                 "roles/serviceusage.serviceUsageAdmin",
             ]):

            result = await iam_service.check_permissions("test-project")

            assert result["hasAll"] is True
            assert len(result["missingRoles"]) == 0
            assert len(result["currentRoles"]) == 3

    @pytest.mark.asyncio
    async def test_missing_permissions(self, mock_asyncio_sleep):
        """Test when some permissions are missing"""
        with patch('services.iam_service.get_current_user', return_value="user@example.com"), \
             patch('services.iam_service.get_user_roles', return_value=[
                 "roles/bigquery.admin",
             ]) as mock_get_roles, \
             patch('services.iam_service.attempt_grant_roles', return_value=True):

            # First call returns partial roles, second call (after grant) returns all
            mock_get_roles.side_effect = [
                ["roles/bigquery.admin"],
                ["roles/bigquery.admin", "roles/logging.admin", "roles/serviceusage.serviceUsageAdmin"]
            ]

            result = await iam_service.check_permissions("test-project")

            assert result["hasAll"] is True

    @pytest.mark.asyncio
    async def test_grant_roles_fails(self, mock_asyncio_sleep):
        """Test when granting roles fails"""
        with patch('services.iam_service.get_current_user', return_value="user@example.com"), \
             patch('services.iam_service.get_user_roles', return_value=["roles/bigquery.admin"]), \
             patch('services.iam_service.attempt_grant_roles', return_value=False):

            result = await iam_service.check_permissions("test-project")

            assert result["hasAll"] is False
            assert len(result["missingRoles"]) == 2


class TestGetCurrentUser:
    """Test get_current_user function"""

    @pytest.mark.asyncio
    async def test_get_current_user_success(self):
        """Test successful user retrieval"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="user@example.com\n",
            stderr=""
        )):
            user = await iam_service.get_current_user()
            assert user == "user@example.com"

    @pytest.mark.asyncio
    async def test_get_current_user_no_user(self):
        """Test when no user is authenticated"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="",
            stderr=""
        )):
            with pytest.raises(Exception, match="Could not determine authenticated user"):
                await iam_service.get_current_user()


class TestGetUserRoles:
    """Test get_user_roles function"""

    @pytest.mark.asyncio
    async def test_get_user_roles_success(self):
        """Test successful role retrieval"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="roles/bigquery.admin\nroles/logging.admin\n",
            stderr=""
        )):
            roles = await iam_service.get_user_roles("test-project", "user@example.com")

            assert len(roles) == 2
            assert "roles/bigquery.admin" in roles
            assert "roles/logging.admin" in roles

    @pytest.mark.asyncio
    async def test_get_user_roles_no_roles(self):
        """Test when user has no roles"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="",
            stderr=""
        )):
            roles = await iam_service.get_user_roles("test-project", "user@example.com")
            assert roles == []

    @pytest.mark.asyncio
    async def test_get_user_roles_command_fails(self):
        """Test when command fails"""
        with patch('subprocess.run', return_value=Mock(
            returncode=1,
            stdout="",
            stderr="error"
        )):
            roles = await iam_service.get_user_roles("test-project", "user@example.com")
            assert roles == []


class TestAttemptGrantRoles:
    """Test attempt_grant_roles function"""

    @pytest.mark.asyncio
    async def test_grant_roles_success(self):
        """Test successful role granting"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="Updated IAM policy",
            stderr=""
        )):
            result = await iam_service.attempt_grant_roles(
                "test-project",
                "user@example.com",
                ["roles/bigquery.admin"]
            )

            assert result is True

    @pytest.mark.asyncio
    async def test_grant_roles_partial_failure(self):
        """Test when some role grants fail"""
        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="success", stderr=""),  # First role succeeds
            Mock(returncode=1, stdout="", stderr="Permission denied"),  # Second fails
        ]):
            result = await iam_service.attempt_grant_roles(
                "test-project",
                "user@example.com",
                ["roles/bigquery.admin", "roles/logging.admin"]
            )

            # Should still return True (doesn't fail hard)
            assert result is True

    @pytest.mark.asyncio
    async def test_grant_roles_exception(self):
        """Test exception handling"""
        with patch('subprocess.run', side_effect=Exception("Connection error")):
            result = await iam_service.attempt_grant_roles(
                "test-project",
                "user@example.com",
                ["roles/bigquery.admin"]
            )

            # Should not raise, just return False
            assert result is False


class TestWaitForIamPropagation:
    """Test wait_for_iam_propagation function"""

    @pytest.mark.asyncio
    async def test_wait_for_propagation(self, mock_asyncio_sleep):
        """Test propagation wait"""
        await iam_service.wait_for_iam_propagation(90)
        # Should complete without error (sleep is mocked)

    @pytest.mark.asyncio
    async def test_wait_custom_duration(self, mock_asyncio_sleep):
        """Test custom wait duration"""
        await iam_service.wait_for_iam_propagation(30)
        # Should complete without error
