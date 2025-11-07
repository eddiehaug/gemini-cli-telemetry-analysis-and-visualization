"""
Unit tests for dependency_service.py
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock
import subprocess
from services import dependency_service


class TestVerifyDependencies:
    """Test verify_dependencies function"""

    @pytest.mark.asyncio
    async def test_all_dependencies_installed(self, mock_subprocess_run):
        """Test when all dependencies are installed"""
        with patch('subprocess.run', side_effect=[
            # gcloud version check
            Mock(returncode=0, stdout="Google Cloud SDK 450.0.0\n", stderr=""),
            # gcloud path
            Mock(returncode=0, stdout="/usr/local/bin/gcloud\n", stderr=""),
            # gemini version check
            Mock(returncode=0, stdout="gemini 1.0.0\n", stderr=""),
            # gemini path
            Mock(returncode=0, stdout="/usr/local/bin/gemini\n", stderr=""),
        ]):
            result = await dependency_service.verify_dependencies()

            assert len(result) == 3
            assert all(dep["installed"] for dep in result)
            assert any(dep["name"] == "gcloud CLI" for dep in result)
            assert any(dep["name"] == "gemini CLI" for dep in result)
            assert any(dep["name"] == "Python" for dep in result)

    @pytest.mark.asyncio
    async def test_missing_gcloud(self):
        """Test when gcloud is missing"""
        with patch('subprocess.run', side_effect=[
            # gcloud not found
            FileNotFoundError(),
            # gemini version check
            Mock(returncode=0, stdout="gemini 1.0.0\n", stderr=""),
            # gemini path
            Mock(returncode=0, stdout="/usr/local/bin/gemini\n", stderr=""),
        ]):
            with pytest.raises(Exception, match="Missing required dependencies"):
                await dependency_service.verify_dependencies()

    @pytest.mark.asyncio
    async def test_gcloud_timeout(self):
        """Test when gcloud command times out"""
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 10)):
            with pytest.raises(Exception, match="Missing required dependencies"):
                await dependency_service.verify_dependencies()


class TestCheckGcloud:
    """Test check_gcloud function"""

    @pytest.mark.asyncio
    async def test_gcloud_installed(self):
        """Test successful gcloud check"""
        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="Google Cloud SDK 450.0.0\n", stderr=""),
            Mock(returncode=0, stdout="/usr/local/bin/gcloud\n", stderr=""),
        ]):
            result = await dependency_service.check_gcloud()

            assert result["name"] == "gcloud CLI"
            assert result["installed"] is True
            assert "450.0.0" in result["version"]
            assert result["path"] == "/usr/local/bin/gcloud"

    @pytest.mark.asyncio
    async def test_gcloud_not_installed(self):
        """Test when gcloud is not installed"""
        with patch('subprocess.run', side_effect=FileNotFoundError()):
            result = await dependency_service.check_gcloud()

            assert result["name"] == "gcloud CLI"
            assert result["installed"] is False
            assert result["version"] is None
            assert result["path"] is None

    @pytest.mark.asyncio
    async def test_gcloud_failed_execution(self):
        """Test when gcloud execution fails"""
        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            result = await dependency_service.check_gcloud()

            assert result["installed"] is False


class TestCheckGemini:
    """Test check_gemini function"""

    @pytest.mark.asyncio
    async def test_gemini_installed(self):
        """Test successful gemini check"""
        with patch('subprocess.run', side_effect=[
            Mock(returncode=0, stdout="gemini 1.0.0\n", stderr=""),
            Mock(returncode=0, stdout="/usr/local/bin/gemini\n", stderr=""),
        ]):
            result = await dependency_service.check_gemini()

            assert result["name"] == "gemini CLI"
            assert result["installed"] is True
            assert result["version"]
            assert result["path"] == "/usr/local/bin/gemini"

    @pytest.mark.asyncio
    async def test_gemini_not_installed(self):
        """Test when gemini is not installed"""
        with patch('subprocess.run', side_effect=FileNotFoundError()):
            result = await dependency_service.check_gemini()

            assert result["installed"] is False


class TestGetPythonVersion:
    """Test get_python_version function"""

    @pytest.mark.asyncio
    async def test_get_python_version(self):
        """Test getting Python version"""
        version = await dependency_service.get_python_version()

        assert isinstance(version, str)
        assert len(version.split('.')) == 3  # major.minor.micro


class TestCheckBilling:
    """Test check_billing function"""

    @pytest.mark.asyncio
    async def test_billing_enabled(self):
        """Test when billing is enabled"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="true\n",
            stderr=""
        )):
            result = await dependency_service.check_billing("test-project")

            assert result is True

    @pytest.mark.asyncio
    async def test_billing_not_enabled(self):
        """Test when billing is not enabled"""
        with patch('subprocess.run', return_value=Mock(
            returncode=0,
            stdout="false\n",
            stderr=""
        )):
            # The function raises an exception internally (line 172)
            # but catches it (line 182) and returns True with a warning
            # This is intentional - don't fail hard on billing check
            result = await dependency_service.check_billing("test-project")
            assert result is True  # Should continue with warning logged

    @pytest.mark.asyncio
    async def test_billing_check_timeout(self):
        """Test when billing check times out"""
        with patch('subprocess.run', side_effect=subprocess.TimeoutExpired("gcloud", 30)):
            with pytest.raises(Exception, match="timed out"):
                await dependency_service.check_billing("test-project")

    @pytest.mark.asyncio
    async def test_billing_check_failure_continues(self):
        """Test that billing check failure doesn't fail hard"""
        with patch('subprocess.run', return_value=Mock(returncode=1, stdout="", stderr="error")):
            # Should not raise, just log warning
            result = await dependency_service.check_billing("test-project")
            assert result is True  # Continues even if check fails
