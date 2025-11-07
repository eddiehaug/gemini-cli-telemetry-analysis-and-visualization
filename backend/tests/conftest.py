"""
Pytest configuration and fixtures
"""
import pytest
from unittest.mock import Mock, AsyncMock, MagicMock
from fastapi.testclient import TestClient
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def mock_subprocess_run():
    """Mock subprocess.run for testing CLI commands"""
    def _mock_run(*args, **kwargs):
        mock = Mock()
        mock.returncode = 0
        mock.stdout = "success"
        mock.stderr = ""
        return mock
    return _mock_run


@pytest.fixture
def mock_subprocess_run_failure():
    """Mock subprocess.run for testing CLI command failures"""
    def _mock_run(*args, **kwargs):
        mock = Mock()
        mock.returncode = 1
        mock.stdout = ""
        mock.stderr = "error"
        return mock
    return _mock_run


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client"""
    client = MagicMock()
    client.create_dataset = MagicMock()
    client.get_dataset = MagicMock()
    client.create_table = MagicMock()
    client.get_table = MagicMock()
    client.query = MagicMock()
    return client


@pytest.fixture
def mock_logging_client():
    """Mock Cloud Logging client"""
    client = MagicMock()
    logger = MagicMock()
    client.logger = MagicMock(return_value=logger)
    client.list_entries = MagicMock(return_value=iter([]))
    return client


@pytest.fixture
def sample_config():
    """Sample deployment configuration"""
    return {
        "projectId": "test-project-123",
        "region": "us-central1",
        "datasetName": "test_dataset",
        "logPrompts": False
    }


@pytest.fixture
def test_client():
    """FastAPI test client"""
    from main import app
    return TestClient(app)


@pytest.fixture
def mock_asyncio_sleep(monkeypatch):
    """Mock asyncio.sleep to speed up tests"""
    async def _mock_sleep(seconds):
        pass

    import asyncio
    monkeypatch.setattr(asyncio, 'sleep', _mock_sleep)
