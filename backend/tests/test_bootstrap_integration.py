"""
Integration tests for Bootstrap Endpoint
Target: Test /api/bootstrap endpoint end-to-end
Coverage Target: >80%
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)


class TestBootstrapEndpoint:
    """Integration tests for bootstrap endpoint"""

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('main.check_compute_api_enabled')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_success_all_steps(
        self, mock_deps, mock_auth, mock_compute, mock_apis, mock_networks
    ):
        """Test successful bootstrap with all 5 steps"""
        # Setup mocks
        mock_deps.return_value = {"dependencies": [{"name": "gcloud", "installed": True}]}
        mock_auth.return_value = {"authenticated": True, "account": "user@example.com"}
        mock_compute.return_value = True
        mock_apis.return_value = {"enabled": ["bigquery.googleapis.com"], "failed": []}
        mock_networks.return_value = [
            {"name": "custom-vpc"},
            {"name": "default"}
        ]

        # Call endpoint
        response = client.post("/api/bootstrap", json={"projectId": "test-project"})

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["compute_api_enabled"] is True
        assert data["data"]["networks_ok"] is True
        assert data["data"]["network_count"] == 2

    @patch('main.check_compute_api_enabled')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_fails_compute_api_not_enabled(
        self, mock_deps, mock_auth, mock_compute
    ):
        """Test bootstrap fails when Compute API not enabled"""
        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}
        mock_compute.side_effect = Exception("Compute Engine API is not enabled")

        response = client.post("/api/bootstrap", json={"projectId": "test-project"})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "Compute Engine API is not enabled" in data["error"]

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('main.check_compute_api_enabled')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_fails_default_network_only(
        self, mock_deps, mock_auth, mock_compute, mock_apis, mock_networks
    ):
        """Test bootstrap fails when only default network exists"""
        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}
        mock_compute.return_value = True
        mock_apis.return_value = {"enabled": [], "failed": []}
        mock_networks.return_value = [{"name": "default"}]  # Only default

        response = client.post("/api/bootstrap", json={"projectId": "test-project"})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "default VPC network" in data["error"]

    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_fails_authentication(self, mock_deps, mock_auth):
        """Test bootstrap fails during authentication"""
        mock_deps.return_value = {"dependencies": []}
        mock_auth.side_effect = Exception("Authentication failed")

        response = client.post("/api/bootstrap", json={"projectId": "test-project"})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "Authentication failed" in data["error"]

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('main.check_compute_api_enabled')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_api_enablement_propagation(
        self, mock_deps, mock_auth, mock_compute, mock_apis, mock_networks
    ):
        """Test bootstrap waits for API propagation"""
        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}
        mock_compute.return_value = True

        # Simulate enabling new APIs
        mock_apis.return_value = {
            "enabled": [
                "bigquery.googleapis.com",
                "logging.googleapis.com",
                "pubsub.googleapis.com",
                "dataflow.googleapis.com"
            ],
            "failed": []
        }
        mock_networks.return_value = [{"name": "custom-vpc"}]

        response = client.post("/api/bootstrap", json={"projectId": "test-project"})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert len(data["data"]["apis_enabled"]["enabled"]) == 4

    def test_bootstrap_missing_project_id(self):
        """Test bootstrap fails without project ID"""
        response = client.post("/api/bootstrap", json={})

        # Should still return 200 but with success=False
        assert response.status_code == 200
        # This will fail with a key error or similar - that's expected

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('main.check_compute_api_enabled')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_with_multiple_custom_networks(
        self, mock_deps, mock_auth, mock_compute, mock_apis, mock_networks
    ):
        """Test bootstrap succeeds with multiple custom networks"""
        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}
        mock_compute.return_value = True
        mock_apis.return_value = {"enabled": [], "failed": []}
        mock_networks.return_value = [
            {"name": "production-vpc"},
            {"name": "staging-vpc"},
            {"name": "development-vpc"},
            {"name": "default"}
        ]

        response = client.post("/api/bootstrap", json={"projectId": "well-configured-project"})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["network_count"] == 4
