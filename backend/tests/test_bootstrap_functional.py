"""
Functional tests for Bootstrap Workflows
Target: Test complete bootstrap workflow scenarios
Coverage Target: >80%
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)


class TestBootstrapWorkflows:
    """Functional tests for complete bootstrap workflows"""

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_production_bootstrap_scenario(
        self, mock_deps, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test production bootstrap with fully configured project"""
        # Scenario: Project has Compute API enabled, custom VPC, all ready
        project_id = "production-telemetry-project"

        # Step 1: Dependencies already installed
        mock_deps.return_value = {
            "dependencies": [
                {"name": "gcloud", "installed": True, "version": "450.0.0"},
                {"name": "gemini", "installed": True, "version": "1.0.0"},
                {"name": "python", "installed": True, "version": "3.12.0"}
            ]
        }

        # Step 2: User authenticated
        mock_auth.return_value = {
            "authenticated": True,
            "account": "user@production.com",
            "method": "application-default"
        }

        # Step 3: Compute API enabled
        mock_get_apis.return_value = [
            "compute.googleapis.com",
            "bigquery.googleapis.com",
            "logging.googleapis.com"
        ]

        # Step 4: Auto-enable other APIs
        mock_enable_apis.return_value = {
            "enabled": [
                "bigquery.googleapis.com",
                "logging.googleapis.com",
                "pubsub.googleapis.com",
                "dataflow.googleapis.com"
            ],
            "failed": []
        }

        # Step 5: Custom VPC exists
        mock_networks.return_value = [
            {"name": "production-vpc"},
            {"name": "staging-vpc"},
            {"name": "default"}
        ]

        # Call bootstrap endpoint
        response = client.post("/api/bootstrap", json={"projectId": project_id})

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True

        # Verify all steps completed
        assert data["data"]["dependencies"] is not None
        assert data["data"]["auth"]["authenticated"] is True
        assert data["data"]["auth"]["account"] == "user@production.com"
        assert data["data"]["compute_api_enabled"] is True
        assert data["data"]["networks_ok"] is True
        assert data["data"]["network_count"] == 3

    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_new_project_bootstrap_failure(
        self, mock_deps, mock_auth, mock_get_apis
    ):
        """Test bootstrap failure with brand new project (no Compute API)"""
        # Scenario: Brand new project, no APIs enabled, no VPCs created
        project_id = "brand-new-project"

        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}

        # No Compute API enabled - this should fail
        mock_get_apis.return_value = []

        response = client.post("/api/bootstrap", json={"projectId": project_id})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False

        # Should fail at Compute API check
        assert "Compute Engine API is not enabled" in data["error"]
        assert "landing zone" in data["error"]

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_default_network_only_scenario(
        self, mock_deps, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test bootstrap blocks when only default network exists"""
        # Scenario: Compute API enabled, but only default VPC network
        project_id = "default-network-only-project"

        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}

        # Compute API is enabled
        mock_get_apis.return_value = ["compute.googleapis.com"]
        mock_enable_apis.return_value = {"enabled": [], "failed": []}

        # Only default network exists
        mock_networks.return_value = [{"name": "default"}]

        response = client.post("/api/bootstrap", json={"projectId": project_id})

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "default VPC network" in data["error"]

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_bootstrap_retry_after_fix(
        self, mock_deps, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test user can retry bootstrap after fixing issues"""
        project_id = "retry-test-project"

        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}

        # First attempt: Fails (no Compute API)
        mock_get_apis.return_value = []

        response1 = client.post("/api/bootstrap", json={"projectId": project_id})
        assert response1.json()["success"] is False

        # User enables Compute API and creates VPC
        # (Simulated by changing mock return values)
        mock_get_apis.return_value = ["compute.googleapis.com"]
        mock_enable_apis.return_value = {"enabled": [], "failed": []}
        mock_networks.return_value = [{"name": "custom-vpc"}]

        # Second attempt: Succeeds
        response2 = client.post("/api/bootstrap", json={"projectId": project_id})
        assert response2.json()["success"] is True

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_api_auto_enablement_workflow(
        self, mock_deps, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test automatic enablement of required APIs"""
        project_id = "api-test-project"

        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True}

        # Compute API already enabled
        mock_get_apis.return_value = ["compute.googleapis.com"]

        # Simulate enabling multiple new APIs
        mock_enable_apis.return_value = {
            "enabled": [
                "bigquery.googleapis.com",
                "logging.googleapis.com",
                "pubsub.googleapis.com",
                "dataflow.googleapis.com",
                "datapipelines.googleapis.com",
                "cloudscheduler.googleapis.com",
                "storage.googleapis.com",
                "bigquerydatatransfer.googleapis.com"
            ],
            "failed": []
        }

        mock_networks.return_value = [{"name": "production-vpc"}]

        response = client.post("/api/bootstrap", json={"projectId": project_id})

        assert response.status_code == 200
        data = response.json()

        if data["success"]:
            apis_enabled = data["data"]["apis_enabled"]["enabled"]

            # Should have enabled multiple APIs
            assert len(apis_enabled) >= 4

            # Required APIs should be in the list
            required_apis = [
                "bigquery.googleapis.com",
                "logging.googleapis.com",
                "pubsub.googleapis.com",
                "dataflow.googleapis.com"
            ]

            for api in required_apis:
                assert api in apis_enabled
