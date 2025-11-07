"""
Functional tests for OAuth Workflows
Target: Test OAuth workflows end-to-end
Coverage Target: >75%
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
import subprocess
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)


class TestOAuthWorkflows:
    """Functional tests for OAuth authentication workflows"""

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.auth_service.authenticate_oauth_flow')
    @patch('services.dependency_service.verify_dependencies')
    def test_separate_project_oauth_flow(
        self, mock_deps, mock_oauth, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test OAuth when using separate Gemini CLI project"""
        # Scenario: Telemetry project != Gemini CLI project
        telemetry_project = "telemetry-project"
        gemini_project = "gemini-cli-project"

        # Bootstrap telemetry project first
        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {"authenticated": True, "account": "user@example.com"}
        mock_get_apis.return_value = ["compute.googleapis.com"]
        mock_enable_apis.return_value = {"enabled": [], "failed": []}
        mock_networks.return_value = [{"name": "custom-vpc"}]

        bootstrap_response = client.post(
            "/api/bootstrap",
            json={"projectId": telemetry_project}
        )
        assert bootstrap_response.json()["success"] is True

        # Authenticate with separate Gemini CLI project
        mock_oauth.return_value = {
            "authenticated": True,
            "account": "user@example.com",
            "project_id": gemini_project,
            "method": "oauth"
        }

        oauth_response = client.post(
            "/api/authenticate-gemini-oauth",
            json={"projectId": gemini_project}
        )

        # Should succeed
        data = oauth_response.json()
        assert data["success"] is True
        assert data["data"]["authenticated"] is True
        assert data["data"]["project_id"] == gemini_project
        assert data["data"]["method"] == "oauth"

    @patch('services.network_service.list_networks')
    @patch('services.api_service.enable_apis')
    @patch('services.api_service.get_enabled_apis')
    @patch('services.auth_service.authenticate')
    @patch('services.dependency_service.verify_dependencies')
    def test_same_project_no_oauth_needed(
        self, mock_deps, mock_auth, mock_get_apis, mock_enable_apis, mock_networks
    ):
        """Test no OAuth needed when using same project"""
        # Scenario: Telemetry project == Gemini CLI project
        project = "unified-project"

        mock_deps.return_value = {"dependencies": []}
        mock_auth.return_value = {
            "authenticated": True,
            "account": "user@example.com",
            "method": "application-default"
        }
        mock_get_apis.return_value = ["compute.googleapis.com"]
        mock_enable_apis.return_value = {"enabled": [], "failed": []}
        mock_networks.return_value = [{"name": "custom-vpc"}]

        # Bootstrap handles authentication
        response = client.post("/api/bootstrap", json={"projectId": project})
        assert response.json()["success"] is True

        # Verify authentication already completed
        auth_data = response.json()["data"]["auth"]
        assert auth_data["authenticated"] is True
        assert auth_data["account"] == "user@example.com"

        # No separate OAuth needed - user will use same project for both purposes

    @patch('services.auth_service.authenticate_oauth_flow')
    def test_oauth_timeout_handling(self, mock_oauth):
        """Test OAuth timeout is handled gracefully"""
        # Scenario: User doesn't complete OAuth in time

        # Mock timeout after 120 seconds
        mock_oauth.side_effect = subprocess.TimeoutExpired("gcloud", 120)

        response = client.post(
            "/api/authenticate-gemini-oauth",
            json={"projectId": "test-project"}
        )

        # Should fail gracefully
        data = response.json()
        assert data["success"] is False
        assert "timeout" in data["error"].lower() or "timed out" in data["error"].lower()
