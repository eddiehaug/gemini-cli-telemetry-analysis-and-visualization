"""
Integration tests for OAuth Endpoint
Target: Test /api/authenticate-gemini-oauth endpoint
Coverage Target: >80%
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, AsyncMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import app

client = TestClient(app)


class TestGeminiOAuthEndpoint:
    """Integration tests for Gemini OAuth endpoint"""

    @patch('services.auth_service.authenticate_oauth_flow')
    def test_oauth_success(self, mock_oauth):
        """Test successful OAuth authentication"""
        mock_oauth.return_value = {
            "authenticated": True,
            "account": "user@example.com",
            "project_id": "gemini-project",
            "method": "oauth"
        }

        response = client.post(
            "/api/authenticate-gemini-oauth",
            json={"projectId": "gemini-project"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["data"]["authenticated"] is True
        assert data["data"]["account"] == "user@example.com"
        assert data["data"]["project_id"] == "gemini-project"
        assert data["data"]["method"] == "oauth"

    @patch('services.auth_service.authenticate_oauth_flow')
    def test_oauth_failure(self, mock_oauth):
        """Test OAuth authentication failure"""
        mock_oauth.side_effect = Exception("OAuth failed")

        response = client.post(
            "/api/authenticate-gemini-oauth",
            json={"projectId": "gemini-project"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "OAuth failed" in data["error"]

    @patch('services.auth_service.authenticate_oauth_flow')
    def test_oauth_manual_fallback_triggered(self, mock_oauth):
        """Test OAuth manual fallback when browser doesn't open"""
        mock_oauth.side_effect = Exception(
            "MANUAL_AUTH_REQUIRED:https://accounts.google.com/auth?token=abc"
        )

        response = client.post(
            "/api/authenticate-gemini-oauth",
            json={"projectId": "gemini-project"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is False
        assert "MANUAL_AUTH_REQUIRED" in data["error"]
        assert "https://accounts.google.com" in data["error"]
