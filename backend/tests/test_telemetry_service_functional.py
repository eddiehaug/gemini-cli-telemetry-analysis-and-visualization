"""
Functional tests for Telemetry Service
Target: End-to-end functionality testing

Tests complete scenarios:
1. Deployment with same project
2. Deployment with different projects
3. Switching between configurations
4. Real-world edge cases
"""
import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from services import telemetry_service


class TestCompleteDeploymentScenarios:
    """Test complete deployment scenarios"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_production_deployment_same_project(
        self, mock_path_class, mock_settings_path
    ):
        """Test production deployment with same project (recommended setup)"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Production configuration
            result = await telemetry_service.configure_telemetry(
                log_prompts=False,  # Don't log prompts in production
                inference_project_id="my-company-prod",
                telemetry_project_id="my-company-prod"
            )

            # Verify success
            assert result["enabled"] is True
            assert result["log_prompts"] is False

            # Verify settings.json
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["telemetry"]["enabled"] is True
                assert settings["telemetry"]["target"] == "gcp"
                assert settings["telemetry"]["logPrompts"] is False
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "my-company-prod"
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

            # Verify shell profile
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert 'export GOOGLE_CLOUD_PROJECT="my-company-prod"' in profile_content
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in profile_content
                assert ">>> Gemini CLI Telemetry Configuration >>>" in profile_content
                assert "<<< Gemini CLI Telemetry Configuration <<<" in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/zsh'})
    async def test_development_deployment_different_projects(
        self, mock_path_class, mock_settings_path
    ):
        """Test development deployment with separate projects"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.zshrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_zshrc = MagicMock()
            mock_zshrc.exists.return_value = True
            mock_zshrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_zshrc
            mock_path_class.home.return_value = mock_home

            # Development configuration with separate projects
            result = await telemetry_service.configure_telemetry(
                log_prompts=True,  # Log prompts for debugging
                inference_project_id="my-company-dev",
                telemetry_project_id="my-company-telemetry-dev"
            )

            # Verify success
            assert result["enabled"] is True
            assert result["log_prompts"] is True

            # Verify settings.json has both environment variables
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["telemetry"]["logPrompts"] is True
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "my-company-dev"
                assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "my-company-telemetry-dev"

            # Verify shell profile has both exports
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert 'export GOOGLE_CLOUD_PROJECT="my-company-dev"' in profile_content
                assert 'export OTLP_GOOGLE_CLOUD_PROJECT="my-company-telemetry-dev"' in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)


class TestConfigurationSwitching:
    """Test switching between different configurations"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_switch_from_same_to_different_projects(
        self, mock_path_class, mock_settings_path
    ):
        """Test switching from same project to different projects"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Step 1: Configure with same project
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="project-a",
                telemetry_project_id="project-a"
            )

            # Verify initial state
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "project-a"
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

            # Step 2: Switch to different projects
            await telemetry_service.configure_telemetry(
                log_prompts=False,
                inference_project_id="project-b",
                telemetry_project_id="project-c"
            )

            # Verify updated state
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "project-b"
                assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "project-c"

            # Verify shell profile updated
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert 'export GOOGLE_CLOUD_PROJECT="project-b"' in profile_content
                assert 'export OTLP_GOOGLE_CLOUD_PROJECT="project-c"' in profile_content
                assert "project-a" not in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_switch_from_different_to_same_project(
        self, mock_path_class, mock_settings_path
    ):
        """Test switching from different projects to same project"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Step 1: Configure with different projects
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="inference-proj",
                telemetry_project_id="telemetry-proj"
            )

            # Verify initial state
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry-proj"

            # Step 2: Switch to same project
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="unified-project",
                telemetry_project_id="unified-project"
            )

            # Verify OTLP variable was removed
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "unified-project"
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

            # Verify shell profile cleaned up
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert 'export GOOGLE_CLOUD_PROJECT="unified-project"' in profile_content
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)


class TestRealWorldEdgeCases:
    """Test real-world edge cases"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_special_characters_in_project_ids(
        self, mock_path_class, mock_settings_path
    ):
        """Test project IDs with hyphens and numbers"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Configure with complex project IDs
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="my-company-ai-2024",
                telemetry_project_id="telemetry-prod-us-central1"
            )

            # Verify settings.json
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "my-company-ai-2024"
                assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry-prod-us-central1"

            # Verify shell profile has properly quoted values
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert 'export GOOGLE_CLOUD_PROJECT="my-company-ai-2024"' in profile_content
                assert 'export OTLP_GOOGLE_CLOUD_PROJECT="telemetry-prod-us-central1"' in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_existing_shell_profile_with_manual_exports(
        self, mock_path_class, mock_settings_path
    ):
        """Test handling of shell profile with manually added exports"""
        existing_profile = """# User's manual configuration
export GOOGLE_CLOUD_PROJECT="old-manual-project"
export OTLP_GOOGLE_CLOUD_PROJECT="old-manual-telemetry"

# Some other config
alias gcp='gcloud'
"""

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name
            f.write(existing_profile)

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Configure telemetry
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="new-project",
                telemetry_project_id="new-project"
            )

            # Verify shell profile has both old manual and new managed config
            with open(profile_file, 'r') as f:
                profile_content = f.read()

                # Old manual exports should still be there
                assert 'export GOOGLE_CLOUD_PROJECT="old-manual-project"' in profile_content

                # New managed exports should be in configuration block
                assert ">>> Gemini CLI Telemetry Configuration >>>" in profile_content
                assert 'export GOOGLE_CLOUD_PROJECT="new-project"' in profile_content

                # Alias should be preserved
                assert "alias gcp='gcloud'" in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_reconfiguration_multiple_times(
        self, mock_path_class, mock_settings_path
    ):
        """Test reconfiguring telemetry multiple times"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Configure 5 times with different settings
            configs = [
                ("proj-1", "proj-1", True),
                ("proj-2", "proj-3", False),
                ("proj-4", "proj-4", True),
                ("proj-5", "proj-6", False),
                ("final", "final", False),
            ]

            for inference, telemetry, log_prompts in configs:
                await telemetry_service.configure_telemetry(
                    log_prompts=log_prompts,
                    inference_project_id=inference,
                    telemetry_project_id=telemetry
                )

            # Verify final state only contains last configuration
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["telemetry"]["logPrompts"] is False
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "final"
                assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

            # Verify shell profile only has one configuration block
            with open(profile_file, 'r') as f:
                profile_content = f.read()
                assert profile_content.count(">>> Gemini CLI Telemetry Configuration >>>") == 1
                assert 'export GOOGLE_CLOUD_PROJECT="final"' in profile_content
                assert "proj-1" not in profile_content
                assert "proj-2" not in profile_content
                assert "proj-3" not in profile_content

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)


class TestGDPRAndPrivacyScenarios:
    """Test GDPR compliance and privacy scenarios"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_gdpr_compliant_telemetry_same_project(
        self, mock_path_class, mock_settings_path
    ):
        """Test GDPR-compliant telemetry with same project (no prompt logging)"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # GDPR-compliant configuration
            result = await telemetry_service.configure_telemetry(
                log_prompts=False,  # GDPR: Don't log prompts with PII
                inference_project_id="eu-gdpr-project",
                telemetry_project_id="eu-gdpr-project"
            )

            assert result["log_prompts"] is False

            # Verify settings
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["telemetry"]["logPrompts"] is False

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_data_residency_different_projects(
        self, mock_path_class, mock_settings_path
    ):
        """Test data residency compliance with different projects"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name

        try:
            # Setup mocks
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Data residency: Use EU project for telemetry, global for inference
            result = await telemetry_service.configure_telemetry(
                log_prompts=False,
                inference_project_id="global-inference",
                telemetry_project_id="eu-telemetry-only"
            )

            # Verify both environment variables set correctly
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "global-inference"
                assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "eu-telemetry-only"

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
            if os.path.exists(profile_file):
                os.unlink(profile_file)
