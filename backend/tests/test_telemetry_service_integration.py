"""
Integration tests for Telemetry Service
Target: Test interactions between components

Tests:
1. Complete telemetry configuration workflow
2. Settings.json and shell profile integration
3. File system interactions
4. Environment variable propagation
"""
import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock
from services import telemetry_service


class TestTelemetryConfigurationWorkflow:
    """Test complete telemetry configuration workflow"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_full_workflow_same_project(self, mock_path_class, mock_settings_path):
        """Test complete workflow with same project"""
        # Setup temporary file for settings
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        try:
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            # Mock shell profile
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
                profile_file = f.name

            try:
                mock_home = MagicMock()
                mock_bashrc = MagicMock()
                mock_bashrc.exists.return_value = True
                mock_bashrc.__str__.return_value = profile_file
                mock_home.__truediv__.return_value = mock_bashrc
                mock_path_class.home.return_value = mock_home

                # Run configuration
                result = await telemetry_service.configure_telemetry(
                    log_prompts=True,
                    inference_project_id="my-project",
                    telemetry_project_id="my-project"
                )

                # Verify result structure
                assert result["enabled"] is True
                assert result["log_prompts"] is True
                assert "env_vars" in result
                assert "shell_profile" in result

                # Verify settings.json was updated
                with open(settings_file, 'r') as f:
                    settings = json.load(f)
                    assert settings["telemetry"]["enabled"] is True
                    assert settings["telemetry"]["logPrompts"] is True
                    assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "my-project"
                    assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

                # Verify shell profile was updated
                with open(profile_file, 'r') as f:
                    profile_content = f.read()
                    assert 'export GOOGLE_CLOUD_PROJECT="my-project"' in profile_content
                    assert ">>> Gemini CLI Telemetry Configuration >>>" in profile_content

            finally:
                if os.path.exists(profile_file):
                    os.unlink(profile_file)

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/zsh'})
    async def test_full_workflow_different_projects(self, mock_path_class, mock_settings_path):
        """Test complete workflow with different projects"""
        # Setup temporary file for settings
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        try:
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            # Mock shell profile
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.zshrc') as f:
                profile_file = f.name

            try:
                mock_home = MagicMock()
                mock_zshrc = MagicMock()
                mock_zshrc.exists.return_value = True
                mock_zshrc.__str__.return_value = profile_file
                mock_home.__truediv__.return_value = mock_zshrc
                mock_path_class.home.return_value = mock_home

                # Run configuration
                result = await telemetry_service.configure_telemetry(
                    log_prompts=False,
                    inference_project_id="inference-proj",
                    telemetry_project_id="telemetry-proj"
                )

                # Verify result
                assert result["enabled"] is True
                assert result["log_prompts"] is False

                # Verify settings.json has both environment variables
                with open(settings_file, 'r') as f:
                    settings = json.load(f)
                    assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "inference-proj"
                    assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry-proj"

                # Verify shell profile has both exports
                with open(profile_file, 'r') as f:
                    profile_content = f.read()
                    assert 'export GOOGLE_CLOUD_PROJECT="inference-proj"' in profile_content
                    assert 'export OTLP_GOOGLE_CLOUD_PROJECT="telemetry-proj"' in profile_content

            finally:
                if os.path.exists(profile_file):
                    os.unlink(profile_file)

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)


class TestSettingsFileIntegration:
    """Test settings.json file operations"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_settings_read_write_cycle(self, mock_path):
        """Test reading and writing settings preserves data"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            initial_data = {"custom": "value", "telemetry": {"enabled": False}}
            json.dump(initial_data, f)

        try:
            mock_path.__str__.return_value = settings_file
            mock_path.exists.return_value = True
            mock_path.parent.mkdir = MagicMock()

            # Read settings
            settings = await telemetry_service.read_gemini_settings()
            assert settings["custom"] == "value"

            # Modify settings
            settings["telemetry"]["enabled"] = True
            settings["env"] = {"GOOGLE_CLOUD_PROJECT": "test"}

            # Write settings
            await telemetry_service.write_gemini_settings(settings)

            # Read again and verify
            updated_settings = await telemetry_service.read_gemini_settings()
            assert updated_settings["custom"] == "value"  # Preserved
            assert updated_settings["telemetry"]["enabled"] is True  # Updated
            assert updated_settings["env"]["GOOGLE_CLOUD_PROJECT"] == "test"  # Added

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_settings_creates_parent_directory(self, mock_path):
        """Test settings file creation creates parent directory"""
        with tempfile.TemporaryDirectory() as tmpdir:
            settings_dir = Path(tmpdir) / ".gemini"
            settings_file = settings_dir / "settings.json"

            mock_path.__str__.return_value = str(settings_file)
            mock_path.parent = settings_dir

            # Write settings (directory doesn't exist yet)
            await telemetry_service.write_gemini_settings({"test": "data"})

            # Verify directory was created
            assert settings_dir.exists()
            assert settings_file.exists()

            # Verify content
            with open(settings_file, 'r') as f:
                data = json.load(f)
                assert data["test"] == "data"


class TestShellProfileIntegration:
    """Test shell profile file operations"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_shell_profile_update_idempotent(self, mock_path_class):
        """Test updating shell profile multiple times is idempotent"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name
            f.write("# Existing config\nexport SOME_VAR='value'\n")

        try:
            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # First update
            await telemetry_service.configure_environment_variables_in_shell(
                "project-1", "project-1"
            )

            # Second update (should replace, not append)
            await telemetry_service.configure_environment_variables_in_shell(
                "project-2", "project-2"
            )

            # Third update with different projects
            await telemetry_service.configure_environment_variables_in_shell(
                "inference", "telemetry"
            )

            # Verify final state
            with open(profile_file, 'r') as f:
                content = f.read()

                # Should have only one configuration block
                assert content.count(">>> Gemini CLI Telemetry Configuration >>>") == 1
                assert content.count("<<< Gemini CLI Telemetry Configuration <<<") == 1

                # Should have latest values
                assert 'export GOOGLE_CLOUD_PROJECT="inference"' in content
                assert 'export OTLP_GOOGLE_CLOUD_PROJECT="telemetry"' in content

                # Should not have old values
                assert "project-1" not in content
                assert "project-2" not in content

        finally:
            if os.path.exists(profile_file):
                os.unlink(profile_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_shell_profile_preserves_other_content(self, mock_path_class):
        """Test shell profile updates preserve other content"""
        initial_content = """# User's custom bash configuration
export PATH="$HOME/bin:$PATH"
alias ll='ls -la'

# Some function
function greet() {
  echo "Hello"
}
"""

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.bashrc') as f:
            profile_file = f.name
            f.write(initial_content)

        try:
            mock_home = MagicMock()
            mock_bashrc = MagicMock()
            mock_bashrc.exists.return_value = True
            mock_bashrc.__str__.return_value = profile_file
            mock_home.__truediv__.return_value = mock_bashrc
            mock_path_class.home.return_value = mock_home

            # Update with Gemini config
            await telemetry_service.configure_environment_variables_in_shell(
                "test-project", "test-project"
            )

            # Verify original content preserved
            with open(profile_file, 'r') as f:
                content = f.read()

                # Original content should still be there
                assert "User's custom bash configuration" in content
                assert 'export PATH="$HOME/bin:$PATH"' in content
                assert "alias ll='ls -la'" in content
                assert "function greet()" in content

                # New content should be added
                assert ">>> Gemini CLI Telemetry Configuration >>>" in content
                assert 'export GOOGLE_CLOUD_PROJECT="test-project"' in content

        finally:
            if os.path.exists(profile_file):
                os.unlink(profile_file)


class TestErrorRecovery:
    """Test error handling and recovery"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_shell_error_doesnt_fail_telemetry_config(
        self, mock_path_class, mock_settings_path
    ):
        """Test shell profile errors don't fail overall configuration"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        try:
            mock_settings_path.__str__.return_value = settings_file
            mock_settings_path.exists.return_value = True
            mock_settings_path.parent.mkdir = MagicMock()

            # Make shell profile fail
            mock_home = MagicMock()
            mock_home.__truediv__.side_effect = Exception("Shell profile error")
            mock_path_class.home.return_value = mock_home

            # Should not raise exception
            result = await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="test",
                telemetry_project_id="test"
            )

            # Settings.json should still be configured
            assert result["enabled"] is True

            # Shell profile should have error
            assert "error" in result["shell_profile"]

            # Verify settings.json was updated despite shell error
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                assert settings["telemetry"]["enabled"] is True
                assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "test"

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_corrupted_settings_file_handling(self, mock_path):
        """Test handling of corrupted settings file"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            f.write("{ invalid json content }")

        try:
            mock_path.__str__.return_value = settings_file
            mock_path.exists.return_value = True

            # Should raise exception for invalid JSON
            with pytest.raises(Exception, match="Invalid JSON"):
                await telemetry_service.read_gemini_settings()

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)


class TestConcurrentAccess:
    """Test concurrent access scenarios"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_multiple_configure_calls(self, mock_path):
        """Test multiple configuration calls in sequence"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            settings_file = f.name
            json.dump({}, f)

        try:
            mock_path.__str__.return_value = settings_file
            mock_path.exists.return_value = True
            mock_path.parent.mkdir = MagicMock()

            # First configuration
            settings = {}
            await telemetry_service.configure_environment_variables_in_settings(
                settings, "proj-1", "proj-1"
            )
            await telemetry_service.write_gemini_settings(settings)

            # Second configuration (same project -> different)
            settings = await telemetry_service.read_gemini_settings()
            await telemetry_service.configure_environment_variables_in_settings(
                settings, "proj-2", "proj-3"
            )
            await telemetry_service.write_gemini_settings(settings)

            # Third configuration (different -> same)
            settings = await telemetry_service.read_gemini_settings()
            await telemetry_service.configure_environment_variables_in_settings(
                settings, "final", "final"
            )
            await telemetry_service.write_gemini_settings(settings)

            # Verify final state
            final_settings = await telemetry_service.read_gemini_settings()
            assert final_settings["env"]["GOOGLE_CLOUD_PROJECT"] == "final"
            assert "OTLP_GOOGLE_CLOUD_PROJECT" not in final_settings["env"]

        finally:
            if os.path.exists(settings_file):
                os.unlink(settings_file)
