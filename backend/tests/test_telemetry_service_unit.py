"""
Unit tests for Telemetry Service
Target: 80% code coverage for telemetry_service.py

Tests the dual-project configuration logic:
1. Environment variable configuration in settings.json
2. Shell profile configuration
3. Same vs different project scenarios
"""
import pytest
import json
import os
from unittest.mock import Mock, patch, mock_open, MagicMock
from pathlib import Path
from services import telemetry_service


class TestConfigureTelemetry:
    """Test main configure_telemetry function"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.configure_environment_variables_in_shell')
    @patch('services.telemetry_service.write_gemini_settings')
    @patch('services.telemetry_service.read_gemini_settings')
    async def test_configure_telemetry_same_project(
        self, mock_read, mock_write, mock_shell
    ):
        """Test telemetry configuration with same project for both"""
        mock_read.return_value = {}
        mock_shell.return_value = {"profile_file": "/home/user/.bashrc", "shell": "bash"}

        result = await telemetry_service.configure_telemetry(
            log_prompts=True,
            inference_project_id="my-project",
            telemetry_project_id="my-project"
        )

        assert result["enabled"] is True
        assert result["log_prompts"] is True
        assert "env_vars" in result
        assert "shell_profile" in result

        # Verify settings were written
        mock_write.assert_called_once()
        settings_arg = mock_write.call_args[0][0]
        assert settings_arg["telemetry"]["enabled"] is True
        assert settings_arg["telemetry"]["logPrompts"] is True
        assert settings_arg["env"]["GOOGLE_CLOUD_PROJECT"] == "my-project"
        assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings_arg["env"]

    @pytest.mark.asyncio
    @patch('services.telemetry_service.configure_environment_variables_in_shell')
    @patch('services.telemetry_service.write_gemini_settings')
    @patch('services.telemetry_service.read_gemini_settings')
    async def test_configure_telemetry_different_projects(
        self, mock_read, mock_write, mock_shell
    ):
        """Test telemetry configuration with different projects"""
        mock_read.return_value = {}
        mock_shell.return_value = {"profile_file": "/home/user/.bashrc", "shell": "bash"}

        result = await telemetry_service.configure_telemetry(
            log_prompts=False,
            inference_project_id="inference-project",
            telemetry_project_id="telemetry-project"
        )

        assert result["enabled"] is True
        assert result["log_prompts"] is False

        # Verify both environment variables were set
        settings_arg = mock_write.call_args[0][0]
        assert settings_arg["env"]["GOOGLE_CLOUD_PROJECT"] == "inference-project"
        assert settings_arg["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry-project"

    @pytest.mark.asyncio
    @patch('services.telemetry_service.configure_environment_variables_in_shell')
    @patch('services.telemetry_service.write_gemini_settings')
    @patch('services.telemetry_service.read_gemini_settings')
    async def test_configure_telemetry_preserves_existing_settings(
        self, mock_read, mock_write, mock_shell
    ):
        """Test that existing telemetry settings are preserved"""
        mock_read.return_value = {
            "telemetry": {
                "enabled": False,
                "target": "local",
                "customField": "preserved"
            }
        }
        mock_shell.return_value = {"profile_file": "/home/user/.bashrc"}

        await telemetry_service.configure_telemetry(
            log_prompts=True,
            inference_project_id="test-project",
            telemetry_project_id="test-project"
        )

        settings_arg = mock_write.call_args[0][0]
        assert settings_arg["telemetry"]["enabled"] is True
        assert settings_arg["telemetry"]["target"] == "gcp"
        assert settings_arg["telemetry"]["logPrompts"] is True

    @pytest.mark.asyncio
    @patch('services.telemetry_service.configure_environment_variables_in_shell')
    @patch('services.telemetry_service.write_gemini_settings')
    @patch('services.telemetry_service.read_gemini_settings')
    async def test_configure_telemetry_error_handling(
        self, mock_read, mock_write, mock_shell
    ):
        """Test error handling in configure_telemetry"""
        mock_read.side_effect = Exception("Failed to read settings")

        with pytest.raises(Exception, match="Failed to read settings"):
            await telemetry_service.configure_telemetry(
                log_prompts=True,
                inference_project_id="test",
                telemetry_project_id="test"
            )


class TestConfigureEnvironmentVariablesInSettings:
    """Test environment variable configuration in settings.json"""

    @pytest.mark.asyncio
    async def test_same_project_sets_single_var(self):
        """Test same project sets only GOOGLE_CLOUD_PROJECT"""
        settings = {}

        await telemetry_service.configure_environment_variables_in_settings(
            settings,
            "my-project",
            "my-project"
        )

        assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "my-project"
        assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

    @pytest.mark.asyncio
    async def test_different_projects_sets_both_vars(self):
        """Test different projects sets both environment variables"""
        settings = {}

        await telemetry_service.configure_environment_variables_in_settings(
            settings,
            "inference-proj",
            "telemetry-proj"
        )

        assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "inference-proj"
        assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry-proj"

    @pytest.mark.asyncio
    async def test_removes_otlp_var_when_switching_to_same_project(self):
        """Test OTLP variable is removed when switching to same project"""
        settings = {
            "env": {
                "GOOGLE_CLOUD_PROJECT": "old-project",
                "OTLP_GOOGLE_CLOUD_PROJECT": "old-telemetry-project"
            }
        }

        await telemetry_service.configure_environment_variables_in_settings(
            settings,
            "new-project",
            "new-project"
        )

        assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "new-project"
        assert "OTLP_GOOGLE_CLOUD_PROJECT" not in settings["env"]

    @pytest.mark.asyncio
    async def test_updates_existing_env_vars(self):
        """Test existing environment variables are updated"""
        settings = {
            "env": {
                "GOOGLE_CLOUD_PROJECT": "old-inference",
                "OTLP_GOOGLE_CLOUD_PROJECT": "old-telemetry"
            }
        }

        await telemetry_service.configure_environment_variables_in_settings(
            settings,
            "new-inference",
            "new-telemetry"
        )

        assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "new-inference"
        assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "new-telemetry"

    @pytest.mark.asyncio
    async def test_preserves_other_env_vars(self):
        """Test other environment variables are preserved"""
        settings = {
            "env": {
                "CUSTOM_VAR": "custom_value",
                "ANOTHER_VAR": "another_value"
            }
        }

        await telemetry_service.configure_environment_variables_in_settings(
            settings,
            "inference",
            "telemetry"
        )

        assert settings["env"]["CUSTOM_VAR"] == "custom_value"
        assert settings["env"]["ANOTHER_VAR"] == "another_value"
        assert settings["env"]["GOOGLE_CLOUD_PROJECT"] == "inference"
        assert settings["env"]["OTLP_GOOGLE_CLOUD_PROJECT"] == "telemetry"

    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Test error handling in settings configuration"""
        # Pass invalid settings object
        with pytest.raises(Exception):
            await telemetry_service.configure_environment_variables_in_settings(
                None,
                "inference",
                "telemetry"
            )


class TestConfigureEnvironmentVariablesInShell:
    """Test shell profile configuration"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_bash_shell_detection(self, mock_path_class):
        """Test bash shell is detected correctly"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = True
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        with patch('builtins.open', mock_open(read_data="")):
            result = await telemetry_service.configure_environment_variables_in_shell(
                "inference", "telemetry"
            )

        assert result["shell"] == "bash"
        assert ".bashrc" in result["profile_file"] or ".bash_profile" in result["profile_file"]

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/zsh'})
    async def test_zsh_shell_detection(self, mock_path_class):
        """Test zsh shell is detected correctly"""
        mock_home = MagicMock()
        mock_zshrc = MagicMock()
        mock_home.__truediv__.return_value = mock_zshrc
        mock_path_class.home.return_value = mock_home

        with patch('builtins.open', mock_open(read_data="")):
            result = await telemetry_service.configure_environment_variables_in_shell(
                "test", "test"
            )

        assert result["shell"] == "zsh"
        assert ".zshrc" in result["profile_file"]

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_same_project_single_export(self, mock_path_class):
        """Test same project creates single export statement"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = True
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        mock_file = mock_open(read_data="")
        with patch('builtins.open', mock_file):
            await telemetry_service.configure_environment_variables_in_shell(
                "my-project", "my-project"
            )

        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        assert 'export GOOGLE_CLOUD_PROJECT="my-project"' in written_content
        assert "OTLP_GOOGLE_CLOUD_PROJECT" not in written_content

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_different_projects_both_exports(self, mock_path_class):
        """Test different projects creates both export statements"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = True
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        mock_file = mock_open(read_data="")
        with patch('builtins.open', mock_file):
            await telemetry_service.configure_environment_variables_in_shell(
                "inference-proj", "telemetry-proj"
            )

        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        assert 'export GOOGLE_CLOUD_PROJECT="inference-proj"' in written_content
        assert 'export OTLP_GOOGLE_CLOUD_PROJECT="telemetry-proj"' in written_content

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_replaces_existing_block(self, mock_path_class):
        """Test existing Gemini CLI configuration block is replaced"""
        existing_content = """
# Some existing config
export SOME_VAR="value"

# >>> Gemini CLI Telemetry Configuration >>>
export GOOGLE_CLOUD_PROJECT="old-project"
export OTLP_GOOGLE_CLOUD_PROJECT="old-telemetry"
# <<< Gemini CLI Telemetry Configuration <<<

# More config
"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = True
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        mock_file = mock_open(read_data=existing_content)
        with patch('builtins.open', mock_file):
            await telemetry_service.configure_environment_variables_in_shell(
                "new-project", "new-project"
            )

        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)

        # Old values should be removed
        assert "old-project" not in written_content
        assert "old-telemetry" not in written_content

        # New value should be present
        assert 'export GOOGLE_CLOUD_PROJECT="new-project"' in written_content

        # Should not have OTLP variable (same project)
        assert "OTLP_GOOGLE_CLOUD_PROJECT" not in written_content or \
               'export OTLP_GOOGLE_CLOUD_PROJECT="new-project"' not in written_content

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_creates_file_if_not_exists(self, mock_path_class):
        """Test profile file is created if it doesn't exist"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = False
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        mock_file = mock_open()
        with patch('builtins.open', mock_file):
            result = await telemetry_service.configure_environment_variables_in_shell(
                "test", "test"
            )

        assert result["profile_file"] is not None

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {'SHELL': '/bin/bash'})
    async def test_error_handling_continues(self, mock_path_class):
        """Test errors in shell configuration don't raise (optional feature)"""
        mock_home = MagicMock()
        mock_home.__truediv__.side_effect = Exception("Permission denied")
        mock_path_class.home.return_value = mock_home

        # Should not raise, but return error in result
        result = await telemetry_service.configure_environment_variables_in_shell(
            "test", "test"
        )

        assert "error" in result
        assert result["profile_file"] is None

    @pytest.mark.asyncio
    @patch('services.telemetry_service.Path')
    @patch.dict(os.environ, {}, clear=True)
    async def test_defaults_to_bash_when_shell_not_set(self, mock_path_class):
        """Test defaults to bash when SHELL env var not set"""
        mock_home = MagicMock()
        mock_bashrc = MagicMock()
        mock_bashrc.exists.return_value = True
        mock_home.__truediv__.return_value = mock_bashrc
        mock_path_class.home.return_value = mock_home

        with patch('builtins.open', mock_open(read_data="")):
            result = await telemetry_service.configure_environment_variables_in_shell(
                "test", "test"
            )

        assert result["shell"] == "bash"


class TestReadGeminiSettings:
    """Test reading Gemini settings"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_read_existing_settings(self, mock_path):
        """Test reading existing settings file"""
        mock_path.exists.return_value = True
        settings_data = {"telemetry": {"enabled": True}}

        with patch('builtins.open', mock_open(read_data=json.dumps(settings_data))):
            result = await telemetry_service.read_gemini_settings()

        assert result == settings_data

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_read_nonexistent_settings(self, mock_path):
        """Test reading non-existent settings returns empty dict"""
        mock_path.exists.return_value = False

        result = await telemetry_service.read_gemini_settings()

        assert result == {}

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_read_invalid_json(self, mock_path):
        """Test reading invalid JSON raises exception"""
        mock_path.exists.return_value = True

        with patch('builtins.open', mock_open(read_data="invalid json")):
            with pytest.raises(Exception, match="Invalid JSON"):
                await telemetry_service.read_gemini_settings()


class TestWriteGeminiSettings:
    """Test writing Gemini settings"""

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_write_settings(self, mock_path):
        """Test writing settings to file"""
        mock_path.parent.mkdir = MagicMock()
        settings_data = {"telemetry": {"enabled": True}}

        mock_file = mock_open()
        with patch('builtins.open', mock_file):
            await telemetry_service.write_gemini_settings(settings_data)

        # Verify file was opened for writing
        mock_file.assert_called_once_with(mock_path, 'w')

        # Verify JSON was written with indent
        written_content = "".join(call.args[0] for call in mock_file().write.call_args_list)
        assert "telemetry" in written_content

    @pytest.mark.asyncio
    @patch('services.telemetry_service.GEMINI_SETTINGS_PATH')
    async def test_write_creates_directory(self, mock_path):
        """Test directory is created if it doesn't exist"""
        mock_parent = MagicMock()
        mock_path.parent = mock_parent

        with patch('builtins.open', mock_open()):
            await telemetry_service.write_gemini_settings({})

        mock_parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
