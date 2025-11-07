"""
Unit tests for config_service.py
"""
import pytest
from pydantic import BaseModel
from services import config_service


class MockConfig(BaseModel):
    """Mock config for testing"""
    projectId: str
    region: str
    datasetName: str
    logPrompts: bool


class TestValidateConfig:
    """Test validate_config function"""

    @pytest.mark.asyncio
    async def test_valid_config(self):
        """Test with valid configuration"""
        config = MockConfig(
            projectId="test-project-123",
            region="us-central1",
            datasetName="test_dataset",
            logPrompts=False
        )

        result = await config_service.validate_config(config)
        assert result is True

    @pytest.mark.asyncio
    async def test_invalid_project_id_empty(self):
        """Test with empty project ID"""
        config = MockConfig(
            projectId="",
            region="us-central1",
            datasetName="test_dataset",
            logPrompts=False
        )

        with pytest.raises(Exception, match="Project ID is required"):
            await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_invalid_project_id_format(self):
        """Test with invalid project ID format"""
        invalid_ids = [
            "Test-Project",  # Uppercase
            "t",  # Too short
            "test_project",  # Underscore
            "123-project",  # Starts with number
            "test-project-" * 10,  # Too long
        ]

        for project_id in invalid_ids:
            config = MockConfig(
                projectId=project_id,
                region="us-central1",
                datasetName="test_dataset",
                logPrompts=False
            )

            with pytest.raises(Exception, match="Invalid project ID format"):
                await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_valid_project_id_edge_cases(self):
        """Test valid project ID edge cases"""
        valid_ids = [
            "a12345",  # Minimum length
            "test-project-" + "x" * 15,  # Near max length
            "my-test-project-123",
        ]

        for project_id in valid_ids:
            config = MockConfig(
                projectId=project_id,
                region="us-central1",
                datasetName="test_dataset",
                logPrompts=False
            )

            result = await config_service.validate_config(config)
            assert result is True

    @pytest.mark.asyncio
    async def test_invalid_dataset_name_empty(self):
        """Test with empty dataset name"""
        config = MockConfig(
            projectId="test-project-123",
            region="us-central1",
            datasetName="",
            logPrompts=False
        )

        with pytest.raises(Exception, match="Dataset name is required"):
            await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_invalid_dataset_name_format(self):
        """Test with invalid dataset name format"""
        invalid_names = [
            "123dataset",  # Starts with number
            "test-dataset",  # Hyphen
            "test dataset",  # Space
            "test@dataset",  # Special char
        ]

        for dataset_name in invalid_names:
            config = MockConfig(
                projectId="test-project-123",
                region="us-central1",
                datasetName=dataset_name,
                logPrompts=False
            )

            with pytest.raises(Exception, match="Invalid dataset name"):
                await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_valid_dataset_names(self):
        """Test valid dataset names"""
        valid_names = [
            "test_dataset",
            "_test_dataset",
            "TestDataset",
            "test123",
            "my_dataset_2024",
        ]

        for dataset_name in valid_names:
            config = MockConfig(
                projectId="test-project-123",
                region="us-central1",
                datasetName=dataset_name,
                logPrompts=False
            )

            result = await config_service.validate_config(config)
            assert result is True

    @pytest.mark.asyncio
    async def test_invalid_region_empty(self):
        """Test with empty region"""
        config = MockConfig(
            projectId="test-project-123",
            region="",
            datasetName="test_dataset",
            logPrompts=False
        )

        with pytest.raises(Exception, match="Region is required"):
            await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_invalid_region_format(self):
        """Test with invalid region"""
        config = MockConfig(
            projectId="test-project-123",
            region="invalid-region",
            datasetName="test_dataset",
            logPrompts=False
        )

        with pytest.raises(Exception, match="Invalid region"):
            await config_service.validate_config(config)

    @pytest.mark.asyncio
    async def test_valid_regions(self):
        """Test all valid regions"""
        valid_regions = [
            'us-central1', 'us-east1', 'us-west1',
            'europe-west1', 'europe-west2',
            'asia-east1', 'asia-northeast1',
        ]

        for region in valid_regions:
            config = MockConfig(
                projectId="test-project-123",
                region=region,
                datasetName="test_dataset",
                logPrompts=False
            )

            result = await config_service.validate_config(config)
            assert result is True

    @pytest.mark.asyncio
    async def test_multiple_validation_errors(self):
        """Test that multiple errors are combined"""
        config = MockConfig(
            projectId="",
            region="invalid",
            datasetName="",
            logPrompts=False
        )

        with pytest.raises(Exception) as exc_info:
            await config_service.validate_config(config)

        error_msg = str(exc_info.value)
        assert "Project ID is required" in error_msg
        assert "Dataset name is required" in error_msg
        assert "Invalid region" in error_msg
