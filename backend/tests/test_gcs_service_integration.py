"""
Integration tests for GCS service.
Tests complete workflows with mocked GCP services.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import storage
from google.api_core.exceptions import Conflict, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import gcs_service


class TestBucketCreationWorkflow:
    """Integration tests for bucket creation workflow"""

    @pytest.mark.asyncio
    async def test_complete_bucket_creation_workflow(self):
        """Test complete bucket creation workflow"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        region = "us-central1"

        # Track workflow steps
        workflow_steps = []

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_bucket.iam_configuration = Mock()
        mock_bucket.iam_configuration.uniform_bucket_level_access_enabled = False

        def mock_create_bucket(bname, location=None):
            workflow_steps.append("bucket_created")
            assert bname == bucket_name
            assert location == region
            return mock_bucket

        def mock_patch():
            workflow_steps.append("permissions_updated")

        mock_storage_client.create_bucket = mock_create_bucket
        mock_bucket.patch = mock_patch

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id, bucket_name, region)

            # Verify workflow completed
            assert "bucket_created" in workflow_steps
            assert "permissions_updated" in workflow_steps
            assert result["status"] == "created"
            assert result["bucket"] == bucket_name

    @pytest.mark.asyncio
    async def test_bucket_uniform_access_enabled(self):
        """Test that uniform bucket-level access is enabled"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_bucket.iam_configuration = Mock()
        mock_bucket.iam_configuration.uniform_bucket_level_access_enabled = False

        mock_storage_client.create_bucket.return_value = mock_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id, bucket_name)

            # Verify uniform access was enabled
            assert mock_bucket.iam_configuration.uniform_bucket_level_access_enabled == True
            assert mock_bucket.patch.called


class TestUDFUploadWorkflow:
    """Integration tests for UDF upload workflow"""

    @pytest.mark.asyncio
    async def test_complete_udf_upload_workflow(self):
        """Test complete UDF upload workflow"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        local_file_path = "transform.js"

        # Track workflow steps
        workflow_steps = []

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()

        def mock_upload_from_filename(path):
            workflow_steps.append("file_uploaded")
            assert path == local_file_path

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename = mock_upload_from_filename

        with patch('os.path.exists', return_value=True):
            with patch('os.path.getsize', return_value=1024):
                with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                    result = await gcs_service.upload_udf(project_id, bucket_name, local_file_path)

                    # Verify workflow completed
                    assert "file_uploaded" in workflow_steps
                    assert result["status"] == "uploaded"
                    assert result["file"] == "transform.js"

    @pytest.mark.asyncio
    async def test_udf_upload_validates_local_file(self):
        """Test that UDF upload validates local file existence"""
        project_id = "test-project-123"
        local_file_path = "nonexistent.js"

        mock_storage_client = Mock()

        with patch('os.path.exists', return_value=False):
            with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                with pytest.raises(FileNotFoundError) as exc_info:
                    await gcs_service.upload_udf(project_id, None, local_file_path)

                assert "UDF file not found" in str(exc_info.value)


class TestVerificationWorkflows:
    """Integration tests for verification workflows"""

    @pytest.mark.asyncio
    async def test_verify_bucket_workflow(self):
        """Test bucket verification workflow"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        mock_storage_client = Mock()
        mock_bucket = Mock()

        # Track reload calls
        reload_called = False

        def mock_reload():
            nonlocal reload_called
            reload_called = True

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.reload = mock_reload

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_bucket_exists(project_id, bucket_name)

            # Verify reload was called
            assert reload_called
            assert result is True

    @pytest.mark.asyncio
    async def test_verify_file_workflow(self):
        """Test file verification workflow"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        file_name = "transform.js"

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()

        # Track blob.exists calls
        exists_called = False

        def mock_exists():
            nonlocal exists_called
            exists_called = True
            return True

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists = mock_exists

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_file_exists(project_id, bucket_name, file_name)

            # Verify exists was called
            assert exists_called
            assert result is True


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_bucket_creation_failure_raises_exception(self):
        """Test that bucket creation failure raises exception"""
        project_id = "test-project-123"

        mock_storage_client = Mock()
        mock_storage_client.create_bucket.side_effect = Exception("API Error")

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            with pytest.raises(Exception) as exc_info:
                await gcs_service.create_bucket(project_id)

            assert "API Error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_udf_upload_failure_raises_exception(self):
        """Test that UDF upload failure raises exception"""
        project_id = "test-project-123"

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.upload_from_filename.side_effect = Exception("Upload error")

        with patch('os.path.exists', return_value=True):
            with patch('os.path.getsize', return_value=1024):
                with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                    with pytest.raises(Exception) as exc_info:
                        await gcs_service.upload_udf(project_id)

                    assert "Upload error" in str(exc_info.value)


class TestFullSetupWorkflow:
    """Integration tests for setup_gcs_for_dataflow convenience function"""

    @pytest.mark.asyncio
    async def test_creates_all_resources_in_order(self, monkeypatch):
        """Test that all resources are created in correct order"""
        project_id = "test-project-123"

        # Track execution order
        execution_order = []

        async def mock_create_bucket(pid, bname=None, region="us-central1"):
            execution_order.append("bucket")
            return {
                "bucket": f"{pid}-dataflow",
                "status": "created"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            execution_order.append("udf")
            return {
                "bucket": f"{pid}-dataflow",
                "file": "transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify execution order: bucket must be created before UDF upload
        assert execution_order == ["bucket", "udf"]

        # Verify result structure
        assert "bucket" in result
        assert "udf" in result
        assert "dataflow_params" in result

    @pytest.mark.asyncio
    async def test_includes_all_dataflow_params(self, monkeypatch):
        """Test that all Dataflow parameters are included"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        async def mock_create_bucket(pid, bname=None, region="us-central1"):
            return {"bucket": bucket_name, "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": bucket_name, "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify Dataflow params exist and have correct values
        assert "dataflow_params" in result
        params = result["dataflow_params"]
        assert "staging_location" in params
        assert "udf_gcs_path" in params
        assert params["staging_location"] == f"gs://{bucket_name}/temp"
        assert params["udf_gcs_path"] == f"gs://{bucket_name}/transform.js"


class TestBucketNaming:
    """Integration tests for bucket naming conventions"""

    @pytest.mark.asyncio
    async def test_default_bucket_naming_convention(self):
        """Test that default bucket name follows {project_id}-dataflow convention"""
        project_id = "test-project-123"
        expected_bucket_name = f"{project_id}-dataflow"

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_bucket.iam_configuration = Mock()
        mock_bucket.iam_configuration.uniform_bucket_level_access_enabled = False

        bucket_name_captured = None

        def mock_create_bucket(bname, location=None):
            nonlocal bucket_name_captured
            bucket_name_captured = bname
            return mock_bucket

        mock_storage_client.create_bucket = mock_create_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id)

            # Verify bucket naming convention
            assert bucket_name_captured == expected_bucket_name
            assert result["bucket"] == expected_bucket_name

    @pytest.mark.asyncio
    async def test_custom_bucket_name_accepted(self):
        """Test that custom bucket names are accepted"""
        project_id = "test-project-123"
        custom_bucket_name = "my-custom-bucket"

        mock_storage_client = Mock()
        mock_bucket = Mock()
        mock_bucket.iam_configuration = Mock()
        mock_bucket.iam_configuration.uniform_bucket_level_access_enabled = False

        mock_storage_client.create_bucket.return_value = mock_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id, custom_bucket_name)

            # Verify custom name was used
            assert result["bucket"] == custom_bucket_name
