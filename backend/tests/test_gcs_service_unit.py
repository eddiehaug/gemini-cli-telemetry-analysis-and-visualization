"""
Unit tests for GCS service.
Tests individual functions with mocked dependencies.
"""
import pytest
import os
from unittest.mock import Mock, MagicMock, patch, AsyncMock, mock_open
from google.cloud import storage
from google.api_core.exceptions import Conflict, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import gcs_service


@pytest.fixture
def mock_storage_client():
    """Mock Cloud Storage Client."""
    mock_client = Mock(spec=storage.Client)
    return mock_client


@pytest.fixture
def mock_bucket():
    """Mock Cloud Storage Bucket."""
    mock = Mock(spec=storage.Bucket)
    mock.iam_configuration = Mock()
    mock.iam_configuration.uniform_bucket_level_access_enabled = False
    return mock


@pytest.fixture
def mock_blob():
    """Mock Cloud Storage Blob."""
    mock = Mock(spec=storage.Blob)
    return mock


class TestCreateBucket:
    """Test create_bucket function"""

    @pytest.mark.asyncio
    async def test_create_bucket_success(self, mock_storage_client, mock_bucket):
        """Test successful bucket creation"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        region = "us-central1"

        # Mock bucket creation
        mock_storage_client.create_bucket.return_value = mock_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id, bucket_name, region)

            # Verify create_bucket was called
            assert mock_storage_client.create_bucket.called
            call_args = mock_storage_client.create_bucket.call_args[0]
            assert call_args[0] == bucket_name
            call_kwargs = mock_storage_client.create_bucket.call_args[1]
            assert call_kwargs["location"] == region

            # Verify uniform bucket-level access was enabled
            assert mock_bucket.iam_configuration.uniform_bucket_level_access_enabled == True
            assert mock_bucket.patch.called

            # Verify result
            assert result["bucket"] == bucket_name
            assert result["region"] == region
            assert result["status"] == "created"
            assert result["url"] == f"gs://{bucket_name}"

    @pytest.mark.asyncio
    async def test_create_bucket_already_exists(self, mock_storage_client):
        """Test bucket creation when bucket already exists"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        # Mock Conflict exception (bucket already exists)
        mock_storage_client.create_bucket.side_effect = Conflict("Bucket exists")

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id, bucket_name)

            # Verify result indicates already exists
            assert result["status"] == "already_exists"
            assert result["bucket"] == bucket_name
            assert bucket_name in result["url"]

    @pytest.mark.asyncio
    async def test_create_bucket_default_name(self, mock_storage_client, mock_bucket):
        """Test bucket creation with default bucket name"""
        project_id = "test-project-123"
        expected_bucket_name = f"{project_id}-dataflow"

        mock_storage_client.create_bucket.return_value = mock_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.create_bucket(project_id)

            # Verify default bucket name was used
            assert result["bucket"] == expected_bucket_name
            assert expected_bucket_name in result["url"]


class TestUploadUDF:
    """Test upload_udf function"""

    @pytest.mark.asyncio
    async def test_upload_udf_success(self, mock_storage_client, mock_bucket, mock_blob):
        """Test successful UDF upload"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        local_file_path = "transform.js"
        file_size = 1024

        # Mock file existence and size
        with patch('os.path.exists', return_value=True):
            with patch('os.path.getsize', return_value=file_size):
                mock_storage_client.bucket.return_value = mock_bucket
                mock_bucket.blob.return_value = mock_blob

                with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                    result = await gcs_service.upload_udf(project_id, bucket_name, local_file_path)

                    # Verify upload was called
                    assert mock_blob.upload_from_filename.called
                    assert mock_blob.upload_from_filename.call_args[0][0] == local_file_path

                    # Verify result
                    assert result["bucket"] == bucket_name
                    assert result["file"] == "transform.js"
                    assert result["status"] == "uploaded"
                    assert result["size_bytes"] == file_size
                    assert f"gs://{bucket_name}/transform.js" in result["url"]

    @pytest.mark.asyncio
    async def test_upload_udf_file_not_found(self, mock_storage_client):
        """Test UDF upload when local file doesn't exist"""
        project_id = "test-project-123"
        local_file_path = "nonexistent.js"

        with patch('os.path.exists', return_value=False):
            with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                with pytest.raises(FileNotFoundError) as exc_info:
                    await gcs_service.upload_udf(project_id, None, local_file_path)

                assert "UDF file not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_upload_udf_custom_destination(self, mock_storage_client, mock_bucket, mock_blob):
        """Test UDF upload with custom destination blob name"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        local_file_path = "transform.js"
        destination_blob_name = "custom/path/transform.js"

        with patch('os.path.exists', return_value=True):
            with patch('os.path.getsize', return_value=1024):
                mock_storage_client.bucket.return_value = mock_bucket
                mock_bucket.blob.return_value = mock_blob

                with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
                    result = await gcs_service.upload_udf(
                        project_id,
                        bucket_name,
                        local_file_path,
                        destination_blob_name
                    )

                    # Verify custom destination was used
                    assert mock_bucket.blob.call_args[0][0] == destination_blob_name
                    assert result["file"] == destination_blob_name


class TestVerifyBucketExists:
    """Test verify_bucket_exists function"""

    @pytest.mark.asyncio
    async def test_bucket_exists(self, mock_storage_client, mock_bucket):
        """Test when bucket exists"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        mock_storage_client.bucket.return_value = mock_bucket

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_bucket_exists(project_id, bucket_name)

            # Verify bucket.reload() was called
            assert mock_bucket.reload.called
            assert result is True

    @pytest.mark.asyncio
    async def test_bucket_not_found(self, mock_storage_client, mock_bucket):
        """Test when bucket doesn't exist"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.reload.side_effect = NotFound("Bucket not found")

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_bucket_exists(project_id, bucket_name)

            assert result is False


class TestVerifyFileExists:
    """Test verify_file_exists function"""

    @pytest.mark.asyncio
    async def test_file_exists(self, mock_storage_client, mock_bucket, mock_blob):
        """Test when file exists"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        file_name = "transform.js"

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = True

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_file_exists(project_id, bucket_name, file_name)

            # Verify blob.exists() was called
            assert mock_blob.exists.called
            assert result is True

    @pytest.mark.asyncio
    async def test_file_not_found(self, mock_storage_client, mock_bucket, mock_blob):
        """Test when file doesn't exist"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        file_name = "transform.js"

        mock_storage_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_blob.exists.return_value = False

        with patch('services.gcs_service.storage.Client', return_value=mock_storage_client):
            result = await gcs_service.verify_file_exists(project_id, bucket_name, file_name)

            assert result is False


class TestSetupGCSForDataflow:
    """Test setup_gcs_for_dataflow convenience function"""

    @pytest.mark.asyncio
    async def test_creates_all_resources(self, monkeypatch):
        """Test that function creates bucket and uploads UDF"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        # Track function calls
        bucket_created = False
        udf_uploaded = False

        async def mock_create_bucket(pid, bname=None, region="us-central1"):
            nonlocal bucket_created
            bucket_created = True
            return {
                "bucket": bucket_name,
                "region": region,
                "status": "created",
                "url": f"gs://{bucket_name}"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            nonlocal udf_uploaded
            udf_uploaded = True
            return {
                "bucket": bucket_name,
                "file": "transform.js",
                "url": f"gs://{bucket_name}/transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify both resources created
        assert bucket_created
        assert udf_uploaded
        assert "bucket" in result
        assert "udf" in result
        assert "dataflow_params" in result

    @pytest.mark.asyncio
    async def test_includes_dataflow_params(self, monkeypatch):
        """Test that result includes Dataflow-specific parameters"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        async def mock_create_bucket(pid, bname=None, region="us-central1"):
            return {
                "bucket": bucket_name,
                "region": region,
                "status": "created",
                "url": f"gs://{bucket_name}"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {
                "bucket": bucket_name,
                "file": "transform.js",
                "url": f"gs://{bucket_name}/transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify Dataflow params
        assert "dataflow_params" in result
        assert result["dataflow_params"]["staging_location"] == f"gs://{bucket_name}/temp"
        assert result["dataflow_params"]["udf_gcs_path"] == f"gs://{bucket_name}/transform.js"
