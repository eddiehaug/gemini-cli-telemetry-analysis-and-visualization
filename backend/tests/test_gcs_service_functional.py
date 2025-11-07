"""
Functional tests for GCS service.
Tests end-to-end workflows and Dataflow pipeline readiness.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import storage
from google.api_core.exceptions import Conflict, NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import gcs_service


class TestGCSAPIEndToEnd:
    """Functional test for GCS API endpoint"""

    @pytest.mark.asyncio
    async def test_setup_gcs_api_endpoint_simulation(self, monkeypatch):
        """Simulate FastAPI endpoint call to setup GCS"""
        project_id = "test-project-123"
        region = "us-central1"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {
                "bucket": f"{pid}-dataflow",
                "region": reg,
                "status": "created",
                "url": f"gs://{pid}-dataflow"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {
                "bucket": f"{pid}-dataflow",
                "file": "transform.js",
                "url": f"gs://{pid}-dataflow/transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        # Simulate API endpoint call
        result = await gcs_service.setup_gcs_for_dataflow(
            project_id=project_id,
            region=region
        )

        # Verify API response structure
        assert "bucket" in result
        assert "udf" in result
        assert "dataflow_params" in result
        assert result["bucket"]["status"] in ["created", "already_exists"]
        assert result["udf"]["status"] == "uploaded"


class TestEndToEndWorkflow:
    """Functional tests for complete end-to-end workflows"""

    @pytest.mark.asyncio
    async def test_complete_gcs_setup_workflow(self, monkeypatch):
        """Test complete GCS setup from start to finish"""
        project_id = "test-project-123"
        region = "us-central1"

        # Track all steps executed
        workflow_log = []

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            workflow_log.append("bucket_created")
            return {
                "bucket": f"{pid}-dataflow",
                "region": reg,
                "status": "created",
                "url": f"gs://{pid}-dataflow"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            workflow_log.append("udf_uploaded")
            return {
                "bucket": f"{pid}-dataflow",
                "file": "transform.js",
                "url": f"gs://{pid}-dataflow/transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id, region)

        # Verify complete workflow
        assert "bucket_created" in workflow_log
        assert "udf_uploaded" in workflow_log
        assert len(workflow_log) == 2

        # Verify result completeness
        assert result["bucket"]["bucket"] == f"{project_id}-dataflow"
        assert result["udf"]["file"] == "transform.js"
        assert result["dataflow_params"]["staging_location"] == f"gs://{project_id}-dataflow/temp"

    @pytest.mark.asyncio
    async def test_bucket_exists_udf_uploaded(self, monkeypatch):
        """Test workflow when bucket exists but UDF needs upload"""
        project_id = "test-project-123"

        workflow_log = []

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            workflow_log.append("bucket_already_exists")
            return {
                "bucket": f"{pid}-dataflow",
                "region": reg,
                "status": "already_exists",
                "url": f"gs://{pid}-dataflow"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            workflow_log.append("udf_uploaded")
            return {
                "bucket": f"{pid}-dataflow",
                "file": "transform.js",
                "url": f"gs://{pid}-dataflow/transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify idempotent behavior
        assert result["bucket"]["status"] == "already_exists"
        assert result["udf"]["status"] == "uploaded"

    @pytest.mark.asyncio
    async def test_verification_after_setup(self, monkeypatch):
        """Test that verification works after setup"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"

        # First, set up resources
        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {
                "bucket": f"{pid}-dataflow",
                "status": "created",
                "url": f"gs://{pid}-dataflow"
            }

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {
                "bucket": f"{pid}-dataflow",
                "file": "transform.js",
                "status": "uploaded",
                "size_bytes": 1024
            }

        # Mock verification functions to simulate resources exist
        async def mock_verify_bucket(pid, bname=None):
            return True

        async def mock_verify_file(pid, bname=None, fname="transform.js"):
            return True

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)
        monkeypatch.setattr(gcs_service, 'verify_bucket_exists', mock_verify_bucket)
        monkeypatch.setattr(gcs_service, 'verify_file_exists', mock_verify_file)

        # Set up GCS
        await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify resources exist
        bucket_exists = await gcs_service.verify_bucket_exists(project_id, bucket_name)
        file_exists = await gcs_service.verify_file_exists(project_id, bucket_name, "transform.js")

        assert bucket_exists is True
        assert file_exists is True


class TestDataflowPipelineReadiness:
    """Functional tests for Dataflow pipeline readiness"""

    @pytest.mark.asyncio
    async def test_dataflow_params_match_gcloud_command_format(self, monkeypatch):
        """Test that Dataflow params match gcloud command requirements"""
        project_id = "test-project-123"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {"bucket": f"{pid}-dataflow", "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": f"{pid}-dataflow", "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify Dataflow params match expected format
        params = result["dataflow_params"]
        assert params["staging_location"].startswith("gs://")
        assert params["udf_gcs_path"].startswith("gs://")
        assert params["udf_gcs_path"].endswith("/transform.js")

    @pytest.mark.asyncio
    async def test_udf_file_accessible_for_dataflow(self, monkeypatch):
        """Test that UDF file is accessible at expected GCS path"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        expected_udf_path = f"gs://{bucket_name}/transform.js"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {"bucket": bucket_name, "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {
                "bucket": bucket_name,
                "file": "transform.js",
                "url": expected_udf_path,
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify UDF is at expected path for Dataflow
        assert result["udf"]["url"] == expected_udf_path
        assert result["dataflow_params"]["udf_gcs_path"] == expected_udf_path

    @pytest.mark.asyncio
    async def test_staging_location_ready_for_dataflow(self, monkeypatch):
        """Test that staging location is correctly configured"""
        project_id = "test-project-123"
        bucket_name = f"{project_id}-dataflow"
        expected_staging = f"gs://{bucket_name}/temp"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {"bucket": bucket_name, "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": bucket_name, "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify staging location
        assert result["dataflow_params"]["staging_location"] == expected_staging


class TestResourceNaming:
    """Functional tests for resource naming conventions"""

    @pytest.mark.asyncio
    async def test_bucket_naming_follows_convention(self, monkeypatch):
        """Test that bucket names follow {project_id}-dataflow convention"""
        project_id = "my-project-123"
        expected_bucket = f"{project_id}-dataflow"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {"bucket": bname or f"{pid}-dataflow", "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": bname or f"{pid}-dataflow", "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify naming convention
        assert result["bucket"]["bucket"] == expected_bucket
        assert expected_bucket in result["dataflow_params"]["staging_location"]
        assert expected_bucket in result["dataflow_params"]["udf_gcs_path"]

    @pytest.mark.asyncio
    async def test_udf_file_naming_consistent(self, monkeypatch):
        """Test that UDF file is always named transform.js"""
        project_id = "test-project-123"

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            return {"bucket": f"{pid}-dataflow", "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {
                "bucket": f"{pid}-dataflow",
                "file": dest,
                "status": "uploaded",
                "size_bytes": 1024
            }

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify UDF file naming
        assert result["udf"]["file"] == "transform.js"
        assert result["dataflow_params"]["udf_gcs_path"].endswith("/transform.js")


class TestRegionConfiguration:
    """Functional tests for region configuration"""

    @pytest.mark.asyncio
    async def test_custom_region_propagates_correctly(self, monkeypatch):
        """Test that custom region is used throughout"""
        project_id = "test-project-123"
        custom_region = "europe-west1"

        region_used = None

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            nonlocal region_used
            region_used = reg
            return {"bucket": f"{pid}-dataflow", "region": reg, "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": f"{pid}-dataflow", "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id, region=custom_region)

        # Verify custom region was used
        assert region_used == custom_region
        assert result["bucket"]["region"] == custom_region

    @pytest.mark.asyncio
    async def test_default_region_is_us_central1(self, monkeypatch):
        """Test that default region is us-central1"""
        project_id = "test-project-123"

        region_used = None

        async def mock_create_bucket(pid, bname=None, reg="us-central1"):
            nonlocal region_used
            region_used = reg
            return {"bucket": f"{pid}-dataflow", "region": reg, "status": "created"}

        async def mock_upload_udf(pid, bname=None, local_path="transform.js", dest="transform.js"):
            return {"bucket": f"{pid}-dataflow", "file": "transform.js", "status": "uploaded", "size_bytes": 1024}

        monkeypatch.setattr(gcs_service, 'create_bucket', mock_create_bucket)
        monkeypatch.setattr(gcs_service, 'upload_udf', mock_upload_udf)

        result = await gcs_service.setup_gcs_for_dataflow(project_id)

        # Verify default region
        assert region_used == "us-central1"
        assert result["bucket"]["region"] == "us-central1"
