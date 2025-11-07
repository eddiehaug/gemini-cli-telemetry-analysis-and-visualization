"""
Unit tests for BigQuery service.
Tests individual functions with mocked dependencies.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

# Import the service functions
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import bigquery_service


class TestCreateDataset:
    """Test create_dataset function"""

    @pytest.mark.asyncio
    async def test_create_dataset_success(self, mock_bigquery_client, monkeypatch):
        """Test successful dataset creation"""
        # Setup
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Mock BigQuery Client
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Mock dataset creation
            mock_dataset = Mock()
            mock_bigquery_client.create_dataset.return_value = mock_dataset

            # Mock create_minimal_logging_table
            async def mock_create_table(*args):
                pass
            monkeypatch.setattr(bigquery_service, 'create_minimal_logging_table', mock_create_table)

            # Mock enrich_table_metadata_internal
            async def mock_enrich_metadata(*args):
                pass
            monkeypatch.setattr(bigquery_service, 'enrich_table_metadata_internal', mock_enrich_metadata)

            # Execute
            result = await bigquery_service.create_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region,
                skip_table_creation=False
            )

            # Assert
            assert result["dataset"] == f"{project_id}.{dataset_name}"
            assert result["location"] == region
            assert result["table_created"] is True
            assert result["metadata_enriched"] is True
            mock_bigquery_client.create_dataset.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_dataset_skip_table_creation(self, mock_bigquery_client):
        """Test dataset creation with skip_table_creation=True"""
        # Setup
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_dataset = Mock()
            mock_bigquery_client.create_dataset.return_value = mock_dataset

            # Execute
            result = await bigquery_service.create_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region,
                skip_table_creation=True
            )

            # Assert
            assert result["table_created"] is False
            assert result["metadata_enriched"] is False
            assert "experiment mode" in result["note"].lower()

    @pytest.mark.asyncio
    async def test_create_dataset_already_exists(self, mock_bigquery_client, monkeypatch):
        """Test dataset creation when dataset already exists"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Mock Conflict exception
            mock_bigquery_client.create_dataset.side_effect = Conflict("Dataset exists")

            # Mock table creation functions
            async def mock_create_table(*args):
                pass
            monkeypatch.setattr(bigquery_service, 'create_minimal_logging_table', mock_create_table)

            async def mock_enrich_metadata(*args):
                pass
            monkeypatch.setattr(bigquery_service, 'enrich_table_metadata_internal', mock_enrich_metadata)

            # Execute
            result = await bigquery_service.create_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region
            )

            # Assert - should still succeed and create table
            assert result["dataset"] == f"{project_id}.{dataset_name}"
            assert result["table_created"] is True


class TestCreateMinimalLoggingTable:
    """Test create_minimal_logging_table function - refactored for ELT pattern"""

    @pytest.mark.asyncio
    async def test_create_table_with_json_string_schema(self, mock_bigquery_client):
        """Test table creation with JSON string schema (unbreakable schema)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Execute
        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Assert
        mock_bigquery_client.create_table.assert_called_once()

        # Get the table object that was passed to create_table
        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify table name is gemini_raw_logs
        assert "gemini_raw_logs" in str(table_arg.table_id)

        # Verify schema has JSON string fields
        schema_fields = {field.name: field.field_type for field in table_arg.schema}

        # Verify simple fields are typed correctly
        assert schema_fields["timestamp"] == "TIMESTAMP"
        assert schema_fields["receiveTimestamp"] == "TIMESTAMP"
        assert schema_fields["logName"] == "STRING"
        assert schema_fields["insertId"] == "STRING"
        assert schema_fields["severity"] == "STRING"

        # Verify complex fields are ALL STRING (not STRUCT or JSON)
        assert schema_fields["resource_json"] == "STRING"
        assert schema_fields["labels_json"] == "STRING"
        assert schema_fields["operation_json"] == "STRING"
        assert schema_fields["httpRequest_json"] == "STRING"
        assert schema_fields["jsonPayload_json"] == "STRING"

        # Verify partitioning
        assert table_arg.time_partitioning is not None
        assert table_arg.time_partitioning.field == "timestamp"

        # Verify clustering
        assert "logName" in table_arg.clustering_fields
        assert "severity" in table_arg.clustering_fields

        # Verify table description warns against direct querying
        assert "DO NOT QUERY DIRECTLY" in table_arg.description

    @pytest.mark.asyncio
    async def test_table_schema_field_count(self, mock_bigquery_client):
        """Test that schema has correct number of fields"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Should have 12 fields total:
        # 5 simple fields (timestamp, receiveTimestamp, logName, insertId, severity)
        # 5 JSON string fields (resource_json, labels_json, operation_json, httpRequest_json, jsonPayload_json)
        # 2 trace fields (trace, spanId)
        assert len(table_arg.schema) == 12

    @pytest.mark.asyncio
    async def test_no_150_second_wait(self, mock_bigquery_client, monkeypatch):
        """Test that 150-second streaming API wait is NOT present (removed for Dataflow)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Track if asyncio.sleep is called
        sleep_calls = []

        async def mock_sleep(seconds):
            sleep_calls.append(seconds)

        import asyncio
        monkeypatch.setattr(asyncio, 'sleep', mock_sleep)

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Assert NO sleep calls (old code had 150-second wait)
        assert len(sleep_calls) == 0


class TestVerifyTableExists:
    """Test verify_table_exists function"""

    @pytest.mark.asyncio
    async def test_table_exists(self, mock_bigquery_client):
        """Test when table exists"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_table = Mock()
            mock_bigquery_client.get_table.return_value = mock_table

            result = await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is True
            # Verify default table name is gemini_raw_logs
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert "gemini_raw_logs" in call_args

    @pytest.mark.asyncio
    async def test_table_not_found(self, mock_bigquery_client):
        """Test when table doesn't exist"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.side_effect = NotFound("Table not found")

            result = await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_custom_table_name(self, mock_bigquery_client):
        """Test with custom table name"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        custom_table = "custom_table"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.return_value = Mock()

            result = await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name,
                table_name=custom_table
            )

            assert result is True
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert custom_table in call_args


class TestEnrichTableMetadata:
    """Test enrich_table_metadata_internal function"""

    @pytest.mark.asyncio
    async def test_enriches_field_descriptions(self, mock_bigquery_client):
        """Test that field descriptions are added for JSON string fields"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Mock table with schema
        mock_table = Mock()
        mock_table.schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("resource_json", "STRING"),
            bigquery.SchemaField("labels_json", "STRING"),
            bigquery.SchemaField("jsonPayload_json", "STRING"),
        ]
        mock_bigquery_client.get_table.return_value = mock_table

        await bigquery_service.enrich_table_metadata_internal(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Assert update_table was called
        mock_bigquery_client.update_table.assert_called_once()

        # Get updated table
        updated_table = mock_bigquery_client.update_table.call_args[0][0]

        # Verify descriptions were added
        for field in updated_table.schema:
            assert field.description is not None
            assert len(field.description) > 0

            # JSON fields should mention how to parse them
            if field.name.endswith("_json"):
                assert "JSON_VALUE" in field.description or "JSON" in field.description

    @pytest.mark.asyncio
    async def test_table_description_elt_pattern(self, mock_bigquery_client):
        """Test that table description reflects ELT pattern"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_table = Mock()
        mock_table.schema = [bigquery.SchemaField("timestamp", "TIMESTAMP")]
        mock_bigquery_client.get_table.return_value = mock_table

        await bigquery_service.enrich_table_metadata_internal(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        updated_table = mock_bigquery_client.update_table.call_args[0][0]
        description = updated_table.description

        # Verify ELT pattern is documented
        assert "ELT" in description or "Extract, Load, Transform" in description.lower()
        assert "Dataflow" in description
        assert "Pub/Sub" in description
        assert "gemini_analytics_view" in description
        assert "DO NOT QUERY DIRECTLY" in description

    @pytest.mark.asyncio
    async def test_uses_gemini_raw_logs_table_name(self, mock_bigquery_client):
        """Test that function uses gemini_raw_logs table name"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_table = Mock()
        mock_table.schema = []
        mock_bigquery_client.get_table.return_value = mock_table

        await bigquery_service.enrich_table_metadata_internal(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Verify get_table was called with gemini_raw_logs
        call_args = mock_bigquery_client.get_table.call_args[0][0]
        assert "gemini_raw_logs" in call_args


class TestVerifyDatasetExists:
    """Test verify_dataset_exists function"""

    @pytest.mark.asyncio
    async def test_dataset_exists(self, mock_bigquery_client):
        """Test when dataset exists"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_dataset = Mock()
            mock_bigquery_client.get_dataset.return_value = mock_dataset

            result = await bigquery_service.verify_dataset_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is True

    @pytest.mark.asyncio
    async def test_dataset_not_found(self, mock_bigquery_client):
        """Test when dataset doesn't exist"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_dataset.side_effect = NotFound("Dataset not found")

            result = await bigquery_service.verify_dataset_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_dataset_error(self, mock_bigquery_client):
        """Test when there's an error checking dataset"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_dataset.side_effect = Exception("API Error")

            result = await bigquery_service.verify_dataset_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is False


class TestCreateAnalyticsView:
    """Test create_analytics_view function"""

    @pytest.mark.asyncio
    async def test_create_view_success(self, mock_bigquery_client):
        """Test successful analytics view creation"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Mock query job
            mock_query_job = Mock()
            mock_query_job.result = Mock()
            mock_bigquery_client.query.return_value = mock_query_job

            result = await bigquery_service.create_analytics_view(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Verify query was called
            assert mock_bigquery_client.query.called

            # Verify result
            assert result["view"] == f"{project_id}.{dataset_name}.gemini_analytics_view"
            assert result["status"] == "created"
            assert result["fields_extracted"] == 30

    @pytest.mark.asyncio
    async def test_create_view_sql_contains_json_parsing(self, mock_bigquery_client):
        """Test that view SQL contains JSON parsing logic"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_query_job = Mock()
            mock_query_job.result = Mock()
            mock_bigquery_client.query.return_value = mock_query_job

            await bigquery_service.create_analytics_view(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Get the SQL query that was executed
            sql_query = mock_bigquery_client.query.call_args[0][0]

            # Verify it contains schema-on-read functions
            assert "SAFE.PARSE_JSON" in sql_query
            assert "JSON_VALUE" in sql_query

            # Verify it references raw table
            assert "gemini_raw_logs" in sql_query

            # Verify it extracts key telemetry fields
            assert "event_name" in sql_query
            assert "session_id" in sql_query
            assert "model" in sql_query
            assert "input_tokens" in sql_query
            assert "output_tokens" in sql_query

    @pytest.mark.asyncio
    async def test_create_view_failure_raises_exception(self, mock_bigquery_client):
        """Test view creation failure raises exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.query.side_effect = Exception("Query failed")

            with pytest.raises(Exception) as exc_info:
                await bigquery_service.create_analytics_view(
                    project_id=project_id,
                    dataset_name=dataset_name
                )

            assert "Query failed" in str(exc_info.value) or "Failed to create" in str(exc_info.value)


class TestVerifyViewExists:
    """Test verify_view_exists function"""

    @pytest.mark.asyncio
    async def test_view_exists(self, mock_bigquery_client):
        """Test when view exists"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_view = Mock()
            mock_bigquery_client.get_table.return_value = mock_view

            result = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is True
            # Verify default view name is gemini_analytics_view
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert "gemini_analytics_view" in call_args

    @pytest.mark.asyncio
    async def test_view_not_found(self, mock_bigquery_client):
        """Test when view doesn't exist"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.side_effect = NotFound("View not found")

            result = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_custom_view_name(self, mock_bigquery_client):
        """Test with custom view name"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        custom_view = "custom_view"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.return_value = Mock()

            result = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name,
                view_name=custom_view
            )

            assert result is True
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert custom_view in call_args
