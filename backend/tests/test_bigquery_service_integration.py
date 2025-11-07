"""
Integration tests for BigQuery service.
Tests complete workflows with mocked GCP services.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock, call
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services import bigquery_service


class TestDatasetAndTableCreationWorkflow:
    """Integration tests for complete dataset and table creation workflow"""

    @pytest.mark.asyncio
    async def test_complete_deployment_workflow(self, mock_bigquery_client, monkeypatch):
        """Test complete workflow: create dataset → create table → enrich metadata"""
        # Setup
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Track function calls
        create_table_called = False
        enrich_metadata_called = False

        async def mock_create_table(client, pid, dname):
            nonlocal create_table_called
            create_table_called = True
            assert pid == project_id
            assert dname == dataset_name

        async def mock_enrich_metadata(client, pid, dname):
            nonlocal enrich_metadata_called
            enrich_metadata_called = True
            assert pid == project_id
            assert dname == dataset_name

        monkeypatch.setattr(bigquery_service, 'create_minimal_logging_table', mock_create_table)
        monkeypatch.setattr(bigquery_service, 'enrich_table_metadata_internal', mock_enrich_metadata)

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_dataset = Mock()
            mock_bigquery_client.create_dataset.return_value = mock_dataset

            # Execute workflow
            result = await bigquery_service.create_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region,
                skip_table_creation=False
            )

            # Verify workflow execution
            assert mock_bigquery_client.create_dataset.called
            assert create_table_called
            assert enrich_metadata_called
            assert result["table_created"] is True
            assert result["metadata_enriched"] is True

    @pytest.mark.asyncio
    async def test_table_creation_calls_client_correctly(self, mock_bigquery_client):
        """Test that table creation calls BigQuery client with correct parameters"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Verify create_table was called
        assert mock_bigquery_client.create_table.called

        # Get the table parameter
        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify table configuration
        # Note: table_id in the Table object is just the table name, full path is in the reference
        assert "gemini_raw_logs" in str(table_arg.table_id)
        assert table_arg.time_partitioning.field == "timestamp"
        assert "logName" in table_arg.clustering_fields
        assert "severity" in table_arg.clustering_fields

    @pytest.mark.asyncio
    async def test_metadata_enrichment_workflow(self, mock_bigquery_client):
        """Test metadata enrichment workflow"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Mock existing table with schema
        mock_table = Mock()
        mock_table.schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("resource_json", "STRING"),
            bigquery.SchemaField("jsonPayload_json", "STRING"),
        ]
        mock_bigquery_client.get_table.return_value = mock_table

        # Execute
        await bigquery_service.enrich_table_metadata_internal(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        # Verify workflow
        assert mock_bigquery_client.get_table.called
        assert mock_bigquery_client.update_table.called

        # Verify table was updated with enriched metadata
        updated_table = mock_bigquery_client.update_table.call_args[0][0]
        assert updated_table.description is not None
        assert len(updated_table.description) > 100  # Should be comprehensive

        # Verify schema fields have descriptions
        for field in updated_table.schema:
            assert field.description is not None


class TestSchemaValidation:
    """Integration tests for schema validation"""

    @pytest.mark.asyncio
    async def test_schema_is_unbreakable_json_strings(self, mock_bigquery_client):
        """Test that schema uses JSON strings for all complex fields (unbreakable schema)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify NO STRUCT or RECORD fields (old schema used these)
        for field in table_arg.schema:
            assert field.field_type != "RECORD"
            assert field.field_type != "STRUCT"

        # Verify complex fields are STRING
        complex_fields = ["resource_json", "labels_json", "operation_json", "httpRequest_json", "jsonPayload_json"]
        schema_dict = {field.name: field.field_type for field in table_arg.schema}

        for complex_field in complex_fields:
            assert complex_field in schema_dict
            assert schema_dict[complex_field] == "STRING"

    @pytest.mark.asyncio
    async def test_schema_has_correct_field_types(self, mock_bigquery_client):
        """Test that schema has correct types for all fields"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        schema_dict = {field.name: field.field_type for field in table_arg.schema}

        # Expected schema types
        expected_types = {
            "timestamp": "TIMESTAMP",
            "receiveTimestamp": "TIMESTAMP",
            "logName": "STRING",
            "insertId": "STRING",
            "severity": "STRING",
            "resource_json": "STRING",
            "labels_json": "STRING",
            "operation_json": "STRING",
            "httpRequest_json": "STRING",
            "jsonPayload_json": "STRING",
            "trace": "STRING",
            "spanId": "STRING",
        }

        for field_name, expected_type in expected_types.items():
            assert field_name in schema_dict, f"Field {field_name} not found in schema"
            assert schema_dict[field_name] == expected_type, f"Field {field_name} has wrong type"

    @pytest.mark.asyncio
    async def test_schema_all_fields_nullable(self, mock_bigquery_client):
        """Test that all schema fields are NULLABLE (flexible ingestion)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # All fields should be NULLABLE for flexible ingestion
        for field in table_arg.schema:
            assert field.mode == "NULLABLE", f"Field {field.name} is not NULLABLE"


class TestELTPatternIntegration:
    """Integration tests verifying ELT pattern implementation"""

    @pytest.mark.asyncio
    async def test_table_name_is_gemini_raw_logs(self, mock_bigquery_client):
        """Test that table name is gemini_raw_logs (not gemini_cli)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        assert "gemini_raw_logs" in str(table_arg.table_id)
        assert "gemini_cli" not in str(table_arg.table_id)

    @pytest.mark.asyncio
    async def test_table_description_references_analytics_view(self, mock_bigquery_client):
        """Test that table description references the analytics view"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        assert "gemini_analytics_view" in table_arg.description

    @pytest.mark.asyncio
    async def test_table_description_warns_against_direct_querying(self, mock_bigquery_client):
        """Test that table description warns users not to query directly"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        assert "DO NOT QUERY DIRECTLY" in table_arg.description

    @pytest.mark.asyncio
    async def test_table_description_mentions_dataflow(self, mock_bigquery_client):
        """Test that table description mentions Dataflow (ELT pipeline)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        assert "Dataflow" in table_arg.description

    @pytest.mark.asyncio
    async def test_enriched_metadata_documents_elt_flow(self, mock_bigquery_client):
        """Test that enriched metadata documents the ELT flow"""
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

        # Verify ELT flow is documented
        assert "Pub/Sub" in description
        assert "Dataflow" in description
        assert "gemini-telemetry-topic" in description


class TestErrorHandling:
    """Integration tests for error handling"""

    @pytest.mark.asyncio
    async def test_dataset_creation_failure_raises_exception(self, mock_bigquery_client):
        """Test that dataset creation failure raises exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.create_dataset.side_effect = Exception("API Error")

            with pytest.raises(Exception) as exc_info:
                await bigquery_service.create_dataset(
                    project_id=project_id,
                    dataset_name=dataset_name,
                    region=region
                )

            assert "API Error" in str(exc_info.value) or "Dataset creation failed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_table_creation_failure_raises_exception(self, mock_bigquery_client):
        """Test that table creation failure raises exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_bigquery_client.create_table.side_effect = Exception("Table creation error")

        with pytest.raises(Exception) as exc_info:
            await bigquery_service.create_minimal_logging_table(
                client=mock_bigquery_client,
                project_id=project_id,
                dataset_name=dataset_name
            )

        assert "Table creation error" in str(exc_info.value) or "Failed to create" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_metadata_enrichment_failure_raises_exception(self, mock_bigquery_client):
        """Test that metadata enrichment failure raises exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_bigquery_client.get_table.side_effect = Exception("Table not found")

        with pytest.raises(Exception) as exc_info:
            await bigquery_service.enrich_table_metadata_internal(
                client=mock_bigquery_client,
                project_id=project_id,
                dataset_name=dataset_name
            )

        assert "Table not found" in str(exc_info.value) or "Failed to enrich" in str(exc_info.value)


class TestBackwardCompatibility:
    """Integration tests for backward compatibility and migration"""

    @pytest.mark.asyncio
    async def test_verify_table_exists_with_custom_table_name(self, mock_bigquery_client):
        """Test that custom table names still work (backward compatibility)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"
        custom_table = "legacy_table"

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

    @pytest.mark.asyncio
    async def test_default_table_name_is_gemini_raw_logs(self, mock_bigquery_client):
        """Test that default table name is gemini_raw_logs (not gemini_cli)"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.return_value = Mock()

            # Call without specifying table_name
            result = await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is True
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert "gemini_raw_logs" in call_args


class TestAnalyticsViewCreation:
    """Integration tests for analytics view creation"""

    @pytest.mark.asyncio
    async def test_create_analytics_view_workflow(self, mock_bigquery_client):
        """Test complete analytics view creation workflow"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_query_job = Mock()
            mock_query_job.result = Mock()
            mock_bigquery_client.query.return_value = mock_query_job

            result = await bigquery_service.create_analytics_view(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Verify query was executed
            assert mock_bigquery_client.query.called

            # Verify result structure
            assert "view" in result
            assert "status" in result
            assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_analytics_view_sql_structure(self, mock_bigquery_client):
        """Test that analytics view SQL has correct structure"""
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

            sql_query = mock_bigquery_client.query.call_args[0][0]

            # Verify CREATE OR REPLACE VIEW
            assert "CREATE OR REPLACE VIEW" in sql_query

            # Verify view name
            assert f"{project_id}.{dataset_name}.gemini_analytics_view" in sql_query

            # Verify FROM clause references raw table
            assert f"FROM `{project_id}.{dataset_name}.gemini_raw_logs`" in sql_query

    @pytest.mark.asyncio
    async def test_analytics_view_extracts_all_key_fields(self, mock_bigquery_client):
        """Test that analytics view extracts all important telemetry fields"""
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

            sql_query = mock_bigquery_client.query.call_args[0][0]

            # Verify event fields
            assert "event_name" in sql_query
            assert "event_domain" in sql_query

            # Verify session fields
            assert "session_id" in sql_query
            assert "session_duration_ms" in sql_query

            # Verify model fields
            assert "model" in sql_query
            assert "temperature" in sql_query
            assert "top_p" in sql_query
            assert "top_k" in sql_query

            # Verify token fields
            assert "input_tokens" in sql_query
            assert "output_tokens" in sql_query
            assert "cached_tokens" in sql_query
            assert "total_tokens" in sql_query

            # Verify function fields
            assert "function_name" in sql_query
            assert "function_type" in sql_query
            assert "function_duration_ms" in sql_query

            # Verify performance fields
            assert "duration_ms" in sql_query
            assert "status" in sql_query
            assert "error_message" in sql_query

            # Verify user tracking fields
            assert "user_email" in sql_query
            assert "installation_id" in sql_query
            assert "cli_version" in sql_query

    @pytest.mark.asyncio
    async def test_analytics_view_uses_safe_json_parsing(self, mock_bigquery_client):
        """Test that view uses SAFE.PARSE_JSON to avoid errors"""
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

            sql_query = mock_bigquery_client.query.call_args[0][0]

            # Verify SAFE.PARSE_JSON is used (not just PARSE_JSON)
            assert "SAFE.PARSE_JSON(resource_json)" in sql_query
            assert "SAFE.PARSE_JSON(labels_json)" in sql_query
            assert "SAFE.PARSE_JSON(operation_json)" in sql_query
            assert "SAFE.PARSE_JSON(httpRequest_json)" in sql_query
            assert "SAFE.PARSE_JSON(jsonPayload_json)" in sql_query

    @pytest.mark.asyncio
    async def test_verify_view_exists_after_creation(self, mock_bigquery_client):
        """Test that view can be verified after creation"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Create view
            mock_query_job = Mock()
            mock_query_job.result = Mock()
            mock_bigquery_client.query.return_value = mock_query_job

            await bigquery_service.create_analytics_view(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Verify view exists
            mock_bigquery_client.get_table.return_value = Mock()

            result = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is True


class TestAnalyticsViewErrorHandling:
    """Integration tests for analytics view error handling"""

    @pytest.mark.asyncio
    async def test_view_creation_failure_raises_exception(self, mock_bigquery_client):
        """Test that view creation failure raises exception"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.query.side_effect = Exception("SQL syntax error")

            with pytest.raises(Exception) as exc_info:
                await bigquery_service.create_analytics_view(
                    project_id=project_id,
                    dataset_name=dataset_name
                )

            assert "SQL syntax error" in str(exc_info.value) or "Failed to create" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_verify_view_handles_not_found(self, mock_bigquery_client):
        """Test that verify_view_exists handles NotFound gracefully"""
        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.side_effect = NotFound("View not found")

            result = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            assert result is False
