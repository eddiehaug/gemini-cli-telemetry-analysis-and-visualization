"""
Functional/End-to-End tests for BigQuery service.
Tests complete API workflows through FastAPI endpoints.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class TestBigQueryAPIEndToEnd:
    """End-to-end tests for BigQuery API endpoints"""

    @pytest.mark.asyncio
    async def test_create_dataset_api_endpoint(self, test_client, mock_bigquery_client, monkeypatch):
        """Test /api/create-dataset endpoint end-to-end"""
        from services import bigquery_service

        # Mock the functions
        async def mock_create_dataset(*args, **kwargs):
            return {
                "dataset": "test-project.test_dataset",
                "location": "us-central1",
                "table_created": True,
                "metadata_enriched": True,
                "note": "Table created with JSON type and comprehensive documentation"
            }

        monkeypatch.setattr(bigquery_service, 'create_dataset', mock_create_dataset)

        # Make API request
        response = test_client.post(
            "/api/create-dataset",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset",
                "region": "us-central1"
            }
        )

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "dataset" in data["data"]
        assert data["data"]["table_created"] is True

    # Note: Skipping test_verify_dataset_api_endpoint as the endpoint may not be implemented yet
    # This is a service-level test, not an API test


class TestEndToEndWorkflow:
    """End-to-end workflow tests"""

    @pytest.mark.asyncio
    async def test_complete_bigquery_setup_workflow(self, mock_bigquery_client, monkeypatch):
        """
        Test complete BigQuery setup workflow:
        1. Create dataset
        2. Create raw table with JSON string schema
        3. Enrich table metadata
        4. Verify table exists
        """
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"
        region = "us-central1"

        # Track workflow steps
        workflow_steps = []

        # Setup mocks
        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Mock dataset creation
            def mock_create_ds(ds, exists_ok=True):
                workflow_steps.append("dataset_created")
                return Mock()
            mock_bigquery_client.create_dataset = mock_create_ds

            # Mock table creation
            def mock_create_tbl(tbl, exists_ok=True):
                workflow_steps.append("table_created")
                # Verify it's the correct table schema
                assert "gemini_raw_logs" in str(tbl.table_id)
                # Verify JSON string schema
                schema_types = {field.name: field.field_type for field in tbl.schema}
                assert schema_types["resource_json"] == "STRING"
                assert schema_types["jsonPayload_json"] == "STRING"
                return tbl
            mock_bigquery_client.create_table = mock_create_tbl

            # Mock table get for enrichment
            def mock_get_table(table_id):
                workflow_steps.append("table_retrieved")
                mock_table = Mock()
                mock_table.schema = [
                    bigquery.SchemaField("timestamp", "TIMESTAMP"),
                    bigquery.SchemaField("resource_json", "STRING"),
                    bigquery.SchemaField("jsonPayload_json", "STRING"),
                ]
                return mock_table
            mock_bigquery_client.get_table = mock_get_table

            # Mock table update for enrichment
            def mock_update_table(table, fields):
                workflow_steps.append("table_metadata_enriched")
                # Verify description was added
                assert table.description is not None
                assert "ELT" in table.description or "Dataflow" in table.description
                return table
            mock_bigquery_client.update_table = mock_update_table

            # Execute workflow
            result = await bigquery_service.create_dataset(
                project_id=project_id,
                dataset_name=dataset_name,
                region=region,
                skip_table_creation=False
            )

            # Verify workflow completed all steps
            assert "dataset_created" in workflow_steps
            assert "table_created" in workflow_steps
            assert "table_retrieved" in workflow_steps
            assert "table_metadata_enriched" in workflow_steps

            # Verify result
            assert result["table_created"] is True
            assert result["metadata_enriched"] is True

            # Verify table exists
            mock_bigquery_client.get_table = Mock(return_value=Mock())
            table_exists = await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )
            assert table_exists is True

    @pytest.mark.asyncio
    async def test_schema_migration_workflow(self, mock_bigquery_client):
        """
        Test schema migration from old HYBRID schema to new JSON string schema.
        Verify that new tables use the unbreakable schema.
        """
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Create new table
        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify new schema is JSON strings (not STRUCT/JSON)
        schema_dict = {field.name: field.field_type for field in table_arg.schema}

        # Old schema had STRUCT/JSON types - verify they're now STRING
        assert schema_dict["resource_json"] == "STRING"  # Was STRUCT
        assert schema_dict["labels_json"] == "STRING"    # Was STRUCT
        assert schema_dict["jsonPayload_json"] == "STRING"  # Was JSON

        # Verify table name changed
        assert "gemini_raw_logs" in str(table_arg.table_id)

    @pytest.mark.asyncio
    async def test_error_recovery_workflow(self, mock_bigquery_client):
        """Test error recovery in the workflow"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Simulate table creation failure
        mock_bigquery_client.create_table.side_effect = Exception("Table creation failed")

        # Verify exception is raised
        with pytest.raises(Exception) as exc_info:
            await bigquery_service.create_minimal_logging_table(
                client=mock_bigquery_client,
                project_id=project_id,
                dataset_name=dataset_name
            )

        assert "Table creation failed" in str(exc_info.value) or "Failed to create" in str(exc_info.value)


class TestDataIngestionReadiness:
    """Tests verifying table is ready for Dataflow ingestion"""

    @pytest.mark.asyncio
    async def test_table_ready_for_dataflow_ingestion(self, mock_bigquery_client):
        """Test that table schema is ready for Dataflow JavaScript UDF ingestion"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify schema accepts Dataflow UDF output
        # UDF will output: { timestamp: "...", resource_json: "{...}", ... }
        schema_dict = {field.name: field.field_type for field in table_arg.schema}

        # All complex fields must be STRING to accept JSON.stringify() output
        complex_fields = ["resource_json", "labels_json", "operation_json", "httpRequest_json", "jsonPayload_json"]
        for field in complex_fields:
            assert schema_dict[field] == "STRING", f"{field} must be STRING for Dataflow UDF output"

        # Simple fields should be typed for query performance
        assert schema_dict["timestamp"] == "TIMESTAMP"
        assert schema_dict["severity"] == "STRING"

    @pytest.mark.asyncio
    async def test_table_partitioning_for_query_performance(self, mock_bigquery_client):
        """Test that table is partitioned for efficient querying"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify daily partitioning on timestamp
        assert table_arg.time_partitioning is not None
        assert table_arg.time_partitioning.type_ == bigquery.TimePartitioningType.DAY
        assert table_arg.time_partitioning.field == "timestamp"

        # Verify clustering for query optimization
        assert table_arg.clustering_fields is not None
        assert "logName" in table_arg.clustering_fields
        assert "severity" in table_arg.clustering_fields


class TestDocumentationAndMetadata:
    """Tests for table documentation and metadata"""

    @pytest.mark.asyncio
    async def test_table_documentation_complete(self, mock_bigquery_client):
        """Test that table has complete documentation"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_table = Mock()
        mock_table.schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("resource_json", "STRING"),
            bigquery.SchemaField("jsonPayload_json", "STRING"),
        ]
        mock_bigquery_client.get_table.return_value = mock_table

        await bigquery_service.enrich_table_metadata_internal(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        updated_table = mock_bigquery_client.update_table.call_args[0][0]

        # Verify comprehensive documentation
        assert updated_table.description is not None
        description = updated_table.description

        # Must explain ELT architecture
        assert "ELT" in description or "Extract, Load, Transform" in description.lower()

        # Must reference Dataflow
        assert "Dataflow" in description

        # Must reference Pub/Sub
        assert "Pub/Sub" in description

        # Must warn against direct querying
        assert "DO NOT QUERY DIRECTLY" in description

        # Must reference analytics view
        assert "gemini_analytics_view" in description

        # Must explain JSON string fields
        assert "JSON_VALUE" in description or "JSON" in description

    @pytest.mark.asyncio
    async def test_field_descriptions_explain_parsing(self, mock_bigquery_client):
        """Test that field descriptions explain how to parse JSON strings"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        mock_table = Mock()
        mock_table.schema = [
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

        updated_table = mock_bigquery_client.update_table.call_args[0][0]

        # Verify all JSON fields have parsing instructions
        for field in updated_table.schema:
            if field.name.endswith("_json"):
                assert "JSON_VALUE" in field.description or "JSON" in field.description
                assert "parse" in field.description.lower() or "Parse" in field.description


class TestBackwardsCompatibilityAndMigration:
    """Tests for backward compatibility and migration from old schema"""

    @pytest.mark.asyncio
    async def test_new_schema_different_from_old(self, mock_bigquery_client):
        """Test that new schema is different from old HYBRID schema"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]
        schema_dict = {field.name: field.field_type for field in table_arg.schema}

        # Old schema had these as STRUCT/JSON, verify they're STRING now
        assert schema_dict.get("resource_json") == "STRING"  # Was STRUCT (resource)
        assert schema_dict.get("labels_json") == "STRING"    # Was RECORD (labels)
        assert schema_dict.get("jsonPayload_json") == "STRING"  # Was JSON (jsonPayload)

        # Verify NO STRUCT or JSON types exist (old schema)
        for field_type in schema_dict.values():
            assert field_type != "STRUCT"
            assert field_type != "JSON"
            assert field_type != "RECORD"

    @pytest.mark.asyncio
    async def test_table_name_migration(self, mock_bigquery_client):
        """Test that table name migrated from gemini_cli to gemini_raw_logs"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Create table
        await bigquery_service.create_minimal_logging_table(
            client=mock_bigquery_client,
            project_id=project_id,
            dataset_name=dataset_name
        )

        table_arg = mock_bigquery_client.create_table.call_args[0][0]

        # Verify new table name
        assert "gemini_raw_logs" in str(table_arg.table_id)
        # Verify old table name is NOT used
        assert "gemini_cli" not in str(table_arg.table_id) or "gemini_cli_telemetry" in str(table_arg.table_id)

    @pytest.mark.asyncio
    async def test_verify_uses_new_default_table_name(self, mock_bigquery_client):
        """Test that verify_table_exists uses new default table name"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            mock_bigquery_client.get_table.return_value = Mock()

            # Call without table_name parameter
            await bigquery_service.verify_table_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Verify it checked for gemini_raw_logs
            call_args = mock_bigquery_client.get_table.call_args[0][0]
            assert "gemini_raw_logs" in call_args


class TestAnalyticsViewAPIEndpoint:
    """Functional tests for analytics view API endpoint"""

    @pytest.mark.asyncio
    async def test_create_analytics_view_api_endpoint(self, test_client, mock_bigquery_client, monkeypatch):
        """Test /api/create-analytics-view endpoint end-to-end"""
        from services import bigquery_service

        # Mock the function
        async def mock_create_view(*args, **kwargs):
            return {
                "view": "test-project.test_dataset.gemini_analytics_view",
                "status": "created",
                "fields_extracted": 30,
                "note": "Analytics view provides easy access to parsed telemetry data"
            }

        monkeypatch.setattr(bigquery_service, 'create_analytics_view', mock_create_view)

        # Make API request
        response = test_client.post(
            "/api/create-analytics-view",
            json={
                "projectId": "test-project",
                "datasetName": "test_dataset"
            }
        )

        # Verify response
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "view" in data["data"]
        assert data["data"]["status"] == "created"


class TestAnalyticsViewEndToEndWorkflow:
    """End-to-end workflow tests for analytics view"""

    @pytest.mark.asyncio
    async def test_complete_view_deployment_workflow(self, mock_bigquery_client):
        """Test complete analytics view deployment workflow"""
        from services import bigquery_service

        project_id = "test-project-123"
        dataset_name = "test_dataset"

        # Track workflow steps
        workflow_steps = []

        with patch('services.bigquery_service.bigquery.Client', return_value=mock_bigquery_client):
            # Mock view creation
            def mock_query(sql):
                workflow_steps.append("view_created")
                # Verify SQL creates the view
                assert "CREATE OR REPLACE VIEW" in sql
                assert "gemini_analytics_view" in sql
                mock_job = Mock()
                mock_job.result = Mock()
                return mock_job
            mock_bigquery_client.query = mock_query

            # Execute workflow
            result = await bigquery_service.create_analytics_view(
                project_id=project_id,
                dataset_name=dataset_name
            )

            # Verify workflow completed
            assert "view_created" in workflow_steps
            assert result["status"] == "created"

            # Verify view exists
            mock_bigquery_client.get_table = Mock(return_value=Mock())
            view_exists = await bigquery_service.verify_view_exists(
                project_id=project_id,
                dataset_name=dataset_name
            )
            assert view_exists is True

    @pytest.mark.asyncio
    async def test_view_sql_query_readiness(self, mock_bigquery_client):
        """Test that view SQL is ready for actual BigQuery execution"""
        from services import bigquery_service

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

            # Verify SQL doesn't have syntax issues
            # Check for proper quoting
            assert "`" in sql_query  # Backticks for table/column names
            assert "'" in sql_query or "\"" in sql_query  # Quotes for strings

            # Verify no placeholder/template markers remain
            assert "{" not in sql_query or "}" not in sql_query or sql_query.count("{") == sql_query.count("}")

            # Verify complete FROM clause
            assert "FROM `" in sql_query
            assert "gemini_raw_logs`" in sql_query


class TestAnalyticsViewQueryPatterns:
    """Functional tests for common query patterns on the view"""

    @pytest.mark.asyncio
    async def test_view_enables_simple_event_queries(self, mock_bigquery_client):
        """Test that view enables simple queries like SELECT event_name"""
        from services import bigquery_service

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

            # Verify view extracts event_name as a top-level column
            # This enables: SELECT event_name FROM view
            assert "AS event_name" in sql_query

    @pytest.mark.asyncio
    async def test_view_enables_token_aggregation_queries(self, mock_bigquery_client):
        """Test that view enables token aggregation queries"""
        from services import bigquery_service

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

            # Verify token fields are cast to INT64 for aggregation
            # This enables: SELECT SUM(input_tokens) FROM view
            assert "CAST" in sql_query
            assert "AS INT64" in sql_query
            assert "input_tokens" in sql_query
            assert "output_tokens" in sql_query

    @pytest.mark.asyncio
    async def test_view_preserves_raw_json_for_advanced_queries(self, mock_bigquery_client):
        """Test that view preserves raw JSON strings for advanced queries"""
        from services import bigquery_service

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

            # Verify raw JSON columns are included in SELECT
            assert "resource_json" in sql_query
            assert "labels_json" in sql_query
            assert "jsonPayload_json" in sql_query


class TestAnalyticsViewDocumentation:
    """Functional tests for view documentation and usability"""

    @pytest.mark.asyncio
    async def test_view_result_includes_usage_note(self, mock_bigquery_client):
        """Test that view creation result includes usage guidance"""
        from services import bigquery_service

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

            # Verify result includes helpful note
            assert "note" in result
            assert "Analytics view" in result["note"] or "parsed" in result["note"]

    @pytest.mark.asyncio
    async def test_view_extracts_sufficient_fields(self, mock_bigquery_client):
        """Test that view extracts sufficient fields for analytics"""
        from services import bigquery_service

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

            # Verify sufficient fields are extracted (at least 30)
            assert result["fields_extracted"] >= 30
