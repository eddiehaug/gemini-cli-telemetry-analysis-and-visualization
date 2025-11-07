"""
Functional tests for BigQuery Views Service
Target: End-to-end functionality testing with realistic scenarios

Tests complete workflows:
1. Full view creation pipeline
2. User column switching (email vs pseudonym)
3. Error recovery scenarios
4. Verification workflows
"""
import pytest
from unittest.mock import Mock, patch, MagicMock, call
from services import bigquery_views_service
from google.cloud import bigquery


# ============================================================================
# Functional Tests for Complete Workflows
# ============================================================================


class TestCompleteViewCreationWorkflow:
    """Test complete view creation workflows"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_full_deployment_with_email(self, mock_bq_client):
        """Test complete deployment of all 15 views with user_email"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_bq_client.return_value = mock_client_instance

        # Create all views
        result = await bigquery_views_service.create_all_analytics_views(
            "production-project",
            "gemini_cli_telemetry",
            use_pseudonyms=False
        )

        # Verify deployment success
        assert result["user_column"] == "user_email"
        assert result["pseudoanonymized"] is False
        assert len(result["created"]) == 15
        assert len(result["failed"]) == 0

        # Verify all created views have expected structure
        for view in result["created"]:
            assert "status" in view
            assert view["status"] == "created"
            assert "type" in view
            assert view["type"] in ["materialized", "view", "scheduled_query_table"]

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_full_deployment_with_pseudonyms(self, mock_bq_client):
        """Test complete deployment with pseudoanonymization enabled"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "production-project",
            "gemini_cli_telemetry",
            use_pseudonyms=True
        )

        assert result["user_column"] == "user_pseudonym"
        assert result["pseudoanonymized"] is True
        assert len(result["created"]) == 15

        # Verify user_pseudonym is used in SQL queries (for views that support user columns)
        user_column_queries = [call for call in mock_client_instance.query.call_args_list
                               if "user_email" in call[0][0] or "user_pseudonym" in call[0][0]]

        # Check that queries with user columns use user_pseudonym
        for call_args in user_column_queries:
            sql = call_args[0][0]
            assert "user_pseudonym" in sql

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_deployment_then_verification(self, mock_bq_client):
        """Test deployment followed by verification workflow"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_client_instance.get_table.return_value = Mock()  # For verification
        mock_bq_client.return_value = mock_client_instance

        # Step 1: Deploy all views
        create_result = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        assert len(create_result["created"]) == 15

        # Step 2: Verify all views exist
        verify_result = await bigquery_views_service.verify_all_analytics_views(
            "test-project",
            "test_dataset"
        )

        assert verify_result["verified_count"] == 15
        assert verify_result["total_expected"] == 15
        assert len(verify_result["missing_views"]) == 0
        assert len(verify_result["missing_tables"]) == 0


class TestUserColumnSwitching:
    """Test switching between user_email and user_pseudonym"""

    @pytest.mark.asyncio
    async def test_daily_metrics_email_vs_pseudonym(self):
        """Test daily_metrics view with both user column types"""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        # Create with user_email
        result_email = await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        # Create with user_pseudonym
        result_pseudonym = await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        # Both should succeed
        assert result_email["status"] == "created"
        assert result_pseudonym["status"] == "created"

        # Verify SQL differences
        sql_email = mock_client.query.call_args_list[0][0][0]
        sql_pseudonym = mock_client.query.call_args_list[1][0][0]

        assert "user_email" in sql_email
        assert "user_pseudonym" in sql_pseudonym

    @pytest.mark.asyncio
    async def test_all_views_support_both_columns(self):
        """Test that all 12 user-based views support both column types"""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        # Views that support user columns
        user_view_functions = [
            bigquery_views_service.create_daily_metrics_view,
            bigquery_views_service.create_user_activity_view,
            bigquery_views_service.create_token_usage_view,
            bigquery_views_service.create_error_analysis_view,
            bigquery_views_service.create_malformed_json_responses_view,
            bigquery_views_service.create_feature_adoption_view,
            bigquery_views_service.create_conversation_analysis_view,
            bigquery_views_service.create_quota_tracking_view,
            bigquery_views_service.create_user_configuration_view,
            bigquery_views_service.create_daily_rollup_table,
            bigquery_views_service.create_quota_alerts_table,
            bigquery_views_service.create_weekly_rollup_table,
        ]

        for view_func in user_view_functions:
            # Test with user_email
            result_email = await view_func(
                mock_client, "test-project", "test_dataset", "user_email"
            )
            assert result_email["status"] == "created"

            # Test with user_pseudonym
            result_pseudonym = await view_func(
                mock_client, "test-project", "test_dataset", "user_pseudonym"
            )
            assert result_pseudonym["status"] == "created"


class TestErrorRecoveryScenarios:
    """Test error handling and recovery scenarios"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_partial_failure_recovery(self, mock_bq_client):
        """Test that deployment continues after partial failures"""
        mock_client_instance = Mock()

        # Simulate failures in views 3, 7, and 12
        failing_indices = {3, 7, 12}
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count in failing_indices:
                raise Exception(f"View {call_count} failed: Permission denied")
            mock_result = Mock()
            mock_result.result.return_value = None
            return mock_result

        mock_client_instance.query = Mock(side_effect=side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        # Should have 12 successes and 3 failures
        assert len(result["created"]) == 12
        assert len(result["failed"]) == 3

        # Verify failed views have error information
        for failure in result["failed"]:
            assert "function" in failure
            assert "error" in failure
            assert "Permission denied" in failure["error"]

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_network_timeout_handling(self, mock_bq_client):
        """Test handling of network timeouts"""
        mock_client_instance = Mock()
        mock_client_instance.query.side_effect = Exception("Connection timeout after 60s")
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        # All should fail with timeout
        assert len(result["failed"]) == 15
        assert len(result["created"]) == 0

        # All errors should mention timeout
        for failure in result["failed"]:
            assert "timeout" in failure["error"].lower()

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_quota_exceeded_handling(self, mock_bq_client):
        """Test handling of BigQuery quota exceeded errors"""
        mock_client_instance = Mock()
        mock_client_instance.query.side_effect = Exception("Quota exceeded: Too many queries")
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        assert len(result["failed"]) == 15
        for failure in result["failed"]:
            assert "Quota exceeded" in failure["error"]


class TestVerificationWorkflows:
    """Test verification workflows"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_freshly_created_views(self, mock_bq_client):
        """Test verification immediately after creation"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        # Create views
        await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        # Immediately verify
        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project",
            "test_dataset"
        )

        assert result["verified_count"] == 15
        assert len(result["verified_views"]) > 0
        assert len(result["verified_tables"]) > 0

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_missing_views(self, mock_bq_client):
        """Test verification detects missing views"""
        mock_client_instance = Mock()

        # Simulate some views missing
        existing_views = [
            "daily_metrics",
            "vw_user_activity",
            "vw_token_usage",
            "vw_error_analysis",
            "vw_malformed_json_responses",
        ]

        def get_table_side_effect(table_id):
            table_name = table_id.split(".")[-1]
            if any(view in table_name for view in existing_views):
                return Mock()
            raise Exception("Not found")

        mock_client_instance.get_table = Mock(side_effect=get_table_side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project",
            "test_dataset"
        )

        # Should find 5 views, miss 10
        assert result["verified_count"] == 5
        assert len(result["missing_views"]) + len(result["missing_tables"]) == 10

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_returns_complete_metadata(self, mock_bq_client):
        """Test verification returns all expected metadata"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project",
            "test_dataset"
        )

        # Check all expected keys
        assert "verified_views" in result
        assert "missing_views" in result
        assert "verified_tables" in result
        assert "missing_tables" in result
        assert "verified_count" in result
        assert "total_expected" in result

        # Verify types
        assert isinstance(result["verified_views"], list)
        assert isinstance(result["missing_views"], list)
        assert isinstance(result["verified_tables"], list)
        assert isinstance(result["missing_tables"], list)
        assert isinstance(result["verified_count"], int)
        assert result["total_expected"] == 15


class TestRealWorldScenarios:
    """Test real-world deployment scenarios"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_production_deployment_scenario(self, mock_bq_client):
        """Simulate production deployment with pseudonymization"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        # Production project settings
        project_id = "my-company-prod"
        dataset_name = "gemini_cli_telemetry"
        use_pseudonyms = True  # GDPR compliance

        # Step 1: Create all views with pseudonymization
        create_result = await bigquery_views_service.create_all_analytics_views(
            project_id,
            dataset_name,
            use_pseudonyms
        )

        assert create_result["pseudoanonymized"] is True
        assert create_result["user_column"] == "user_pseudonym"
        assert len(create_result["created"]) == 15

        # Step 2: Verify deployment
        verify_result = await bigquery_views_service.verify_all_analytics_views(
            project_id,
            dataset_name
        )

        assert verify_result["verified_count"] == 15

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_development_deployment_scenario(self, mock_bq_client):
        """Simulate development deployment without pseudonymization"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_bq_client.return_value = mock_client_instance

        # Dev project settings
        project_id = "my-company-dev"
        dataset_name = "gemini_cli_telemetry_dev"
        use_pseudonyms = False  # Easier debugging

        result = await bigquery_views_service.create_all_analytics_views(
            project_id,
            dataset_name,
            use_pseudonyms
        )

        assert result["pseudoanonymized"] is False
        assert result["user_column"] == "user_email"
        assert len(result["created"]) == 15

        # Verify user_email is in SQL
        for call_args in mock_client_instance.query.call_args_list:
            sql = call_args[0][0]
            # Check that views with user columns use user_email
            if "user_" in sql and "-- no user column" not in sql.lower():
                assert "user_email" in sql or "user_pseudonym" not in sql

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_redeployment_scenario(self, mock_bq_client):
        """Test re-running deployment (CREATE OR REPLACE)"""
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_bq_client.return_value = mock_client_instance

        # First deployment
        result1 = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=False
        )

        assert len(result1["created"]) == 15

        # Second deployment (should replace existing views)
        result2 = await bigquery_views_service.create_all_analytics_views(
            "test-project",
            "test_dataset",
            use_pseudonyms=True  # Switch to pseudonyms
        )

        assert len(result2["created"]) == 15
        assert result2["user_column"] == "user_pseudonym"

        # Verify CREATE OR REPLACE or CREATE IF NOT EXISTS was used
        for call_args in mock_client_instance.query.call_args_list:
            sql = call_args[0][0]
            assert ("CREATE OR REPLACE" in sql or
                    "CREATE TABLE IF NOT EXISTS" in sql or
                    "CREATE MATERIALIZED VIEW IF NOT EXISTS" in sql or
                    "CREATE VIEW IF NOT EXISTS" in sql)


class TestMaterializedViewRefreshSettings:
    """Test materialized view refresh configurations"""

    @pytest.mark.asyncio
    async def test_all_materialized_views_have_refresh(self):
        """Test all materialized views have ENABLE REFRESH"""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        materialized_functions = [
            bigquery_views_service.create_daily_metrics_view,
            bigquery_views_service.create_user_activity_view,
            bigquery_views_service.create_token_usage_view,
            bigquery_views_service.create_error_analysis_view,
            bigquery_views_service.create_malformed_json_responses_view,
            bigquery_views_service.create_feature_adoption_view,
            bigquery_views_service.create_conversation_analysis_view,
            bigquery_views_service.create_tool_performance_view,
            bigquery_views_service.create_cli_performance_and_resilience_view,
            bigquery_views_service.create_model_routing_analysis_view,
        ]

        for view_func in materialized_functions:
            await view_func(mock_client, "test-project", "test_dataset", "user_email")

        # Verify all have ENABLE REFRESH (accept both CREATE OR REPLACE and IF NOT EXISTS)
        for call_args in mock_client.query.call_args_list:
            sql = call_args[0][0]
            assert ("CREATE OR REPLACE MATERIALIZED VIEW" in sql or "CREATE MATERIALIZED VIEW IF NOT EXISTS" in sql)
            assert "enable_refresh" in sql
            assert "refresh_interval_minutes" in sql

    @pytest.mark.asyncio
    async def test_regular_views_no_refresh(self):
        """Test regular views don't have refresh settings"""
        mock_client = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        regular_view_functions = [
            bigquery_views_service.create_quota_tracking_view,
            bigquery_views_service.create_user_configuration_view,
        ]

        for view_func in regular_view_functions:
            await view_func(mock_client, "test-project", "test_dataset", "user_email")

        # Verify none have ENABLE REFRESH
        for call_args in mock_client.query.call_args_list:
            sql = call_args[0][0]
            assert "CREATE OR REPLACE VIEW" in sql
            assert "ENABLE REFRESH" not in sql
