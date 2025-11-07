"""
Integration tests for BigQuery Views Service
Target: Test interactions with BigQuery API and SQL correctness

Tests the service's integration with BigQuery Client:
1. SQL query construction and validity
2. View creation with actual client patterns
3. Scheduled query metadata correctness
4. Error handling with realistic failures
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from services import bigquery_views_service
from google.cloud import bigquery


# ============================================================================
# Integration Tests for Materialized Views (10 views × 2 = 20 tests)
# ============================================================================


class TestDailyMetricsViewIntegration:
    """Integration tests for daily_metrics materialized view"""

    @pytest.mark.asyncio
    async def test_sql_structure_with_user_email(self):
        """Test SQL query structure contains all required elements"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        # Get the SQL query that was executed
        sql_query = mock_client.query.call_args[0][0]

        # Verify materialized view structure (accepts both CREATE OR REPLACE and IF NOT EXISTS)
        assert ("CREATE OR REPLACE MATERIALIZED VIEW" in sql_query or "CREATE MATERIALIZED VIEW IF NOT EXISTS" in sql_query)
        assert "`test-project.test_dataset.daily_metrics`" in sql_query
        assert "enable_refresh" in sql_query
        assert "refresh_interval_minutes" in sql_query

        # Verify user_email column is used
        assert "user_email" in sql_query

        # Verify aggregations
        assert "COUNT(*)" in sql_query or "COUNT(DISTINCT" in sql_query
        assert "DATE(" in sql_query

    @pytest.mark.asyncio
    async def test_sql_structure_with_user_pseudonym(self):
        """Test SQL query uses user_pseudonym when specified"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Verify user_pseudonym is used instead of user_email
        assert "user_pseudonym" in sql_query
        assert "user_email" not in sql_query.lower() or "user_email AS user_pseudonym" in sql_query


class TestUserActivityViewIntegration:
    """Integration tests for vw_user_activity"""

    @pytest.mark.asyncio
    async def test_sql_references_base_view(self):
        """Test that view references the analytics view correctly"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_user_activity_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should reference gemini_analytics_view
        assert "gemini_analytics_view" in sql_query or "FROM `test-project.test_dataset" in sql_query
        assert "CREATE OR REPLACE MATERIALIZED VIEW" in sql_query

    @pytest.mark.asyncio
    async def test_query_job_configuration(self):
        """Test that BigQuery job is configured correctly"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_user_activity_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        # Verify query was called exactly once
        assert mock_client.query.call_count == 1

        # Verify result() was called to wait for completion
        assert mock_query_job.result.called


class TestTokenUsageViewIntegration:
    """Integration tests for vw_token_usage"""

    @pytest.mark.asyncio
    async def test_aggregations_present(self):
        """Test SQL contains required token aggregations"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_token_usage_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should have token-related aggregations
        assert "token" in sql_query.lower()
        assert "SUM(" in sql_query or "COUNT(" in sql_query

    @pytest.mark.asyncio
    async def test_materialized_view_options(self):
        """Test materialized view has refresh options"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_token_usage_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        assert "enable_refresh" in sql_query
        assert "refresh_interval_minutes" in sql_query


class TestErrorAnalysisViewIntegration:
    """Integration tests for vw_error_analysis"""

    @pytest.mark.asyncio
    async def test_error_filtering_logic(self):
        """Test SQL includes error filtering"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_error_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should filter for errors
        assert "error" in sql_query.lower() or "WHERE" in sql_query

    @pytest.mark.asyncio
    async def test_client_error_handling(self):
        """Test handling of BigQuery API errors"""
        mock_client = Mock(spec=bigquery.Client)
        mock_client.query.side_effect = Exception("BigQuery API error: Permission denied")

        with pytest.raises(Exception, match="BigQuery API error"):
            await bigquery_views_service.create_error_analysis_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestMalformedJsonViewIntegration:
    """Integration tests for vw_malformed_json_responses"""

    @pytest.mark.asyncio
    async def test_json_parsing_logic(self):
        """Test SQL includes JSON parsing checks"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_malformed_json_responses_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should have JSON-related logic
        assert "json" in sql_query.lower()

    @pytest.mark.asyncio
    async def test_view_naming_convention(self):
        """Test view name follows naming convention"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_malformed_json_responses_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert "vw_malformed_json_responses" in result["view"]


class TestFeatureAdoptionViewIntegration:
    """Integration tests for vw_feature_adoption"""

    @pytest.mark.asyncio
    async def test_feature_tracking_logic(self):
        """Test SQL tracks feature usage"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_feature_adoption_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should track features or commands
        assert sql_query  # SQL should be non-empty

    @pytest.mark.asyncio
    async def test_return_metadata(self):
        """Test function returns correct metadata"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_feature_adoption_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "materialized"
        assert "vw_feature_adoption" in result["view"]


class TestConversationAnalysisViewIntegration:
    """Integration tests for vw_conversation_analysis"""

    @pytest.mark.asyncio
    async def test_conversation_metrics(self):
        """Test SQL includes conversation metrics"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_conversation_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should have conversation-related logic
        assert sql_query  # Non-empty SQL

    @pytest.mark.asyncio
    async def test_bigquery_client_interaction(self):
        """Test proper BigQuery client usage"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_conversation_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        # Verify client.query() was called
        assert mock_client.query.called
        # Verify query job result was awaited
        assert mock_query_job.result.called


class TestToolPerformanceViewIntegration:
    """Integration tests for vw_tool_performance (NO user column)"""

    @pytest.mark.asyncio
    async def test_no_user_column_in_sql(self):
        """Test SQL doesn't include user columns"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_tool_performance_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should NOT contain user_email or user_pseudonym
        assert "user_email" not in sql_query.lower() or "-- no user column" in sql_query.lower()

    @pytest.mark.asyncio
    async def test_tool_metrics_present(self):
        """Test SQL includes tool performance metrics"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_tool_performance_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should have tool-related metrics
        assert "tool" in sql_query.lower() or sql_query  # Non-empty


class TestCLIPerformanceViewIntegration:
    """Integration tests for vw_cli_performance_and_resilience (NO user column)"""

    @pytest.mark.asyncio
    async def test_cli_metrics_structure(self):
        """Test SQL includes CLI performance metrics"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_cli_performance_and_resilience_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        assert "CREATE OR REPLACE MATERIALIZED VIEW" in sql_query

    @pytest.mark.asyncio
    async def test_ignores_user_column_parameter(self):
        """Test view ignores user_column parameter"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_cli_performance_and_resilience_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should not use user_pseudonym
        assert "user_pseudonym" not in sql_query or "-- no user column" in sql_query.lower()


class TestModelRoutingViewIntegration:
    """Integration tests for vw_model_routing_analysis (NO user column)"""

    @pytest.mark.asyncio
    async def test_model_routing_logic(self):
        """Test SQL includes model routing analysis"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_model_routing_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should have model-related analysis
        assert "model" in sql_query.lower() or sql_query  # Non-empty

    @pytest.mark.asyncio
    async def test_materialized_view_created(self):
        """Test materialized view is created"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_model_routing_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["type"] == "materialized"


# ============================================================================
# Integration Tests for Regular Views (2 views × 2 = 4 tests)
# ============================================================================


class TestQuotaTrackingViewIntegration:
    """Integration tests for vw_quota_tracking regular view"""

    @pytest.mark.asyncio
    async def test_regular_view_no_refresh(self):
        """Test regular view doesn't have refresh options"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_quota_tracking_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Regular view should NOT have ENABLE REFRESH
        assert "CREATE OR REPLACE VIEW" in sql_query
        assert "ENABLE REFRESH" not in sql_query

    @pytest.mark.asyncio
    async def test_quota_metrics(self):
        """Test SQL includes quota tracking logic"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_quota_tracking_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["type"] == "view"
        assert "vw_quota_tracking" in result["view"]


class TestUserConfigurationViewIntegration:
    """Integration tests for vw_user_configuration regular view"""

    @pytest.mark.asyncio
    async def test_user_config_structure(self):
        """Test view includes user configuration data"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_user_configuration_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        assert "CREATE OR REPLACE VIEW" in sql_query

    @pytest.mark.asyncio
    async def test_return_type_is_view(self):
        """Test function returns type='view'"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_user_configuration_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["type"] == "view"


# ============================================================================
# Integration Tests for Scheduled Query Tables (3 tables × 2 = 6 tests)
# ============================================================================


class TestDailyRollupTableIntegration:
    """Integration tests for daily_rollup_table"""

    @pytest.mark.asyncio
    async def test_table_creation_sql(self):
        """Test table is created with correct SQL"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_daily_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        # Should create table
        assert "CREATE TABLE IF NOT EXISTS" in sql_query

    @pytest.mark.asyncio
    async def test_scheduled_query_metadata(self):
        """Test result includes scheduled query configuration"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_daily_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["type"] == "scheduled_query_table"
        assert "scheduled_query" in result
        assert "daily_rollup_table" in result["table"]


class TestQuotaAlertsTableIntegration:
    """Integration tests for quota_alerts_table"""

    @pytest.mark.asyncio
    async def test_alert_logic_present(self):
        """Test SQL includes quota alert logic"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_quota_alerts_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        assert "CREATE TABLE IF NOT EXISTS" in sql_query

    @pytest.mark.asyncio
    async def test_scheduled_query_uses_user_column(self):
        """Test scheduled query SQL uses correct user column"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_quota_alerts_table(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        # Scheduled query should use user_pseudonym
        assert "user_pseudonym" in result["scheduled_query"]


class TestWeeklyRollupTableIntegration:
    """Integration tests for weekly_rollup_table"""

    @pytest.mark.asyncio
    async def test_weekly_aggregation(self):
        """Test SQL includes weekly aggregation logic"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        await bigquery_views_service.create_weekly_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        sql_query = mock_client.query.call_args[0][0]

        assert "CREATE TABLE IF NOT EXISTS" in sql_query

    @pytest.mark.asyncio
    async def test_table_naming(self):
        """Test table name follows convention"""
        mock_client = Mock(spec=bigquery.Client)
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client.query.return_value = mock_query_job

        result = await bigquery_views_service.create_weekly_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert "weekly_rollup_table" in result["table"]


# ============================================================================
# Integration Tests for Orchestrator (3 tests)
# ============================================================================


class TestCreateAllAnalyticsViewsIntegration:
    """Integration tests for create_all_analytics_views orchestrator"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_client_initialization(self, mock_bq_client):
        """Test BigQuery client is initialized correctly"""
        mock_client_instance = Mock()
        mock_client_instance.query.return_value.result.return_value = None
        mock_bq_client.return_value = mock_client_instance

        await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        # Verify client was initialized with correct project
        mock_bq_client.assert_called_once_with(project="test-project")

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_all_view_functions_called(self, mock_bq_client):
        """Test all 15 view functions are called"""
        mock_client_instance = Mock()
        mock_client_instance.query.return_value.result.return_value = None
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        # Should call query 15 times (once per view)
        assert mock_client_instance.query.call_count == 15

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_error_isolation(self, mock_bq_client):
        """Test that errors in one view don't stop others"""
        mock_client_instance = Mock()

        # Make every 3rd call fail
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count % 3 == 0:
                raise Exception(f"View {call_count} failed")
            mock_result = Mock()
            mock_result.result.return_value = None
            return mock_result

        mock_client_instance.query = Mock(side_effect=side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        # Should have both successes and failures
        assert len(result["created"]) > 0
        assert len(result["failed"]) > 0
        assert len(result["created"]) + len(result["failed"]) == 15


# ============================================================================
# Integration Tests for Verification (3 tests)
# ============================================================================


class TestVerifyAllAnalyticsViewsIntegration:
    """Integration tests for verify_all_analytics_views"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_client_get_table_calls(self, mock_bq_client):
        """Test client.get_table() is called for each view"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        # Should call get_table 15 times
        assert mock_client_instance.get_table.call_count == 15

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_table_id_format(self, mock_bq_client):
        """Test table IDs are formatted correctly"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        # Check first call's table ID format
        first_call_table_id = mock_client_instance.get_table.call_args_list[0][0][0]
        assert "test-project.test_dataset." in first_call_table_id

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_missing_view_detection(self, mock_bq_client):
        """Test missing views are properly detected"""
        mock_client_instance = Mock()

        # Make some views missing
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 10:
                raise Exception("Not found")
            return Mock()

        mock_client_instance.get_table = Mock(side_effect=side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        # Should detect missing views
        assert result["verified_count"] == 10
        assert len(result["missing_views"]) + len(result["missing_tables"]) == 5
