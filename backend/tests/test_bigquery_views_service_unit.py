"""
Unit tests for BigQuery Views Service
Target: 80% code coverage

Tests each of the 15 view creation functions with:
1. Successful creation with user_email
2. Successful creation with user_pseudonym
3. Error handling when BigQuery query fails
"""
import pytest
from unittest.mock import Mock, patch, AsyncMock, MagicMock
from services import bigquery_views_service


# ============================================================================
# Test Materialized Views (10 tests × 3 = 30 tests)
# ============================================================================


class TestDailyMetricsView:
    """Test daily_metrics materialized view creation"""

    @pytest.mark.asyncio
    async def test_create_success_with_user_email(self):
        """Test successful creation with user_email column"""
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert "daily_metrics" in result["view"]
        assert result["type"] == "materialized"
        mock_client.query.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_success_with_user_pseudonym(self):
        """Test successful creation with user_pseudonym column"""
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_daily_metrics_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        # Verify SQL contains user_pseudonym
        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call
        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_failure_raises_exception(self):
        """Test that creation failures raise exceptions"""
        mock_client = Mock()
        mock_client.query.side_effect = Exception("BigQuery error")

        with pytest.raises(Exception, match="BigQuery error"):
            await bigquery_views_service.create_daily_metrics_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestUserActivityView:
    """Test vw_user_activity materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_user_activity_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert "vw_user_activity" in result["view"]

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_user_activity_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_user_activity_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestTokenUsageView:
    """Test vw_token_usage materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_token_usage_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert "vw_token_usage" in result["view"]

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_token_usage_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Query failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_token_usage_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestErrorAnalysisView:
    """Test vw_error_analysis materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_error_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_error_analysis_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_error_analysis_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestMalformedJsonView:
    """Test vw_malformed_json_responses materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_malformed_json_responses_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_malformed_json_responses_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_malformed_json_responses_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestFeatureAdoptionView:
    """Test vw_feature_adoption materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_feature_adoption_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_feature_adoption_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_feature_adoption_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestConversationAnalysisView:
    """Test vw_conversation_analysis materialized view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_conversation_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_conversation_analysis_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_conversation_analysis_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestToolPerformanceView:
    """Test vw_tool_performance materialized view (NO user column)"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_tool_performance_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_ignores_user_column_parameter(self):
        """This view doesn't use user_column, so parameter is ignored"""
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_tool_performance_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        # Should NOT contain user_pseudonym since this view doesn't track users
        assert "user_pseudonym" not in query_call
        assert "user_email" not in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_tool_performance_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestCLIPerformanceView:
    """Test vw_cli_performance_and_resilience view (NO user column)"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_cli_performance_and_resilience_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_ignores_user_column_parameter(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_cli_performance_and_resilience_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" not in query_call
        assert "user_email" not in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_cli_performance_and_resilience_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestModelRoutingView:
    """Test vw_model_routing_analysis view (NO user column)"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_model_routing_analysis_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_ignores_user_column_parameter(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_model_routing_analysis_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" not in query_call
        assert "user_email" not in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_model_routing_analysis_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


# ============================================================================
# Test Regular Views (2 views × 3 = 6 tests)
# ============================================================================


class TestQuotaTrackingView:
    """Test vw_quota_tracking regular view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_quota_tracking_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "view"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_quota_tracking_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_quota_tracking_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestUserConfigurationView:
    """Test vw_user_configuration regular view"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_user_configuration_view(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "view"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        await bigquery_views_service.create_user_configuration_view(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        query_call = mock_client.query.call_args[0][0]
        assert "user_pseudonym" in query_call

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_user_configuration_view(
                mock_client, "test-project", "test_dataset", "user_email"
            )


# ============================================================================
# Test Scheduled Query Tables (3 tables × 3 = 9 tests)
# ============================================================================


class TestDailyRollupTable:
    """Test daily_rollup_table creation"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_daily_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "scheduled_query_table"
        assert "scheduled_query" in result

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_daily_rollup_table(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        # Check the scheduled query uses user_pseudonym
        assert "user_pseudonym" in result["scheduled_query"]

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_daily_rollup_table(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestQuotaAlertsTable:
    """Test quota_alerts_table creation"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_quota_alerts_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "scheduled_query_table"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_quota_alerts_table(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        assert "user_pseudonym" in result["scheduled_query"]

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_quota_alerts_table(
                mock_client, "test-project", "test_dataset", "user_email"
            )


class TestWeeklyRollupTable:
    """Test weekly_rollup_table creation"""

    @pytest.mark.asyncio
    async def test_create_success(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_weekly_rollup_table(
            mock_client, "test-project", "test_dataset", "user_email"
        )

        assert result["status"] == "created"
        assert result["type"] == "scheduled_query_table"

    @pytest.mark.asyncio
    async def test_create_with_pseudonym(self):
        mock_client = Mock()
        mock_client.query.return_value.result.return_value = None

        result = await bigquery_views_service.create_weekly_rollup_table(
            mock_client, "test-project", "test_dataset", "user_pseudonym"
        )

        assert "user_pseudonym" in result["scheduled_query"]

    @pytest.mark.asyncio
    async def test_create_failure(self):
        mock_client = Mock()
        mock_client.query.side_effect = Exception("Failed")

        with pytest.raises(Exception):
            await bigquery_views_service.create_weekly_rollup_table(
                mock_client, "test-project", "test_dataset", "user_email"
            )


# ============================================================================
# Test Orchestrator Function (5 tests)
# ============================================================================


class TestCreateAllAnalyticsViews:
    """Test create_all_analytics_views orchestrator"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_create_all_views_user_email(self, mock_bq_client):
        """Test creating all 15 views with user_email"""
        mock_client_instance = Mock()
        mock_client_instance.query.return_value.result.return_value = None
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        assert result["user_column"] == "user_email"
        assert result["pseudoanonymized"] is False
        assert len(result["created"]) == 15
        assert len(result["failed"]) == 0

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_create_all_views_user_pseudonym(self, mock_bq_client):
        """Test creating all 15 views with user_pseudonym"""
        mock_client_instance = Mock()
        mock_client_instance.query.return_value.result.return_value = None
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=True
        )

        assert result["user_column"] == "user_pseudonym"
        assert result["pseudoanonymized"] is True
        assert len(result["created"]) == 15

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_partial_failure_continues(self, mock_bq_client):
        """Test that failures in some views don't stop others"""
        mock_client_instance = Mock()

        # Make first view fail, rest succeed
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("First view failed")
            mock_result = Mock()
            mock_result.result.return_value = None
            return mock_result

        mock_client_instance.query = Mock(side_effect=side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        assert len(result["failed"]) == 1
        assert len(result["created"]) == 14

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_all_failures_reported(self, mock_bq_client):
        """Test that all failures are collected"""
        mock_client_instance = Mock()
        mock_client_instance.query.side_effect = Exception("All failed")
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        assert len(result["failed"]) == 15
        assert len(result["created"]) == 0

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_created_views_have_correct_metadata(self, mock_bq_client):
        """Test that created view metadata is correct"""
        mock_client_instance = Mock()
        mock_client_instance.query.return_value.result.return_value = None
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.create_all_analytics_views(
            "test-project", "test_dataset", use_pseudonyms=False
        )

        # Check first created view has expected fields
        first_view = result["created"][0]
        assert "view" in first_view or "table" in first_view
        assert "type" in first_view
        assert "status" in first_view


# ============================================================================
# Test Verification Function (4 tests)
# ============================================================================


class TestVerifyAllAnalyticsViews:
    """Test verify_all_analytics_views function"""

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_all_views_exist(self, mock_bq_client):
        """Test verification when all views exist"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.return_value = Mock()  # Success
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        assert result["verified_count"] == 15
        assert len(result["missing_views"]) == 0
        assert len(result["missing_tables"]) == 0

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_some_views_missing(self, mock_bq_client):
        """Test verification when some views are missing"""
        mock_client_instance = Mock()

        # First 12 calls succeed, last 3 fail
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 12:
                raise Exception("Not found")
            return Mock()

        mock_client_instance.get_table = Mock(side_effect=side_effect)
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        assert result["verified_count"] == 12
        assert len(result["missing_views"]) + len(result["missing_tables"]) == 3

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_no_views_exist(self, mock_bq_client):
        """Test verification when no views exist"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.side_effect = Exception("Not found")
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        assert result["verified_count"] == 0
        assert len(result["missing_views"]) + len(result["missing_tables"]) == 15

    @pytest.mark.asyncio
    @patch('services.bigquery_views_service.bigquery.Client')
    async def test_verify_returns_expected_fields(self, mock_bq_client):
        """Test that verification returns all expected fields"""
        mock_client_instance = Mock()
        mock_client_instance.get_table.return_value = Mock()
        mock_bq_client.return_value = mock_client_instance

        result = await bigquery_views_service.verify_all_analytics_views(
            "test-project", "test_dataset"
        )

        assert "verified_views" in result
        assert "missing_views" in result
        assert "verified_tables" in result
        assert "missing_tables" in result
        assert "verified_count" in result
        assert "total_expected" in result
        assert result["total_expected"] == 15
