"""
BigQuery Analytics Views Service
Creates materialized views, regular views, and scheduled queries for Gemini CLI telemetry analysis.

This service creates all 15 analytics views specified in bq-queries.md:
- 10 Materialized Views (auto-refresh enabled)
- 2 Regular Views (no refresh)
- 3 Scheduled Query Tables (require manual scheduling)
"""

import logging
from typing import Dict, List
from google.cloud import bigquery
from utils.validators import (
    validate_gcp_project_id,
    validate_dataset_name,
    ValidationError
)

logger = logging.getLogger(__name__)

# ============================================================================
# VIEW DEFINITIONS - Single source of truth for all analytics views
# ============================================================================

# List of all view creation functions (order matters for creation)
VIEW_CREATION_FUNCTIONS = [
    # Regular views (converted from materialized due to BigQuery limitations)
    "create_daily_metrics_view",
    "create_user_activity_view",
    "create_error_analysis_view",
    "create_tool_performance_view",
    "create_cli_performance_and_resilience_view",
    "create_model_routing_analysis_view",

    # Materialized views (working)
    "create_token_usage_view",
    "create_malformed_json_responses_view",
    "create_feature_adoption_view",
    "create_conversation_analysis_view",

    # Regular views (no refresh needed)
    "create_quota_tracking_view",
    "create_user_configuration_view",

    # Scheduled query tables
    "create_daily_rollup_table",
    "create_quota_alerts_table",
    "create_weekly_rollup_table",
]

# Expected view names (for verification) - extracted from function names
EXPECTED_VIEW_NAMES = [
    "daily_metrics",
    "vw_user_activity",
    "vw_error_analysis",
    "vw_tool_performance",
    "vw_cli_performance_and_resilience",
    "vw_model_routing_analysis",
    "vw_token_usage",
    "vw_malformed_json_responses",
    "vw_feature_adoption",
    "vw_conversation_analysis",
    "vw_quota_tracking",
    "vw_user_configuration",
]

# Expected table names (for verification)
EXPECTED_TABLE_NAMES = [
    "daily_rollup_table",
    "quota_alerts_table",
    "weekly_rollup_table",
]

# ============================================================================
# MATERIALIZED VIEWS (10 views with auto-refresh)
# Note: First 7 use user_email, last 3 do NOT use user_email
# ============================================================================


async def create_daily_metrics_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create daily_metrics regular view (converted from materialized view due to BigQuery limitations).
    Aggregates key metrics on a daily basis for each model.
    Note: Queries on-demand for real-time data.
    """
    view_id = f"{project_id}.{dataset_name}.daily_metrics"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      DATE(timestamp) as day,
      model,
      COUNT(*) AS request_count,
      COUNT(DISTINCT {user_column}) AS distinct_users,
      SUM(input_tokens) AS total_input_tokens,
      SUM(output_tokens) AS total_output_tokens,
      SUM(total_tokens) AS total_tokens,
      AVG(duration_ms) AS average_duration_ms,
      COUNTIF(error_code IS NOT NULL) AS error_count
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    GROUP BY
      day,
      model
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_user_activity_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_user_activity regular view (converted from materialized view due to BigQuery limitations).
    Aggregates usage metrics by user.
    Note: Queries on-demand for real-time data.
    """
    view_id = f"{project_id}.{dataset_name}.vw_user_activity"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      {user_column},
      MIN(timestamp) AS first_seen,
      MAX(timestamp) AS last_seen,
      COUNT(*) AS total_requests,
      COUNT(DISTINCT session_id) AS total_sessions,
      AVG(duration_ms) AS average_request_duration_ms,
      SUM(total_tokens) AS total_tokens_used
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      {user_column} IS NOT NULL
    GROUP BY
      {user_column}
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_token_usage_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_token_usage materialized view.
    Detailed token consumption analysis.
    Refresh interval: 60 minutes.
    """
    view_id = f"{project_id}.{dataset_name}.vw_token_usage"

    query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{view_id}`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    ) AS
    SELECT
      DATE(timestamp) AS day,
      {user_column},
      model,
      SUM(input_tokens) AS total_input_tokens,
      SUM(output_tokens) AS total_output_tokens,
      SUM(cached_tokens) AS total_cached_tokens,
      SUM(total_tokens) AS grand_total_tokens
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      total_tokens IS NOT NULL
    GROUP BY
      day,
      {user_column},
      model
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created materialized view: {view_id}")
        return {"view": view_id, "type": "materialized", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_error_analysis_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_error_analysis regular view (converted from materialized view due to BigQuery limitations).
    Error tracking by day, CLI version, model, and user.
    Note: Queries on-demand for real-time data.
    """
    view_id = f"{project_id}.{dataset_name}.vw_error_analysis"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      DATE(timestamp) AS day,
      cli_version,
      model,
      error_code,
      error_message,
      COUNT(*) AS error_count,
      COUNT(DISTINCT {user_column}) AS distinct_users_affected
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      error_code IS NOT NULL
    GROUP BY
      day,
      cli_version,
      model,
      error_code,
      error_message
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_malformed_json_responses_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_malformed_json_responses materialized view.
    Tracks malformed JSON responses from generateJson command.
    Refresh interval: 60 minutes.
    """
    view_id = f"{project_id}.{dataset_name}.vw_malformed_json_responses"

    query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{view_id}`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    ) AS
    SELECT
      DATE(timestamp) AS day,
      model,
      {user_column},
      COUNT(*) AS malformed_json_count
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      event_name = 'gemini_cli.malformed_json_response'
    GROUP BY
      day,
      model,
      {user_column}
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created materialized view: {view_id}")
        return {"view": view_id, "type": "materialized", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_feature_adoption_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_feature_adoption materialized view.
    Tracks adoption of slash commands, agent mode, extensions.
    Refresh interval: 60 minutes.
    """
    view_id = f"{project_id}.{dataset_name}.vw_feature_adoption"

    query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{view_id}`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    ) AS
    SELECT
      DATE(timestamp) AS day,
      {user_column},
      cli_version,
      COUNTIF(event_name = 'gemini_cli.slash_command') AS slash_command_usage_count,
      COUNTIF(event_name = 'gemini_cli.agent.start') AS agent_mode_usage_count,
      COUNTIF(event_name = 'gemini_cli.extension_install') AS extension_install_count
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      event_name IN ('gemini_cli.slash_command', 'gemini_cli.agent.start', 'gemini_cli.extension_install')
    GROUP BY
      day,
      {user_column},
      cli_version
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created materialized view: {view_id}")
        return {"view": view_id, "type": "materialized", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_conversation_analysis_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_conversation_analysis materialized view.
    Analyzes chat sessions and turn counts.
    Refresh interval: 60 minutes.
    """
    view_id = f"{project_id}.{dataset_name}.vw_conversation_analysis"

    query = f"""
    CREATE OR REPLACE MATERIALIZED VIEW `{view_id}`
    OPTIONS(
      enable_refresh = true,
      refresh_interval_minutes = 60
    ) AS
    SELECT
      DATE(timestamp) AS day,
      {user_column},
      cli_version,
      JSON_VALUE(payload.attributes.approvalMode) AS approval_mode,
      COUNT(*) AS conversation_count,
      AVG(CAST(JSON_VALUE(payload.attributes.turnCount) AS INT64)) AS avg_turn_count,
      MIN(CAST(JSON_VALUE(payload.attributes.turnCount) AS INT64)) AS min_turn_count,
      MAX(CAST(JSON_VALUE(payload.attributes.turnCount) AS INT64)) AS max_turn_count
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      event_name = 'gemini_cli.conversation_finished'
    GROUP BY
      day,
      {user_column},
      cli_version,
      approval_mode
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created materialized view: {view_id}")
        return {"view": view_id, "type": "materialized", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_tool_performance_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_tool_performance regular view (converted from materialized view due to BigQuery limitations).
    Tool call performance and success rates.
    Note: Queries on-demand for real-time data.

    NOTE: This view does NOT use user_email, so user_column parameter is ignored.
    """
    view_id = f"{project_id}.{dataset_name}.vw_tool_performance"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      DATE(timestamp) AS day,
      model,
      function_name,
      COUNT(*) AS total_calls,
      COUNTIF(error_code IS NULL) AS successful_calls,
      COUNTIF(error_code IS NOT NULL) AS failed_calls,
      SAFE_DIVIDE(COUNTIF(error_code IS NULL), COUNT(*)) AS success_rate,
      AVG(duration_ms) AS average_execution_duration_ms
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      function_name IS NOT NULL
    GROUP BY
      day,
      model,
      function_name
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_cli_performance_and_resilience_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_cli_performance_and_resilience regular view (converted from materialized view due to BigQuery limitations).
    Monitors CLI operational health, fallbacks, retries, compression.
    Note: Queries on-demand for real-time data.

    NOTE: This view does NOT use user_email, so user_column parameter is ignored.
    """
    view_id = f"{project_id}.{dataset_name}.vw_cli_performance_and_resilience"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      DATE(timestamp) AS day,
      model,
      COUNTIF(event_name = 'gemini_cli.flash_fallback') AS flash_fallback_count,
      COUNTIF(event_name = 'gemini_cli.chat.content_retry') AS content_retry_count,
      COUNTIF(event_name = 'gemini_cli.chat.content_retry_failure') AS content_retry_failure_count,
      AVG(CASE WHEN event_name = 'gemini_cli.chat_compression' THEN CAST(JSON_VALUE(payload.attributes.tokens_before) AS INT64) END) AS avg_tokens_before_compression,
      AVG(CASE WHEN event_name = 'gemini_cli.chat_compression' THEN CAST(JSON_VALUE(payload.attributes.tokens_after) AS INT64) END) AS avg_tokens_after_compression,
      SAFE_DIVIDE(
        SUM(CASE WHEN event_name = 'gemini_cli.chat_compression' THEN CAST(JSON_VALUE(payload.attributes.tokens_before) AS INT64) - CAST(JSON_VALUE(payload.attributes.tokens_after) AS INT64) END),
        SUM(CASE WHEN event_name = 'gemini_cli.chat_compression' THEN CAST(JSON_VALUE(payload.attributes.tokens_before) AS INT64) END)
      ) AS avg_compression_ratio
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      event_name IN ('gemini_cli.flash_fallback', 'gemini_cli.chat.content_retry', 'gemini_cli.chat.content_retry_failure', 'gemini_cli.chat_compression')
    GROUP BY
      day,
      model
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_model_routing_analysis_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_model_routing_analysis regular view (converted from materialized view due to BigQuery limitations).
    Analyzes model router behavior and performance.
    Note: Queries on-demand for real-time data.

    NOTE: This view does NOT use user_email, so user_column parameter is ignored.
    """
    view_id = f"{project_id}.{dataset_name}.vw_model_routing_analysis"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}`
    AS
    SELECT
      DATE(timestamp) AS day,
      JSON_VALUE(payload.attributes.decision_source) AS decision_source,
      JSON_VALUE(payload.attributes.decision_model) AS decision_model,
      COUNT(*) AS decision_count,
      COUNTIF(CAST(JSON_VALUE(payload.attributes.failed) AS BOOL)) AS failure_count,
      SAFE_DIVIDE(COUNTIF(CAST(JSON_VALUE(payload.attributes.failed) AS BOOL)), COUNT(*)) AS failure_rate,
      AVG(CAST(JSON_VALUE(payload.attributes.routing_latency_ms) AS INT64)) AS avg_routing_latency_ms
    FROM
      `{project_id}.{dataset_name}.gemini_analytics_view`
    WHERE
      event_name = 'gemini_cli.model_routing'
    GROUP BY
      day,
      decision_source,
      decision_model
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created regular view: {view_id}")
        return {"view": view_id, "type": "regular", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


# ============================================================================
# REGULAR VIEWS (2 views, no refresh)
# ============================================================================


async def create_quota_tracking_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_quota_tracking regular view.
    Minute-level aggregation for RPM/RPD quota monitoring.
    """
    view_id = f"{project_id}.{dataset_name}.vw_quota_tracking"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH MinuteAgg AS (
      SELECT
        DATE(timestamp) AS day,
        TIMESTAMP_TRUNC(timestamp, MINUTE) AS minute,
        {user_column},
        model,
        COUNTIF(event_name = 'gemini_cli.api_response') AS requests_per_minute,
        SUM(input_tokens) AS input_tokens_per_minute,
        SUM(output_tokens) AS output_tokens_per_minute,
        SUM(total_tokens) AS total_tokens_per_minute
      FROM
        `{project_id}.{dataset_name}.gemini_analytics_view`
      WHERE
        {user_column} IS NOT NULL AND event_name = 'gemini_cli.api_response'
      GROUP BY
        day,
        minute,
        {user_column},
        model
    )
    SELECT
      ma.day,
      ma.minute,
      ma.{user_column},
      ma.model,
      ma.requests_per_minute,
      ma.input_tokens_per_minute,
      ma.output_tokens_per_minute,
      ma.total_tokens_per_minute,
      SUM(ma.requests_per_minute) OVER (PARTITION BY ma.day, ma.{user_column}, ma.model) AS requests_per_day
    FROM
      MinuteAgg AS ma
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created view: {view_id}")
        return {"view": view_id, "type": "view", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


async def create_user_configuration_view(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create vw_user_configuration regular view.
    Captures latest known configuration for each user.
    """
    view_id = f"{project_id}.{dataset_name}.vw_user_configuration"

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH RankedConfigs AS (
      SELECT
        {user_column},
        cli_version,
        JSON_VALUE(payload.attributes.sandbox_enabled) AS sandbox_enabled,
        JSON_VALUE(payload.attributes.log_user_prompts_enabled) AS log_prompts_enabled,
        JSON_VALUE(payload.attributes.output_format) AS output_format,
        JSON_VALUE(payload.attributes.extensions) AS extensions,
        CAST(JSON_VALUE(payload.attributes.extension_count) AS INT64) AS extension_count,
        timestamp,
        ROW_NUMBER() OVER(PARTITION BY {user_column} ORDER BY timestamp DESC) as rn
      FROM
        `{project_id}.{dataset_name}.gemini_analytics_view`
      WHERE
        event_name = 'gemini_cli.config'
    )
    SELECT
      {user_column},
      cli_version,
      sandbox_enabled,
      log_prompts_enabled,
      output_format,
      extensions,
      extension_count,
      timestamp AS last_config_timestamp
    FROM
      RankedConfigs
    WHERE
      rn = 1
    """

    try:
        query_job = client.query(query)
        query_job.result()
        logger.info(f"✓ Created view: {view_id}")
        return {"view": view_id, "type": "view", "status": "created"}
    except Exception as e:
        logger.error(f"✗ Failed to create {view_id}: {str(e)}")
        raise


# ============================================================================
# SCHEDULED QUERIES (3 tables with BigQuery Data Transfer)
# ============================================================================


async def create_daily_rollup_table(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create daily_rollup_table with scheduled query.
    Processes previous day's data and aggregates key metrics.

    Note: Creates the table structure. Scheduled query setup requires
    BigQuery Data Transfer API.
    """
    table_id = f"{project_id}.{dataset_name}.daily_rollup_table"

    # First, create the table structure
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}`
    (
      day DATE,
      model STRING,
      request_count INT64,
      distinct_users INT64,
      total_input_tokens INT64,
      total_output_tokens INT64,
      total_tokens INT64,
      average_duration_ms FLOAT64,
      error_count INT64
    )
    PARTITION BY day
    CLUSTER BY model
    """

    try:
        query_job = client.query(create_table_query)
        query_job.result()
        logger.info(f"✓ Created table: {table_id}")

        # Note: Actual scheduled query creation requires BigQuery Data Transfer API
        # For now, we just create the table and provide the query
        scheduled_query = f"""
        INSERT INTO `{table_id}` (day, model, request_count, distinct_users, total_input_tokens, total_output_tokens, total_tokens, average_duration_ms, error_count)
        SELECT
          DATE(timestamp) as day,
          model,
          COUNT(*) AS request_count,
          COUNT(DISTINCT {user_column}) AS distinct_users,
          SUM(input_tokens) AS total_input_tokens,
          SUM(output_tokens) AS total_output_tokens,
          SUM(total_tokens) AS total_tokens,
          AVG(duration_ms) AS average_duration_ms,
          COUNTIF(error_code IS NOT NULL) AS error_count
        FROM
          `{project_id}.{dataset_name}.gemini_analytics_view`
        WHERE
          DATE(timestamp) = CURRENT_DATE() - INTERVAL 1 DAY
        GROUP BY
          day,
          model
        """

        return {
            "table": table_id,
            "type": "scheduled_query_table",
            "status": "created",
            "note": "Table created. Schedule query to run daily via BigQuery console.",
            "scheduled_query": scheduled_query
        }
    except Exception as e:
        logger.error(f"✗ Failed to create {table_id}: {str(e)}")
        raise


async def create_quota_alerts_table(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create quota_alerts_table with scheduled query.
    Identifies users exceeding RPM/RPD thresholds.

    Note: Creates the table structure. Scheduled query setup requires
    BigQuery Data Transfer API.
    """
    table_id = f"{project_id}.{dataset_name}.quota_alerts_table"

    # Create table structure
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}`
    (
      alert_date DATE,
      user_identifier STRING,
      model STRING,
      alert_type STRING,
      current_value INT64,
      quota_limit INT64,
      violation_time TIMESTAMP
    )
    PARTITION BY alert_date
    CLUSTER BY user_identifier, alert_type
    """

    try:
        query_job = client.query(create_table_query)
        query_job.result()
        logger.info(f"✓ Created table: {table_id}")

        # Provide scheduled query for user to set up
        scheduled_query = f"""
        INSERT INTO `{table_id}` (alert_date, user_identifier, model, alert_type, current_value, quota_limit, violation_time)
        SELECT
          CURRENT_DATE() AS alert_date,
          {user_column} AS user_identifier,
          model,
          'RPM_EXCEEDED' AS alert_type,
          requests_per_minute AS current_value,
          120 AS quota_limit,
          minute AS violation_time
        FROM
          `{project_id}.{dataset_name}.vw_quota_tracking`
        WHERE
          requests_per_minute > 120
          AND minute >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
        UNION ALL
        SELECT
          CURRENT_DATE() AS alert_date,
          {user_column} AS user_identifier,
          model,
          'RPD_EXCEEDED' AS alert_type,
          requests_per_day AS current_value,
          2000 AS quota_limit,
          NULL AS violation_time
        FROM
          `{project_id}.{dataset_name}.vw_quota_tracking`
        WHERE
          requests_per_day > 2000
          AND day = CURRENT_DATE() - INTERVAL 1 DAY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {user_column}, model, alert_type, day ORDER BY minute DESC) = 1
        """

        return {
            "table": table_id,
            "type": "scheduled_query_table",
            "status": "created",
            "note": "Table created. Schedule query to run every 10 minutes via BigQuery console.",
            "scheduled_query": scheduled_query
        }
    except Exception as e:
        logger.error(f"✗ Failed to create {table_id}: {str(e)}")
        raise


async def create_weekly_rollup_table(
    client: bigquery.Client,
    project_id: str,
    dataset_name: str,
    user_column: str = "user_email"
) -> Dict:
    """
    Create weekly_rollup_table with scheduled query.
    Processes previous week's data and aggregates key metrics.

    Note: Creates the table structure. Scheduled query setup requires
    BigQuery Data Transfer API.
    """
    table_id = f"{project_id}.{dataset_name}.weekly_rollup_table"

    # Create table structure
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}`
    (
      week_start_date DATE,
      model STRING,
      request_count INT64,
      distinct_users INT64,
      total_input_tokens INT64,
      total_output_tokens INT64,
      total_tokens INT64,
      average_duration_ms FLOAT64,
      error_count INT64
    )
    PARTITION BY week_start_date
    CLUSTER BY model
    """

    try:
        query_job = client.query(create_table_query)
        query_job.result()
        logger.info(f"✓ Created table: {table_id}")

        # Provide scheduled query for user to set up
        scheduled_query = f"""
        INSERT INTO `{table_id}` (week_start_date, model, request_count, distinct_users, total_input_tokens, total_output_tokens, total_tokens, average_duration_ms, error_count)
        SELECT
          DATE_TRUNC(timestamp, WEEK) as week_start_date,
          model,
          COUNT(*) AS request_count,
          COUNT(DISTINCT {user_column}) AS distinct_users,
          SUM(input_tokens) AS total_input_tokens,
          SUM(output_tokens) AS total_output_tokens,
          SUM(total_tokens) AS total_tokens,
          AVG(duration_ms) AS average_duration_ms,
          COUNTIF(error_code IS NOT NULL) AS error_count
        FROM
          `{project_id}.{dataset_name}.gemini_analytics_view`
        WHERE
          DATE_TRUNC(timestamp, WEEK) = DATE_TRUNC(CURRENT_DATE() - INTERVAL 7 DAY, WEEK)
        GROUP BY
          week_start_date,
          model
        """

        return {
            "table": table_id,
            "type": "scheduled_query_table",
            "status": "created",
            "note": "Table created. Schedule query to run weekly via BigQuery console.",
            "scheduled_query": scheduled_query
        }
    except Exception as e:
        logger.error(f"✗ Failed to create {table_id}: {str(e)}")
        raise


# ============================================================================
# ORCHESTRATOR FUNCTION
# ============================================================================


async def create_all_analytics_views(
    project_id: str,
    dataset_name: str,
    use_pseudonyms: bool = False
) -> Dict:
    """
    Create all analytics views, materialized views, and scheduled query tables.

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        use_pseudonyms: If True, use 'user_pseudonym' column; else 'user_email'

    Returns:
        Dict with creation results for all views
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    client = bigquery.Client(project=project_id)
    user_column = "user_pseudonym" if use_pseudonyms else "user_email"

    results = {
        "created": [],
        "failed": [],
        "user_column": user_column,
        "pseudoanonymized": use_pseudonyms
    }

    # List of all view creation functions - matches VIEW_CREATION_FUNCTIONS constant
    view_functions = [
        # Regular views (converted from materialized due to BigQuery limitations)
        create_daily_metrics_view,
        create_user_activity_view,
        create_error_analysis_view,
        create_tool_performance_view,
        create_cli_performance_and_resilience_view,
        create_model_routing_analysis_view,

        # Materialized views (working)
        create_token_usage_view,
        create_malformed_json_responses_view,
        create_feature_adoption_view,
        create_conversation_analysis_view,

        # Regular views (no refresh needed)
        create_quota_tracking_view,
        create_user_configuration_view,

        # Scheduled query tables
        create_daily_rollup_table,
        create_quota_alerts_table,
        create_weekly_rollup_table,
    ]

    logger.info(f"Creating {len(view_functions)} analytics views with user column: {user_column}")

    # Create each view sequentially
    for view_func in view_functions:
        try:
            result = await view_func(client, project_id, dataset_name, user_column)
            results["created"].append(result)
        except Exception as e:
            results["failed"].append({
                "function": view_func.__name__,
                "error": str(e)
            })
            logger.error(f"Failed to create view via {view_func.__name__}: {str(e)}")

    # Summary
    created_count = len(results["created"])
    failed_count = len(results["failed"])

    logger.info(f"✓ View creation complete: {created_count} created, {failed_count} failed")

    return results


async def verify_all_analytics_views(
    project_id: str,
    dataset_name: str
) -> Dict:
    """
    Verify that all 15 analytics views were created successfully.

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name

    Returns:
        Dict with verification results
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    client = bigquery.Client(project=project_id)

    # Use the single source of truth for expected views and tables
    expected_views = EXPECTED_VIEW_NAMES
    expected_tables = EXPECTED_TABLE_NAMES

    results = {
        "verified_views": [],
        "missing_views": [],
        "verified_tables": [],
        "missing_tables": [],
        "verified_count": 0,
        "total_expected": 15
    }

    # Check views (must be VIEW or MATERIALIZED_VIEW type)
    for view_name in expected_views:
        view_id = f"{project_id}.{dataset_name}.{view_name}"
        try:
            table_obj = client.get_table(view_id)
            # Verify it's actually a view, not a table
            if table_obj.table_type in ["VIEW", "MATERIALIZED_VIEW"]:
                results["verified_views"].append(view_name)
                logger.info(f"✓ Verified view: {view_name} (type: {table_obj.table_type})")
            else:
                results["missing_views"].append(view_name)
                logger.warning(f"✗ Wrong type for {view_name}: expected VIEW/MATERIALIZED_VIEW, got {table_obj.table_type}")
        except Exception as e:
            results["missing_views"].append(view_name)
            logger.warning(f"✗ Missing view: {view_name}")

    # Check tables (must be TABLE type, not VIEW)
    for table_name in expected_tables:
        table_id = f"{project_id}.{dataset_name}.{table_name}"
        try:
            table_obj = client.get_table(table_id)
            # Verify it's actually a table, not a view
            if table_obj.table_type == "TABLE":
                results["verified_tables"].append(table_name)
                logger.info(f"✓ Verified table: {table_name} (type: {table_obj.table_type})")
            else:
                results["missing_tables"].append(table_name)
                logger.warning(f"✗ Wrong type for {table_name}: expected TABLE, got {table_obj.table_type}")
        except Exception as e:
            results["missing_tables"].append(table_name)
            logger.warning(f"✗ Missing table: {table_name}")

    results["verified_count"] = len(results["verified_views"]) + len(results["verified_tables"])

    if results["verified_count"] == 15:
        logger.info("✓ All 15 analytics views/tables verified successfully")
    else:
        logger.warning(f"⚠ Only {results['verified_count']}/15 views/tables verified")

    return results
