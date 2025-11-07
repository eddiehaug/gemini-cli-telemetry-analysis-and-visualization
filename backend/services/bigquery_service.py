"""
BigQuery service.
Handles BigQuery dataset and table creation.
"""
import asyncio
import logging
from typing import Dict
from google.cloud import bigquery
from google.cloud.exceptions import NotFound, Conflict
from utils.validators import (
    validate_gcp_project_id,
    validate_dataset_name,
    validate_region,
    validate_table_name,
    validate_view_name,
    ValidationError
)

logger = logging.getLogger(__name__)


async def create_dataset(project_id: str, dataset_name: str, region: str, skip_table_creation: bool = False) -> Dict:
    """
    Create BigQuery dataset only (optionally skip table creation).

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        region: GCP region
        skip_table_creation: If True, skip table creation and let sink auto-create it

    The log sink with --use-partitioned-tables will auto-create the table
    with the correct schema when the first logs arrive.
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
        region = validate_region(region)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        client = bigquery.Client(project=project_id)

        # Create dataset
        dataset_id = f"{project_id}.{dataset_name}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = region

        try:
            dataset = client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Dataset {dataset_id} created successfully")
        except Conflict:
            logger.info(f"Dataset {dataset_id} already exists")

        if skip_table_creation:
            logger.info("EXPERIMENT MODE: Skipping table creation - letting sink auto-create it")
            logger.info("This will show us the exact schema the sink expects")
            return {
                "dataset": dataset_id,
                "location": region,
                "table_created": False,
                "metadata_enriched": False,
                "note": "Dataset created. Table will be auto-created by sink (experiment mode)"
            }

        # Create table with JSON type for jsonPayload
        # This prevents schema inference issues with complex Gemini CLI logs
        await create_minimal_logging_table(client, project_id, dataset_name)
        logger.info("Table created with JSON type for jsonPayload (flexible schema)")

        # Enrich table metadata immediately after creation
        await enrich_table_metadata_internal(client, project_id, dataset_name)
        logger.info("Table metadata enriched with field descriptions and documentation")

        return {
            "dataset": dataset_id,
            "location": region,
            "table_created": True,
            "metadata_enriched": True,
            "note": "Table created with JSON type and comprehensive documentation"
        }

    except Exception as e:
        logger.error(f"Dataset creation failed: {str(e)}")
        raise


async def create_minimal_logging_table(client: bigquery.Client, project_id: str, dataset_name: str) -> None:
    """
    Create "unbreakable" BigQuery table for ELT pattern (Dataflow ingestion).

    Per instructions.md Step 3: This table uses STRING columns for ALL complex fields.
    This guarantees zero schema validation errors during ingestion.

    Why STRING instead of JSON/STRUCT?
    - Dataflow JavaScript UDF will stringify all complex objects
    - BigQuery never validates structure during ingestion
    - Eliminates all "schema-on-write" errors
    - Query-time transformation via SQL views (schema-on-read)

    Reference: instructions.md "The Unbreakable BigQuery Table"
    """
    try:
        # Table name changed for ELT pattern: gemini_raw_logs (not gemini_cli)
        # This is the raw ingestion table; users query the analytics view instead
        table_id = f"{project_id}.{dataset_name}.gemini_raw_logs"

        # "Unbreakable" schema from instructions.md Step 3
        # ALL complex fields stored as JSON STRING blobs
        # Dataflow UDF transforms complex objects → JSON.stringify() → STRING columns
        schema = [
            # Simple fields: pass through as-is
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("receiveTimestamp", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("logName", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("insertId", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("severity", "STRING", mode="NULLABLE"),

            # Complex fields: stored as raw JSON strings
            # This is what makes the pipeline unbreakable - no validation!
            bigquery.SchemaField("resource_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("labels_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("operation_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("httpRequest_json", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("jsonPayload_json", "STRING", mode="NULLABLE"),

            # Trace fields
            bigquery.SchemaField("trace", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("spanId", "STRING", mode="NULLABLE"),
        ]

        table = bigquery.Table(table_id, schema=schema)

        # Configure time partitioning and clustering for query performance
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="timestamp"
        )
        table.clustering_fields = ["logName", "severity"]

        # Add table description
        table.description = "Raw telemetry ingestion table for Dataflow pipeline. DO NOT QUERY DIRECTLY. Use gemini_analytics_view instead."

        # Create the table
        table = client.create_table(table, exists_ok=True)
        logger.info(f"✓ Table {table_id} created with UNBREAKABLE JSON string schema")
        logger.info(f"  - All complex fields stored as STRING (JSON blobs)")
        logger.info(f"  - Zero schema validation during ingestion")
        logger.info(f"  - Schema-on-read via SQL views")

        # NO WAIT NEEDED: Dataflow doesn't use streaming API cache
        # The 150-second wait was only needed for direct Cloud Logging sinks
        # Dataflow uses batch insert API which doesn't have caching issues

    except Exception as e:
        logger.error(f"Failed to create raw logs table: {str(e)}")
        raise


async def verify_dataset_exists(project_id: str, dataset_name: str) -> bool:
    """Verify that a dataset exists."""
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        client = bigquery.Client(project=project_id)
        dataset_id = f"{project_id}.{dataset_name}"

        client.get_dataset(dataset_id)
        logger.info(f"Dataset {dataset_id} exists")
        return True

    except NotFound:
        logger.warning(f"Dataset {dataset_id} not found")
        return False
    except Exception as e:
        logger.error(f"Dataset verification failed: {str(e)}")
        return False


async def verify_table_exists(project_id: str, dataset_name: str, table_name: str = "gemini_raw_logs") -> bool:
    """Verify that a table exists.

    Default table name changed to 'gemini_raw_logs' for ELT pattern.
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
        table_name = validate_table_name(table_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        client.get_table(table_id)
        logger.info(f"Table {table_id} exists")
        return True

    except NotFound:
        logger.warning(f"Table {table_id} not found")
        return False
    except Exception as e:
        logger.error(f"Table verification failed: {str(e)}")
        return False


async def create_analytics_view(
    project_id: str,
    dataset_name: str,
    pseudoanonymize_pii: bool = False
) -> Dict:
    """
    Create analytics view for easy querying of raw telemetry data.

    This view provides schema-on-read transformation of the raw JSON string data
    in gemini_raw_logs table. It extracts common fields and provides a clean interface
    for querying telemetry data.

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        pseudoanonymize_pii: If True, hash user emails with SHA256 for GDPR compliance (default: False)

    Returns:
        Dict with view creation details including pseudoanonymization status
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        client = bigquery.Client(project=project_id)
        view_id = f"{project_id}.{dataset_name}.gemini_analytics_view"

        # Determine column names based on pseudoanonymization setting
        # Note: user.email and installation.id are common attributes in jsonPayload per OpenTelemetry spec
        if pseudoanonymize_pii:
            user_email_field = "TO_HEX(SHA256(COALESCE(JSON_EXTRACT_SCALAR(jsonPayload_json, \"$['user.email']\"), 'unknown'))) AS user_pseudonym"
        else:
            user_email_field = "JSON_EXTRACT_SCALAR(jsonPayload_json, \"$['user.email']\") AS user_email"

        # SQL query that defines the view
        # This implements schema-on-read pattern: parse JSON strings at query time
        view_query = f"""
CREATE OR REPLACE VIEW `{project_id}.{dataset_name}.gemini_analytics_view` AS
SELECT
  -- Timestamp fields
  timestamp,
  receiveTimestamp,

  -- Simple fields
  logName,
  insertId,
  severity,
  trace,
  spanId,

  -- Parse complex JSON fields
  SAFE.PARSE_JSON(resource_json) AS resource,
  SAFE.PARSE_JSON(labels_json) AS labels,
  SAFE.PARSE_JSON(operation_json) AS operation,
  SAFE.PARSE_JSON(httpRequest_json) AS httpRequest,
  SAFE.PARSE_JSON(jsonPayload_json) AS payload,

  -- Extract common telemetry fields for convenience
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['event.name']") AS event_name,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['event.domain']") AS event_domain,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['event.timestamp']") AS event_timestamp,

  -- Common attributes (session, installation, user) from jsonPayload
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['session.id']") AS session_id,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['installation.id']") AS installation_id,

  -- Gen AI request fields (from gemini_cli.api_request/response events)
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.model') AS model,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.model_name') AS model_name,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.auth_type') AS auth_type,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.prompt_id') AS prompt_id,

  -- GenAI semantic convention fields (from gen_ai.client.inference.operation.details)
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.request.model']") AS gen_ai_request_model,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.request.temperature']") AS FLOAT64) AS temperature,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.request.top_p']") AS FLOAT64) AS top_p,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.request.top_k']") AS INT64) AS top_k,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.request.max_tokens']") AS INT64) AS max_output_tokens,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.response.finish_reasons']") AS finish_reason,

  -- Token usage (from gemini_cli.api_response - uses _token_count suffix)
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.input_token_count') AS INT64) AS input_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.output_token_count') AS INT64) AS output_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.cached_content_token_count') AS INT64) AS cached_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.thoughts_token_count') AS INT64) AS thoughts_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.tool_token_count') AS INT64) AS tool_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.total_token_count') AS INT64) AS total_tokens,

  -- GenAI semantic convention token usage (alternative path)
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.usage.input_tokens']") AS INT64) AS gen_ai_input_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, "$['gen_ai.usage.output_tokens']") AS INT64) AS gen_ai_output_tokens,

  -- Tool/function call information
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.function_name') AS function_name,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.function_args') AS function_args,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.duration_ms') AS FLOAT64) AS function_duration_ms,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.success') AS BOOL) AS function_success,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.tool_type') AS tool_type,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.mcp_server_name') AS mcp_server_name,

  -- Performance metrics
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.duration_ms') AS FLOAT64) AS duration_ms,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.status_code') AS INT64) AS status_code,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.error') AS error,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['error.message']") AS error_message,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['error.type']") AS error_code,

  -- User/installation tracking (CONDITIONAL PSEUDOANONYMIZATION)
  JSON_EXTRACT_SCALAR(resource_json, '$.labels.project_id') AS resource_project_id,
  {user_email_field},
  JSON_EXTRACT_SCALAR(labels_json, '$.cli_version') AS cli_version,

  -- Keep original JSON strings for advanced queries
  resource_json,
  labels_json,
  operation_json,
  httpRequest_json,
  jsonPayload_json

FROM `{project_id}.{dataset_name}.gemini_raw_logs`
"""

        # Execute the CREATE OR REPLACE VIEW statement
        query_job = client.query(view_query)
        query_job.result()  # Wait for completion

        logger.info(f"✓ Analytics view created: {view_id}")
        logger.info(f"  - Schema-on-read transformation enabled")
        logger.info(f"  - Extracted 30+ common telemetry fields")
        logger.info(f"  - Pseudoanonymization: {'enabled' if pseudoanonymize_pii else 'disabled'}")
        logger.info(f"  - View ready for querying")

        return {
            "view": view_id,
            "status": "created",
            "fields_extracted": 30,
            "pseudoanonymized": pseudoanonymize_pii,
            "user_field": "user_pseudonym" if pseudoanonymize_pii else "user_email",
            "note": "Analytics view provides easy access to parsed telemetry data"
        }

    except Exception as e:
        logger.error(f"Failed to create analytics view: {str(e)}")
        raise


async def verify_view_exists(project_id: str, dataset_name: str, view_name: str = "gemini_analytics_view") -> bool:
    """
    Verify that a BigQuery view exists.

    Args:
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
        view_name: View name (default: gemini_analytics_view)

    Returns:
        True if view exists, False otherwise
    """
    # Validate inputs
    try:
        project_id = validate_gcp_project_id(project_id)
        dataset_name = validate_dataset_name(dataset_name)
        view_name = validate_view_name(view_name)
    except ValidationError as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise ValueError(f"Invalid input: {str(e)}")

    try:
        client = bigquery.Client(project=project_id)
        view_id = f"{project_id}.{dataset_name}.{view_name}"

        client.get_table(view_id)
        logger.info(f"View {view_id} exists")
        return True

    except NotFound:
        logger.warning(f"View {view_id} not found")
        return False
    except Exception as e:
        logger.error(f"View verification failed: {str(e)}")
        return False


async def enrich_table_metadata_internal(client: bigquery.Client, project_id: str, dataset_name: str) -> None:
    """
    Internal function to enrich RAW table with field descriptions for ELT pattern.

    This is called immediately after table creation to add helpful descriptions
    explaining the JSON string fields and how to query them.

    Args:
        client: BigQuery client instance
        project_id: GCP project ID
        dataset_name: BigQuery dataset name
    """
    try:
        # Table name changed for ELT pattern: "gemini_raw_logs"
        table_id = f"{project_id}.{dataset_name}.gemini_raw_logs"

        logger.info(f"Enriching table metadata for {table_id}")

        # Get the existing table
        table = client.get_table(table_id)

        # Field descriptions for ELT "unbreakable" schema
        # All complex fields are JSON strings - explain how to parse them
        field_descriptions = {
            "timestamp": "Log entry timestamp - when the event occurred (partitioned by day for query performance)",
            "receiveTimestamp": "Time when Cloud Logging received the log entry (may differ from timestamp)",
            "severity": "Log severity level: DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY",
            "insertId": "Unique identifier for the log entry (used for deduplication)",
            "logName": "Full resource name of the log (format: projects/{project}/logs/gemini_cli)",
            "trace": "Cloud Trace identifier for distributed tracing (format: projects/{project}/traces/{trace_id})",
            "spanId": "Span identifier within the trace (hex string)",

            # JSON string fields - explain how to parse them
            "resource_json": "Monitored resource as JSON string. Parse with: JSON_VALUE(resource_json, '$.type'), JSON_VALUE(resource_json, '$.labels.project_id')",
            "labels_json": "User-defined labels as JSON string. Parse with: JSON_VALUE(labels_json, '$.key_name')",
            "operation_json": "Long-running operation info as JSON string. Parse with: JSON_VALUE(operation_json, '$.id')",
            "httpRequest_json": "HTTP request details as JSON string. Parse with: JSON_VALUE(httpRequest_json, '$.requestMethod')",
            "jsonPayload_json": "Gemini CLI telemetry data as JSON string (OpenTelemetry format). Common fields: event.name, session.id, installation.id, user.email, model, status_code, input_token_count, output_token_count, total_token_count, error.message, error.type. Parse with: JSON_EXTRACT_SCALAR(jsonPayload_json, \"$['event.name']\"), JSON_EXTRACT_SCALAR(jsonPayload_json, '$.model'). Use the analytics view for easier querying.",
        }

        # Create new schema with descriptions
        new_schema = []
        for field in table.schema:
            description = field_descriptions.get(field.name, field.description or "")

            # Create new field with description
            new_field = bigquery.SchemaField(
                name=field.name,
                field_type=field.field_type,
                mode=field.mode,
                description=description,
                fields=field.fields  # Preserve nested fields
            )
            new_schema.append(new_field)

        # Update table schema
        table.schema = new_schema

        # Add comprehensive table description for ELT pattern
        table.description = """
⚠️ RAW INGESTION TABLE - DO NOT QUERY DIRECTLY ⚠️

This is a RAW telemetry ingestion table for the Dataflow ELT pipeline.
For analysis and reporting, use the `gemini_analytics_view` instead.

## ELT Architecture

Data Flow:
1. Gemini CLI → Cloud Logging
2. Cloud Logging → Pub/Sub Topic (gemini-telemetry-topic)
3. Pub/Sub → Dataflow Pipeline (with JavaScript UDF transformation)
4. Dataflow → This RAW table (gemini_raw_logs)
5. Query-time transformation → Analytics View (gemini_analytics_view)

## Schema Design: "Unbreakable" JSON Strings

This table uses STRING columns for ALL complex fields to guarantee zero schema validation errors during ingestion.

Why STRING instead of JSON/STRUCT?
- Dataflow JavaScript UDF stringifies all complex objects: JSON.stringify(obj)
- BigQuery never validates structure during ingestion (schema-on-write eliminated)
- Zero ingestion failures due to schema mismatches
- Query-time transformation via SQL views (schema-on-read pattern)

## Table Schema

### Simple Fields (typed)
- `timestamp`: Log entry timestamp (TIMESTAMP) - partitioned by day
- `receiveTimestamp`: Cloud Logging receive time (TIMESTAMP)
- `logName`: Full log resource name (STRING)
- `insertId`: Unique log entry ID for deduplication (STRING)
- `severity`: Log level (STRING): DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY
- `trace`: Cloud Trace ID (STRING)
- `spanId`: Span ID within trace (STRING)

### Complex Fields (JSON strings)
ALL complex fields are stored as raw JSON string blobs:
- `resource_json`: Monitored resource metadata
- `labels_json`: User-defined labels
- `operation_json`: Long-running operation info
- `httpRequest_json`: HTTP request details
- `jsonPayload_json`: Gemini CLI telemetry data (OpenTelemetry format)

## How to Query This Table

**DON'T query this table directly.** Use the `gemini_analytics_view` instead, which provides:
- Pre-parsed JSON fields for easy querying
- Extracted common attributes (event_name, session_id, model, etc.)
- Proper typing and null handling

If you MUST query this table directly, use BigQuery JSON functions:

### Parse JSON Fields
```sql
-- Parse resource_json
SELECT
  timestamp,
  JSON_VALUE(resource_json, '$.type') as resource_type,
  JSON_VALUE(resource_json, '$.labels.project_id') as project_id
FROM `{table_id}`
WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
```

### Extract Telemetry Data
```sql
-- Extract Gemini CLI event data from jsonPayload_json
-- Note: The JSON structure uses flat keys with dots (not nested objects)
-- Token fields use _token_count suffix per the telemetry specification
SELECT
  timestamp,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['event.name']") as event_name,
  JSON_EXTRACT_SCALAR(jsonPayload_json, "$['session.id']") as session_id,
  JSON_EXTRACT_SCALAR(jsonPayload_json, '$.model') as model,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.input_token_count') AS INT64) as input_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.output_token_count') AS INT64) as output_tokens,
  CAST(JSON_EXTRACT_SCALAR(jsonPayload_json, '$.total_token_count') AS INT64) as total_tokens
FROM `{table_id}`
WHERE JSON_EXTRACT_SCALAR(jsonPayload_json, "$['event.name']") = 'gemini_cli.api_response'
  AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY timestamp DESC
LIMIT 100
```

## Data Sources

- Gemini CLI with telemetry enabled (`telemetry.enabled: true`, `telemetry.target: gcp`)
- Cloud Logging sink: `gemini-pubsub-sink`
- Pub/Sub topic: `gemini-telemetry-topic`
- Dataflow job: `gemini-telemetry-pipeline`

## Table Configuration

- **Partitioning**: Daily partitions on `timestamp` field (reduces query costs)
- **Clustering**: Clustered by `logName` and `severity` (improves query performance)
- **Retention**: Data retained according to BigQuery dataset settings

## Telemetry Events

This table contains OpenTelemetry-formatted events from Gemini CLI, including:
- API requests/responses (`gemini_cli.api_request`, `gemini_cli.api_response`)
- Tool calls (`gemini_cli.tool_call`)
- Errors (`gemini_cli.api_error`, `gemini_cli.error`)
- Session events (`gemini_cli.startup`, `gemini_cli.conversation_finished`)
- Extension events (`gemini_cli.extension_*`)
- Agent events (`gemini_cli.agent.*`)
- Model routing (`gemini_cli.model_routing`)
- And 20+ other event types

For detailed event schemas and attributes, see:
- Gemini CLI Telemetry Docs: https://github.com/google-gemini/gemini-cli/blob/main/docs/cli/telemetry.md
- OpenTelemetry Conventions: https://opentelemetry.io/docs/specs/semconv/

## Recommended: Use the Analytics View

Instead of querying this raw table, use `gemini_analytics_view`:
```sql
SELECT
  timestamp,
  event_name,
  session_id,
  model,
  input_tokens,
  output_tokens
FROM `{project}.{dataset}.gemini_analytics_view`
WHERE event_name = 'gemini_cli.api_response'
  AND timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
ORDER BY timestamp DESC
LIMIT 100
```

## Reference Documentation

- BigQuery JSON Functions: https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions
- Cloud Logging Export: https://cloud.google.com/logging/docs/export/bigquery
- Dataflow Templates: https://cloud.google.com/dataflow/docs/guides/templates/provided-streaming
""".format(table_id=table_id, project=project_id, dataset=dataset_name)

        # Update the table
        table = client.update_table(table, ["schema", "description"])

        logger.info(f"✓ Table metadata enriched successfully")
        logger.info(f"  - Updated {len(new_schema)} field descriptions")
        logger.info(f"  - Added comprehensive table description")

    except Exception as e:
        logger.error(f"Failed to enrich table metadata: {str(e)}")
        raise
