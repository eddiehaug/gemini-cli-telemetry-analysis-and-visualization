/**
 * JavaScript UDF for Dataflow transformation
 *
 * Transforms Cloud Logging JSON entries into the "unbreakable" BigQuery schema:
 * - Simple fields (timestamp, logName, etc.) → pass through as-is
 * - Complex nested objects (resource, labels, etc.) → stringify to JSON blobs
 *
 * This ensures zero schema validation errors during BigQuery ingestion.
 *
 * Used by: Dataflow PubSub_Subscription_to_BigQuery template
 * Target table: gemini_raw_logs (with all complex fields as STRING)
 */

function transform(inJson) {
  var src = JSON.parse(inJson);
  var dst = {};

  // Simple fields: pass through as-is
  dst.timestamp = src.timestamp;
  dst.receiveTimestamp = src.receiveTimestamp;
  dst.logName = src.logName;
  dst.insertId = src.insertId;
  dst.severity = src.severity;
  dst.trace = src.trace;
  dst.spanId = src.spanId;

  // Complex fields: stringify to JSON blobs
  // These will be stored as STRING in BigQuery for "unbreakable" schema
  if (src.resource) {
    dst.resource_json = JSON.stringify(src.resource);
  }

  if (src.labels) {
    dst.labels_json = JSON.stringify(src.labels);
  }

  if (src.operation) {
    dst.operation_json = JSON.stringify(src.operation);
  }

  if (src.httpRequest) {
    dst.httpRequest_json = JSON.stringify(src.httpRequest);
  }

  if (src.jsonPayload) {
    dst.jsonPayload_json = JSON.stringify(src.jsonPayload);
  }

  // Return the transformed record as JSON string
  return JSON.stringify(dst);
}
