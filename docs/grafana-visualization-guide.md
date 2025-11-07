# Grafana Visualization Guide for Gemini CLI Telemetry

## Overview

This guide provides comprehensive instructions for creating Grafana dashboards programmatically to visualize the 15 BigQuery views created by the Gemini CLI Telemetry application.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Grafana Setup with BigQuery](#grafana-setup-with-bigquery)
3. [Visualization Recommendations by View](#visualization-recommendations-by-view)
4. [Code Examples](#code-examples)
5. [Best Practices](#best-practices)

---

## Prerequisites

### Required Components

- **Grafana**: Version 9.0+ (with BigQuery data source plugin)
- **BigQuery**: Dataset with all 15 analytics views deployed
- **GCP Service Account**: With BigQuery Data Viewer role
- **Grafana BigQuery Plugin**: Installed and configured

### Installing BigQuery Data Source

```bash
# Install the BigQuery plugin
grafana-cli plugins install doitintl-bigquery-datasource

# Restart Grafana
sudo systemctl restart grafana-server
```

### Configuring BigQuery Data Source

```bash
# Create a service account for Grafana
gcloud iam service-accounts create grafana-bigquery-reader \
  --display-name="Grafana BigQuery Reader"

# Grant BigQuery Data Viewer role
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:grafana-bigquery-reader@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

# Create and download key
gcloud iam service-accounts keys create grafana-sa-key.json \
  --iam-account=grafana-bigquery-reader@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

---

## Visualization Recommendations by View

### 1. **daily_metrics** (Regular View)
**Purpose**: Daily aggregated metrics by model
**Best Visualization**: Time Series + Stat Panels

**Recommended Panels**:
- **Time Series**: Request count over time (by model)
- **Stat Panel**: Total requests today
- **Bar Chart**: Distinct users by model
- **Time Series**: Token usage trends (input/output)
- **Stat Panel**: Error rate percentage

**Query Example**:
```sql
SELECT
  day,
  model,
  request_count,
  distinct_users,
  total_tokens,
  error_count
FROM `${project_id}.${dataset_name}.daily_metrics`
WHERE day >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
ORDER BY day DESC
```

---

### 2. **vw_user_activity** (Regular View)
**Purpose**: User engagement and usage patterns
**Best Visualization**: Table + Bar Gauge

**Recommended Panels**:
- **Table**: Top 20 users by activity
- **Bar Gauge**: Total tokens used per user (top 10)
- **Stat Panel**: Total active users
- **Histogram**: Distribution of session counts

**Query Example**:
```sql
SELECT
  user_email,
  total_requests,
  total_sessions,
  total_tokens_used,
  TIMESTAMP_DIFF(last_seen, first_seen, DAY) as days_active
FROM `${project_id}.${dataset_name}.vw_user_activity`
ORDER BY total_requests DESC
LIMIT 20
```

---

### 3. **vw_token_usage** (Materialized View)
**Purpose**: Detailed token consumption analysis
**Best Visualization**: Stacked Area Chart + Gauge

**Recommended Panels**:
- **Stacked Area Chart**: Input vs Output tokens over time
- **Pie Chart**: Token distribution by model
- **Gauge**: Daily token consumption vs budget
- **Time Series**: Cached token utilization rate

**Query Example**:
```sql
SELECT
  day,
  model,
  SUM(total_input_tokens) as input_tokens,
  SUM(total_output_tokens) as output_tokens,
  SUM(total_cached_tokens) as cached_tokens
FROM `${project_id}.${dataset_name}.vw_token_usage`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY day, model
ORDER BY day ASC
```

---

### 4. **vw_error_analysis** (Regular View)
**Purpose**: Error tracking and debugging
**Best Visualization**: Heatmap + Logs Panel

**Recommended Panels**:
- **Heatmap**: Error frequency by day and CLI version
- **Table**: Error messages with affected user counts
- **Bar Chart**: Top 10 errors by occurrence
- **Time Series**: Error rate trend

**Query Example**:
```sql
SELECT
  day,
  cli_version,
  error_code,
  error_message,
  error_count,
  distinct_users_affected
FROM `${project_id}.${dataset_name}.vw_error_analysis`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
ORDER BY error_count DESC
LIMIT 50
```

---

### 5. **vw_malformed_json_responses** (Materialized View)
**Purpose**: Track malformed JSON from generateJson command
**Best Visualization**: Time Series + Alert List

**Recommended Panels**:
- **Time Series**: Malformed JSON count by model over time
- **Stat Panel**: Total malformed responses (with alert threshold)
- **Table**: Users affected by malformed JSON
- **Bar Chart**: Malformed JSON by model

**Query Example**:
```sql
SELECT
  day,
  model,
  SUM(malformed_json_count) as total_malformed
FROM `${project_id}.${dataset_name}.vw_malformed_json_responses`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY day, model
ORDER BY day ASC
```

---

### 6. **vw_feature_adoption** (Materialized View)
**Purpose**: Track feature usage across CLI versions
**Best Visualization**: Bar Chart + Growth Curve

**Recommended Panels**:
- **Bar Chart**: Feature adoption by CLI version
- **Time Series**: Slash command usage growth
- **Stat Panel**: Agent mode adoption rate
- **Pie Chart**: Feature usage distribution

**Query Example**:
```sql
SELECT
  cli_version,
  SUM(slash_command_usage_count) as slash_commands,
  SUM(agent_mode_usage_count) as agent_mode,
  SUM(extension_install_count) as extensions
FROM `${project_id}.${dataset_name}.vw_feature_adoption`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY cli_version
ORDER BY cli_version DESC
```

---

### 7. **vw_conversation_analysis** (Materialized View)
**Purpose**: Analyze chat sessions and engagement
**Best Visualization**: Histogram + Stat Panels

**Recommended Panels**:
- **Histogram**: Distribution of turn counts
- **Stat Panel**: Average conversation length
- **Bar Chart**: Conversations by approval mode
- **Time Series**: Daily conversation volume

**Query Example**:
```sql
SELECT
  day,
  approval_mode,
  conversation_count,
  avg_turn_count,
  min_turn_count,
  max_turn_count
FROM `${project_id}.${dataset_name}.vw_conversation_analysis`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY day DESC
```

---

### 8. **vw_tool_performance** (Regular View)
**Purpose**: Tool call performance and success rates
**Best Visualization**: Table + Success Rate Gauge

**Recommended Panels**:
- **Table**: Tool performance metrics (sortable)
- **Gauge**: Overall tool success rate
- **Bar Chart**: Most frequently used tools
- **Time Series**: Tool failure rate over time

**Query Example**:
```sql
SELECT
  function_name,
  SUM(total_calls) as calls,
  SUM(successful_calls) as successes,
  SUM(failed_calls) as failures,
  AVG(success_rate) as success_rate,
  AVG(average_execution_duration_ms) as avg_duration_ms
FROM `${project_id}.${dataset_name}.vw_tool_performance`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY function_name
ORDER BY calls DESC
```

---

### 9. **vw_cli_performance_and_resilience** (Regular View)
**Purpose**: Monitor CLI operational health
**Best Visualization**: Stat Panels + Line Graphs

**Recommended Panels**:
- **Stat Panel**: Flash fallback count (with threshold alert)
- **Stat Panel**: Content retry success rate
- **Bar Gauge**: Compression ratio effectiveness
- **Time Series**: Resilience events over time

**Query Example**:
```sql
SELECT
  day,
  model,
  flash_fallback_count,
  content_retry_count,
  content_retry_failure_count,
  SAFE_DIVIDE(content_retry_count - content_retry_failure_count, content_retry_count) as retry_success_rate,
  avg_compression_ratio
FROM `${project_id}.${dataset_name}.vw_cli_performance_and_resilience`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
ORDER BY day DESC
```

---

### 10. **vw_model_routing_analysis** (Regular View)
**Purpose**: Analyze model router behavior
**Best Visualization**: Sankey Diagram + Stats

**Recommended Panels**:
- **Table**: Routing decisions by source and model
- **Gauge**: Routing failure rate
- **Stat Panel**: Average routing latency
- **Bar Chart**: Decisions by decision source

**Query Example**:
```sql
SELECT
  decision_source,
  decision_model,
  decision_count,
  failure_count,
  failure_rate,
  avg_routing_latency_ms
FROM `${project_id}.${dataset_name}.vw_model_routing_analysis`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY decision_count DESC
```

---

### 11. **vw_quota_tracking** (Regular View)
**Purpose**: RPM/RPD quota monitoring
**Best Visualization**: Time Series + Alert Thresholds

**Recommended Panels**:
- **Time Series**: Requests per minute (with 120 RPM threshold line)
- **Time Series**: Requests per day (with 2000 RPD threshold line)
- **Table**: Users approaching quota limits
- **Alert List**: Recent quota violations

**Query Example**:
```sql
SELECT
  minute,
  user_email,
  model,
  requests_per_minute,
  requests_per_day,
  total_tokens_per_minute
FROM `${project_id}.${dataset_name}.vw_quota_tracking`
WHERE day = CURRENT_DATE()
  AND requests_per_minute > 100  -- Alert threshold
ORDER BY minute DESC
LIMIT 100
```

---

### 12. **vw_user_configuration** (Regular View)
**Purpose**: User configuration tracking
**Best Visualization**: Table + Pie Charts

**Recommended Panels**:
- **Table**: Latest configuration by user
- **Pie Chart**: Sandbox enabled/disabled distribution
- **Pie Chart**: Prompt logging preferences
- **Bar Chart**: Extension usage by user

**Query Example**:
```sql
SELECT
  user_email,
  cli_version,
  sandbox_enabled,
  log_prompts_enabled,
  output_format,
  extension_count,
  last_config_timestamp
FROM `${project_id}.${dataset_name}.vw_user_configuration`
ORDER BY last_config_timestamp DESC
LIMIT 100
```

---

### 13. **daily_rollup_table** (Scheduled Query Table)
**Purpose**: Pre-aggregated daily metrics
**Best Visualization**: Time Series Dashboard

**Recommended Panels**:
- **Time Series**: Historical trends (optimized for fast queries)
- **Stat Panel**: 30-day totals
- **Bar Chart**: Model comparison

**Query Example**:
```sql
SELECT
  day,
  model,
  request_count,
  distinct_users,
  total_tokens,
  error_count
FROM `${project_id}.${dataset_name}.daily_rollup_table`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY day DESC
```

---

### 14. **quota_alerts_table** (Scheduled Query Table)
**Purpose**: Quota violation tracking
**Best Visualization**: Alert List + Timeline

**Recommended Panels**:
- **Table**: Recent quota violations (last 24 hours)
- **Timeline**: Violation events
- **Bar Chart**: Top violators
- **Stat Panel**: Current violations count

**Query Example**:
```sql
SELECT
  alert_date,
  user_identifier,
  model,
  alert_type,
  current_value,
  quota_limit,
  violation_time
FROM `${project_id}.${dataset_name}.quota_alerts_table`
WHERE alert_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
ORDER BY alert_date DESC, violation_time DESC
```

---

### 15. **weekly_rollup_table** (Scheduled Query Table)
**Purpose**: Weekly aggregated metrics
**Best Visualization**: Weekly Trend Dashboard

**Recommended Panels**:
- **Time Series**: Week-over-week comparisons
- **Stat Panel**: This week vs last week
- **Bar Chart**: Weekly model distribution

**Query Example**:
```sql
SELECT
  week_start_date,
  model,
  request_count,
  distinct_users,
  total_tokens,
  SAFE_DIVIDE(error_count, request_count) as error_rate
FROM `${project_id}.${dataset_name}.weekly_rollup_table`
ORDER BY week_start_date DESC
LIMIT 12  -- Last 12 weeks
```

---

## Code Examples

### Programmatic Dashboard Creation with Grafana API

#### 1. Create Complete Dashboard via API

```python
import requests
import json

GRAFANA_URL = "http://localhost:3000"
GRAFANA_API_KEY = "your-api-key"
PROJECT_ID = "your-project-id"
DATASET_NAME = "gemini_telemetry"

headers = {
    "Authorization": f"Bearer {GRAFANA_API_KEY}",
    "Content-Type": "application/json"
}

# Complete dashboard definition
dashboard_json = {
    "dashboard": {
        "title": "Gemini CLI Telemetry - Overview",
        "tags": ["gemini", "telemetry", "bigquery"],
        "timezone": "browser",
        "schemaVersion": 38,
        "version": 0,
        "refresh": "5m",
        "panels": [
            {
                "id": 1,
                "title": "Daily Request Volume",
                "type": "timeseries",
                "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0},
                "targets": [{
                    "datasource": {"type": "doitintl-bigquery-datasource"},
                    "rawSql": f"""
                        SELECT
                          TIMESTAMP(day) as time,
                          model,
                          request_count as value
                        FROM `{PROJECT_ID}.{DATASET_NAME}.daily_metrics`
                        WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                        ORDER BY day ASC
                    """,
                    "format": "time_series"
                }],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "axisLabel": "Requests",
                            "drawStyle": "line",
                            "fillOpacity": 10,
                            "showPoints": "auto"
                        }
                    }
                }
            },
            {
                "id": 2,
                "title": "Total Active Users",
                "type": "stat",
                "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
                "targets": [{
                    "datasource": {"type": "doitintl-bigquery-datasource"},
                    "rawSql": f"""
                        SELECT COUNT(DISTINCT user_email) as value
                        FROM `{PROJECT_ID}.{DATASET_NAME}.vw_user_activity`
                    """,
                    "format": "table"
                }],
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": 0, "color": "red"},
                                {"value": 10, "color": "yellow"},
                                {"value": 50, "color": "green"}
                            ]
                        }
                    }
                }
            },
            {
                "id": 3,
                "title": "Error Rate (%)",
                "type": "gauge",
                "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
                "targets": [{
                    "datasource": {"type": "doitintl-bigquery-datasource"},
                    "rawSql": f"""
                        SELECT
                          SAFE_DIVIDE(SUM(error_count), SUM(request_count)) * 100 as value
                        FROM `{PROJECT_ID}.{DATASET_NAME}.daily_metrics`
                        WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                    """,
                    "format": "table"
                }],
                "fieldConfig": {
                    "defaults": {
                        "max": 10,
                        "min": 0,
                        "thresholds": {
                            "mode": "absolute",
                            "steps": [
                                {"value": 0, "color": "green"},
                                {"value": 2, "color": "yellow"},
                                {"value": 5, "color": "red"}
                            ]
                        },
                        "unit": "percent"
                    }
                }
            },
            {
                "id": 4,
                "title": "Token Usage by Model",
                "type": "piechart",
                "gridPos": {"h": 8, "w": 12, "x": 12, "y": 4},
                "targets": [{
                    "datasource": {"type": "doitintl-bigquery-datasource"},
                    "rawSql": f"""
                        SELECT
                          model,
                          SUM(grand_total_tokens) as value
                        FROM `{PROJECT_ID}.{DATASET_NAME}.vw_token_usage`
                        WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                        GROUP BY model
                    """,
                    "format": "table"
                }],
                "options": {
                    "legend": {"displayMode": "table", "placement": "right"},
                    "pieType": "pie",
                    "displayLabels": ["percent"]
                }
            }
        ]
    },
    "overwrite": True
}

# Create dashboard
response = requests.post(
    f"{GRAFANA_URL}/api/dashboards/db",
    headers=headers,
    data=json.dumps(dashboard_json)
)

if response.status_code == 200:
    print(f"Dashboard created: {response.json()['url']}")
else:
    print(f"Error: {response.status_code} - {response.text}")
```

---

#### 2. Create Alert Rules for Quota Violations

```python
# Create alert rule for RPM quota violations
alert_rule = {
    "title": "High RPM Quota Usage",
    "condition": "B",
    "data": [
        {
            "refId": "A",
            "queryType": "sql",
            "datasourceUid": "bigquery-datasource-uid",
            "rawSql": f"""
                SELECT
                  minute as time,
                  user_email,
                  requests_per_minute as value
                FROM `{PROJECT_ID}.{DATASET_NAME}.vw_quota_tracking`
                WHERE day = CURRENT_DATE()
                  AND requests_per_minute > 100
            """
        },
        {
            "refId": "B",
            "queryType": "",
            "model": {
                "expr": "A",
                "reducer": "last",
                "settings": {"mode": "absolute"},
                "evaluator": {
                    "params": [120],
                    "type": "gt"
                }
            }
        }
    ],
    "noDataState": "NoData",
    "execErrState": "Alerting",
    "for": "5m",
    "annotations": {
        "description": "User {{$labels.user_email}} exceeded 120 RPM quota",
        "summary": "RPM quota violation detected"
    },
    "labels": {
        "severity": "warning",
        "team": "platform"
    }
}

response = requests.post(
    f"{GRAFANA_URL}/api/v1/provisioning/alert-rules",
    headers=headers,
    data=json.dumps(alert_rule)
)
```

---

#### 3. Terraform Configuration for Dashboard Provisioning

```hcl
# Configure Grafana provider
terraform {
  required_providers {
    grafana = {
      source = "grafana/grafana"
      version = "~> 2.0"
    }
  }
}

provider "grafana" {
  url  = "http://localhost:3000"
  auth = var.grafana_api_key
}

# BigQuery data source
resource "grafana_data_source" "bigquery" {
  type = "doitintl-bigquery-datasource"
  name = "Gemini Telemetry BigQuery"

  json_data_encoded = jsonencode({
    authenticationType = "gce"
    defaultProject     = var.project_id
  })

  secure_json_data_encoded = jsonencode({
    privateKey = file("${path.module}/grafana-sa-key.json")
  })
}

# Dashboard resource
resource "grafana_dashboard" "gemini_overview" {
  config_json = templatefile("${path.module}/dashboards/overview.json", {
    project_id   = var.project_id
    dataset_name = var.dataset_name
  })
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "dataset_name" {
  description = "BigQuery dataset name"
  type        = string
  default     = "gemini_telemetry"
}

variable "grafana_api_key" {
  description = "Grafana API key"
  type        = string
  sensitive   = true
}
```

---

#### 4. Python Script to Generate All 15 Dashboard Panels

```python
#!/usr/bin/env python3
"""
Generate complete Grafana dashboard with all 15 BigQuery views
"""

import requests
import json
from typing import List, Dict

class GrafanaDashboardBuilder:
    def __init__(self, grafana_url: str, api_key: str, project_id: str, dataset_name: str):
        self.grafana_url = grafana_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.panel_id = 1
        self.y_position = 0

    def create_timeseries_panel(self, title: str, sql: str, x: int = 0, w: int = 12, h: int = 8) -> Dict:
        """Create a time series panel"""
        panel = {
            "id": self.panel_id,
            "title": title,
            "type": "timeseries",
            "gridPos": {"h": h, "w": w, "x": x, "y": self.y_position},
            "targets": [{
                "datasource": {"type": "doitintl-bigquery-datasource"},
                "rawSql": sql,
                "format": "time_series"
            }],
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "palette-classic"},
                    "custom": {"drawStyle": "line", "fillOpacity": 10}
                }
            }
        }
        self.panel_id += 1
        if x + w >= 24:
            self.y_position += h
        return panel

    def create_stat_panel(self, title: str, sql: str, x: int = 0, w: int = 6, h: int = 4, thresholds: List = None) -> Dict:
        """Create a stat panel"""
        if thresholds is None:
            thresholds = [
                {"value": 0, "color": "green"}
            ]

        panel = {
            "id": self.panel_id,
            "title": title,
            "type": "stat",
            "gridPos": {"h": h, "w": w, "x": x, "y": self.y_position},
            "targets": [{
                "datasource": {"type": "doitintl-bigquery-datasource"},
                "rawSql": sql,
                "format": "table"
            }],
            "fieldConfig": {
                "defaults": {
                    "color": {"mode": "thresholds"},
                    "thresholds": {
                        "mode": "absolute",
                        "steps": thresholds
                    }
                }
            }
        }
        self.panel_id += 1
        if x + w >= 24:
            self.y_position += h
        return panel

    def create_table_panel(self, title: str, sql: str, x: int = 0, w: int = 24, h: int = 8) -> Dict:
        """Create a table panel"""
        panel = {
            "id": self.panel_id,
            "title": title,
            "type": "table",
            "gridPos": {"h": h, "w": w, "x": x, "y": self.y_position},
            "targets": [{
                "datasource": {"type": "doitintl-bigquery-datasource"},
                "rawSql": sql,
                "format": "table"
            }]
        }
        self.panel_id += 1
        self.y_position += h
        return panel

    def build_complete_dashboard(self) -> Dict:
        """Build dashboard with all 15 views"""
        panels = []

        # Panel 1: Daily Metrics - Request Volume
        panels.append(self.create_timeseries_panel(
            "Daily Request Volume",
            f"""
            SELECT TIMESTAMP(day) as time, model, request_count as value
            FROM `{self.project_id}.{self.dataset_name}.daily_metrics`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ORDER BY day ASC
            """,
            x=0, w=12
        ))

        # Panel 2: User Activity - Active Users
        panels.append(self.create_stat_panel(
            "Active Users (30d)",
            f"""
            SELECT COUNT(DISTINCT user_email) as value
            FROM `{self.project_id}.{self.dataset_name}.vw_user_activity`
            WHERE last_seen >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
            """,
            x=12, w=6
        ))

        # Panel 3: Error Analysis - Error Rate
        panels.append(self.create_stat_panel(
            "Error Rate (7d)",
            f"""
            SELECT SAFE_DIVIDE(SUM(error_count), SUM(request_count)) * 100 as value
            FROM `{self.project_id}.{self.dataset_name}.daily_metrics`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            """,
            x=18, w=6,
            thresholds=[
                {"value": 0, "color": "green"},
                {"value": 2, "color": "yellow"},
                {"value": 5, "color": "red"}
            ]
        ))

        # Panel 4: Token Usage - Trend
        panels.append(self.create_timeseries_panel(
            "Token Usage Over Time",
            f"""
            SELECT
              TIMESTAMP(day) as time,
              'Input Tokens' as metric,
              SUM(total_input_tokens) as value
            FROM `{self.project_id}.{self.dataset_name}.vw_token_usage`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY day
            UNION ALL
            SELECT
              TIMESTAMP(day) as time,
              'Output Tokens' as metric,
              SUM(total_output_tokens) as value
            FROM `{self.project_id}.{self.dataset_name}.vw_token_usage`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY day
            ORDER BY time ASC
            """,
            x=0, w=24
        ))

        # Panel 5: Top Errors Table
        panels.append(self.create_table_panel(
            "Top Errors (Last 7 Days)",
            f"""
            SELECT
              error_code,
              error_message,
              SUM(error_count) as occurrences,
              SUM(distinct_users_affected) as users_affected
            FROM `{self.project_id}.{self.dataset_name}.vw_error_analysis`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY error_code, error_message
            ORDER BY occurrences DESC
            LIMIT 20
            """
        ))

        # Panel 6: Tool Performance
        panels.append(self.create_table_panel(
            "Tool Performance Metrics",
            f"""
            SELECT
              function_name,
              SUM(total_calls) as calls,
              AVG(success_rate) * 100 as success_rate_pct,
              AVG(average_execution_duration_ms) as avg_duration_ms
            FROM `{self.project_id}.{self.dataset_name}.vw_tool_performance`
            WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
            GROUP BY function_name
            ORDER BY calls DESC
            LIMIT 15
            """
        ))

        # Panel 7: Quota Tracking
        panels.append(self.create_timeseries_panel(
            "RPM Quota Usage (Last Hour)",
            f"""
            SELECT
              minute as time,
              user_email,
              requests_per_minute as value
            FROM `{self.project_id}.{self.dataset_name}.vw_quota_tracking`
            WHERE minute >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
            ORDER BY minute ASC
            """,
            x=0, w=24
        ))

        # Build final dashboard JSON
        dashboard = {
            "dashboard": {
                "title": "Gemini CLI Telemetry - Complete Overview",
                "tags": ["gemini", "telemetry", "analytics"],
                "timezone": "browser",
                "panels": panels,
                "refresh": "5m",
                "schemaVersion": 38,
                "version": 0
            },
            "overwrite": True
        }

        return dashboard

    def deploy(self):
        """Deploy dashboard to Grafana"""
        dashboard = self.build_complete_dashboard()

        response = requests.post(
            f"{self.grafana_url}/api/dashboards/db",
            headers=self.headers,
            data=json.dumps(dashboard)
        )

        if response.status_code == 200:
            result = response.json()
            print(f"✓ Dashboard deployed successfully")
            print(f"  URL: {result['url']}")
            print(f"  UID: {result['uid']}")
        else:
            print(f"✗ Deployment failed: {response.status_code}")
            print(f"  Error: {response.text}")

        return response

# Usage
if __name__ == "__main__":
    builder = GrafanaDashboardBuilder(
        grafana_url="http://localhost:3000",
        api_key="your-api-key",
        project_id="your-project-id",
        dataset_name="gemini_telemetry"
    )

    builder.deploy()
```

---

## Best Practices

### 1. **Query Optimization**
- Use materialized views for frequently accessed data
- Leverage the scheduled query tables for historical analysis
- Add time range filters to all queries
- Use `LIMIT` clauses to prevent large result sets

### 2. **Dashboard Performance**
- Set appropriate refresh intervals (5m for real-time, 1h for historical)
- Use cached queries where possible
- Avoid N+1 query patterns
- Implement query result caching in Grafana

### 3. **Alert Configuration**
- Set up alerts for:
  - RPM quota > 100 (warning) and > 120 (critical)
  - RPD quota > 1800 (warning) and > 2000 (critical)
  - Error rate > 5%
  - Malformed JSON responses > 10/day

### 4. **Security Considerations**
- Use service accounts with minimum required permissions (BigQuery Data Viewer)
- Rotate API keys regularly
- Implement row-level security in BigQuery if needed
- Use Grafana's built-in authentication and authorization

### 5. **Dashboard Organization**
Create separate dashboards for different audiences:
- **Executive Dashboard**: High-level metrics, user growth, token costs
- **Engineering Dashboard**: Error rates, tool performance, resilience metrics
- **Product Dashboard**: Feature adoption, conversation analysis, user engagement
- **Operations Dashboard**: Quota tracking, alerts, system health

---

## Additional Resources

- [Grafana BigQuery Plugin Documentation](https://grafana.com/grafana/plugins/doitintl-bigquery-datasource/)
- [Grafana Provisioning Documentation](https://grafana.com/docs/grafana/latest/administration/provisioning/)
- [BigQuery Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Grafana Dashboard API Reference](https://grafana.com/docs/grafana/latest/developers/http_api/dashboard/)
