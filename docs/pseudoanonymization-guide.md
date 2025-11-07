# Pseudoanonymization in Gemini CLI Telemetry

## Overview

This guide provides a detailed explanation of pseudoanonymization implementation in the Gemini CLI Telemetry application, focusing on SHA-256 hashing for GDPR compliance while maintaining analytical capabilities.

## Table of Contents

1. [What is Pseudoanonymization](#what-is-pseudoanonymization)
2. [Implementation in This Project](#implementation-in-this-project)
3. [Technical Deep Dive](#technical-deep-dive)
4. [Querying Pseudonymized Data](#querying-pseudonymized-data)
5. [Access Control and Security](#access-control-and-security)
6. [GDPR Compliance](#gdpr-compliance)
7. [Example Queries](#example-queries)

---

## What is Pseudoanonymization

### Definition (GDPR Article 4(5))

> "Pseudonymisation means the processing of personal data in such a manner that the personal data can no longer be attributed to a specific data subject without the use of additional information, provided that such additional information is kept separately and is subject to technical and organisational measures to ensure that the personal data are not attributed to an identified or identifiable natural person."

### Key Characteristics

**Pseudoanonymization:**
- Replaces identifying fields with pseudonyms (hashes)
- Allows data analysis without exposing real identities
- Reversible WITH access to the mapping table (cleartext data)
- Reduces privacy risk but doesn't eliminate it

**vs Anonymization:**
- Anonymization is irreversible (no way to identify individuals)
- Pseudoanonymization is reversible with the right access

---

## Implementation in This Project

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     Data Flow Architecture                      │
└─────────────────────────────────────────────────────────────────┘

   Gemini CLI (Client)
          │
          │ user_email: "alice@example.com"
          │ installation_id: "abc-123-def"
          ▼
   Cloud Logging
          │
          ▼
   Log Sink Filter
          │
          ▼
   Pub/Sub Topic
          │
          ▼
   Dataflow Pipeline
          │
          ▼
   ┌──────────────────────────────────────┐
   │   BigQuery: gemini_raw_logs          │
   │   (Cleartext - ACCESS RESTRICTED)    │
   │                                      │
   │   - user_email: "alice@example.com" │
   │   - installation_id: "abc-123-def"  │
   └──────────────────────────────────────┘
          │
          │ CREATE VIEW with SHA256()
          ▼
   ┌──────────────────────────────────────┐
   │   BigQuery: gemini_analytics_view    │
   │   (Pseudonymized - PUBLIC ACCESS)    │
   │                                      │
   │   - user_pseudonym: "a3f9c2..."     │
   │   - installation_id_hash: "7b2e..."  │
   └──────────────────────────────────────┘
          │
          │ Used by 15 analytics views
          ▼
   ┌──────────────────────────────────────┐
   │   Analytics Views                    │
   │   - daily_metrics                    │
   │   - vw_user_activity                 │
   │   - vw_token_usage                   │
   │   - ... (12 more views)              │
   └──────────────────────────────────────┘
```

### When Pseudoanonymization is Applied

The application supports **two modes** configured during deployment (Step 13):

#### Mode 1: Cleartext (Default)
```sql
-- gemini_analytics_view uses cleartext
SELECT
  JSON_VALUE(labels_json, '$.user_email') AS user_email,
  JSON_VALUE(labels_json, '$.installation_id') AS installation_id
FROM `project.dataset.gemini_raw_logs`
```

#### Mode 2: Pseudoanonymized (GDPR-Compliant)
```sql
-- gemini_analytics_view uses SHA256 hashing
SELECT
  TO_HEX(SHA256(COALESCE(JSON_VALUE(labels_json, '$.user_email'), 'unknown'))) AS user_pseudonym,
  TO_HEX(SHA256(COALESCE(JSON_VALUE(labels_json, '$.installation_id'), 'unknown'))) AS installation_id_hash
FROM `project.dataset.gemini_raw_logs`
```

### Configuration During Deployment

The pseudoanonymization mode is selected in the deployment wizard (Step 13):

```typescript
// Frontend configuration
interface DeploymentConfig {
  pseudoanonymizePii: boolean;  // If true, use SHA256 hashing
  logPrompts: boolean;          // If true, include prompts/responses in logs
}
```

```python
# Backend service: services/bigquery_service.py
async def create_analytics_view(
    project_id: str,
    dataset_name: str,
    pseudoanonymize_pii: bool = False  # Deployment-time decision
) -> Dict:
    """
    Create analytics view with optional pseudoanonymization.

    Args:
        pseudoanonymize_pii: If True, hash user emails with SHA256 for GDPR compliance
    """
    if pseudoanonymize_pii:
        user_email_field = "TO_HEX(SHA256(COALESCE(JSON_VALUE(labels_json, '$.user_email'), 'unknown'))) AS user_pseudonym"
        install_id_field = "TO_HEX(SHA256(COALESCE(JSON_VALUE(labels_json, '$.installation_id'), 'unknown'))) AS installation_id_hash"
    else:
        user_email_field = "JSON_VALUE(labels_json, '$.user_email') AS user_email"
        install_id_field = "JSON_VALUE(labels_json, '$.installation_id') AS installation_id"
```

---

## Technical Deep Dive

### SHA-256 Hashing Details

#### What is SHA-256?

- **Cryptographic hash function** producing 256-bit (64 hex characters) output
- **Deterministic**: Same input always produces same output
- **One-way**: Cannot reverse the hash to get original value
- **Collision-resistant**: Extremely unlikely for two inputs to produce same hash

#### Implementation in BigQuery

```sql
-- Example transformation
SELECT
  -- Original value
  'alice@example.com' as original,

  -- SHA256 returns BYTES
  SHA256('alice@example.com') as hash_bytes,

  -- TO_HEX converts to readable hex string
  TO_HEX(SHA256('alice@example.com')) as hash_hex,

  -- Handle NULL values
  TO_HEX(SHA256(COALESCE(NULL, 'unknown'))) as null_handling
```

**Result:**
```
original: alice@example.com
hash_bytes: b'\xa3\xf9\xc2...' (binary)
hash_hex: a3f9c2e4b5d6789abc123def456789012345678901234567890abcdef123456
null_handling: 7b2e4c8a1d3f5e9c2b4a6d8e1f3a5c7b9d0e2f4a6c8b0d2e4f6a8c0b2d4e6f8
```

### Fields That Are Pseudonymized

When `pseudoanonymize_pii = True`:

| Original Field     | Pseudonymized Field      | Purpose                          |
|--------------------|--------------------------|----------------------------------|
| `user_email`       | `user_pseudonym`         | User identification              |
| `installation_id`  | `installation_id_hash`   | CLI installation tracking        |

### Fields That Are NOT Pseudonymized

The following fields remain in cleartext for analytical purposes:

- `timestamp` - When the event occurred
- `model` - Which Gemini model was used
- `cli_version` - CLI version number
- `session_id` - Session identifier (already a UUID)
- `event_name` - Type of telemetry event
- Token counts (input_tokens, output_tokens, total_tokens)
- Performance metrics (duration_ms, error_code)
- All JSON payloads (except extracted user_email)

**Rationale**: These fields don't identify individuals and are essential for analytics.

---

## Querying Pseudonymized Data

### Analytics Without Cleartext Access

When pseudoanonymization is enabled, analysts can perform all standard queries without accessing real email addresses:

#### Example 1: Count Active Users
```sql
SELECT COUNT(DISTINCT user_pseudonym) as active_users
FROM `project.dataset.vw_user_activity`
WHERE last_seen >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
```

**Result:**
```
active_users: 247
```

No email addresses are revealed. You can count users, but can't identify them.

#### Example 2: Top Token Consumers
```sql
SELECT
  user_pseudonym,
  SUM(total_tokens_used) as total_tokens
FROM `project.dataset.vw_user_activity`
GROUP BY user_pseudonym
ORDER BY total_tokens DESC
LIMIT 10
```

**Result:**
```
user_pseudonym: a3f9c2e4b5d6789abc... | total_tokens: 1,250,000
user_pseudonym: 7b2e4c8a1d3f5e9c2b... | total_tokens: 980,000
user_pseudonym: c5d3e7f9a1b3c5d7e9... | total_tokens: 875,000
...
```

You can identify high-usage patterns without knowing WHO the users are.

---

### Identifying Specific Users (With Cleartext Access)

If you need to identify a specific user (e.g., for support), you need:
1. **Access to the cleartext table** (`gemini_raw_logs`)
2. **Permission to compute the hash**

#### Step 1: Compute the Pseudonym
```sql
-- Compute hash for a known email
SELECT TO_HEX(SHA256('alice@example.com')) as user_pseudonym
```

**Result:**
```
user_pseudonym: a3f9c2e4b5d6789abc123def456789012345678901234567890abcdef123456
```

#### Step 2: Query Analytics with the Pseudonym
```sql
-- Now query using the computed pseudonym
SELECT
  user_pseudonym,
  total_requests,
  total_tokens_used,
  first_seen,
  last_seen
FROM `project.dataset.vw_user_activity`
WHERE user_pseudonym = 'a3f9c2e4b5d6789abc123def456789012345678901234567890abcdef123456'
```

**Result:**
```
user_pseudonym: a3f9c2e4b5d6789abc...
total_requests: 1,542
total_tokens_used: 1,250,000
first_seen: 2025-01-15 08:23:45 UTC
last_seen: 2025-11-07 14:32:10 UTC
```

This approach allows support teams to look up specific users WITHOUT exposing email addresses in the analytics views.

---

### Reverse Lookup (Requires Cleartext Access)

If you have a pseudonym and need to find the original email:

```sql
-- Requires SELECT permission on gemini_raw_logs
WITH cleartext AS (
  SELECT DISTINCT
    JSON_VALUE(labels_json, '$.user_email') as user_email,
    TO_HEX(SHA256(JSON_VALUE(labels_json, '$.user_email'))) as user_pseudonym
  FROM `project.dataset.gemini_raw_logs`
  WHERE JSON_VALUE(labels_json, '$.user_email') IS NOT NULL
)
SELECT user_email
FROM cleartext
WHERE user_pseudonym = 'a3f9c2e4b5d6789abc123def456789012345678901234567890abcdef123456'
```

**Result:**
```
user_email: alice@example.com
```

**IMPORTANT**: This query requires access to the raw logs table, which should be restricted to authorized personnel only.

---

## Access Control and Security

### IAM Roles and Permissions

The security model uses BigQuery's native access control to separate cleartext from pseudonymized data:

#### Tier 1: General Analytics Team (No PII Access)
```bash
# Grant access ONLY to analytics views (pseudonymized)
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="group:analytics-team@company.com" \
  --role="roles/bigquery.dataViewer" \
  --condition='expression=resource.name.startsWith("projects/PROJECT_ID/datasets/DATASET_NAME/tables/vw_"),title=analytics-views-only'
```

**What they can access:**
- ✅ All 15 analytics views (pseudonymized data)
- ✅ Aggregated metrics, trends, dashboards
- ❌ Raw logs table (cleartext emails)

**Use case**: Data analysts, dashboard creators, ML teams

---

#### Tier 2: Support Team (Limited PII Access)
```bash
# Grant access to analytics views + ability to compute hashes
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="group:support-team@company.com" \
  --role="roles/bigquery.dataViewer"

# Allow them to run hash computation queries (but not see raw data)
# This is done via a Cloud Function or stored procedure
```

**What they can access:**
- ✅ Analytics views (pseudonymized)
- ✅ Hash computation service (email → pseudonym lookup)
- ❌ Direct access to raw logs

**Use case**: Customer support, user-specific troubleshooting

---

#### Tier 3: Security/Compliance Team (Full Access)
```bash
# Grant full access including raw logs
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="group:security-team@company.com" \
  --role="roles/bigquery.admin"
```

**What they can access:**
- ✅ All analytics views
- ✅ Raw logs table (cleartext)
- ✅ Full reverse lookup capability

**Use case**: Security audits, GDPR data subject requests, legal compliance

---

### Recommended Access Control Configuration

```sql
-- Create separate datasets for different access levels

-- Dataset 1: Raw data (cleartext) - HIGHLY RESTRICTED
CREATE SCHEMA `project.gemini_telemetry_raw`
OPTIONS (
  description = "Raw telemetry logs with cleartext PII - RESTRICTED ACCESS",
  location = "us-central1"
);

-- Dataset 2: Analytics (pseudonymized) - GENERAL ACCESS
CREATE SCHEMA `project.gemini_telemetry_analytics`
OPTIONS (
  description = "Analytics views with pseudonymized PII - GENERAL ACCESS",
  location = "us-central1"
);

-- Move raw table to restricted dataset
-- CREATE TABLE gemini_telemetry_raw.gemini_raw_logs ...

-- Create analytics view in public dataset
CREATE OR REPLACE VIEW `project.gemini_telemetry_analytics.gemini_analytics_view`
AS SELECT ... FROM `project.gemini_telemetry_raw.gemini_raw_logs`
```

Then apply different IAM policies to each dataset:
- `gemini_telemetry_raw`: Only security/compliance team
- `gemini_telemetry_analytics`: All analysts

---

## GDPR Compliance

### How Pseudoanonymization Helps

#### Article 32: Security of Processing
Pseudoanonymization is explicitly mentioned as a security measure:
> "Taking into account the state of the art... implement appropriate technical measures, such as **pseudonymisation**..."

✅ **Compliance**: By hashing PII in the analytics layer, we reduce risk of unauthorized access.

#### Article 5: Data Minimization
> "Personal data shall be adequate, relevant and limited to what is necessary..."

✅ **Compliance**: Analysts only access pseudonyms, not actual email addresses.

#### Article 25: Data Protection by Design
> "Implement appropriate technical and organisational measures... such as **pseudonymisation**..."

✅ **Compliance**: Pseudoanonymization is built into the data pipeline from day one.

---

### What Cleartext Data Remains

Even with pseudoanonymization enabled, the following cleartext data exists:

#### In `gemini_raw_logs` table:
- User email addresses
- Installation IDs
- All telemetry payloads

**Access Control**: Restricted to security/compliance team via IAM

**Retention**: Subject to data retention policies (recommended: 90 days)

**Purpose**: Required for:
- GDPR data subject access requests (Article 15)
- Right to erasure requests (Article 17)
- Security incident investigations
- Billing and usage tracking

---

### GDPR Data Subject Rights

#### Right to Access (Article 15)
Users can request their data:

```sql
-- Retrieve all data for a specific user
SELECT *
FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com'
ORDER BY timestamp DESC
```

#### Right to Erasure (Article 17)
Users can request deletion:

```sql
-- Delete all data for a specific user
DELETE FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com'
```

**Important**: After deletion from raw logs, the pseudonymized data in views will also disappear (views query raw table).

#### Right to Data Portability (Article 20)
Export user data:

```sql
-- Export user's data to JSON
EXPORT DATA OPTIONS(
  uri='gs://bucket/user-data-export/alice-*.json',
  format='JSON'
) AS
SELECT *
FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com'
```

---

## Example Queries

### Example 1: Analyze Token Usage Without PII

```sql
-- Top 10 token consumers (no email addresses revealed)
SELECT
  user_pseudonym,
  SUM(grand_total_tokens) as total_tokens,
  COUNT(DISTINCT day) as days_active,
  ROUND(SUM(grand_total_tokens) / COUNT(DISTINCT day), 2) as avg_daily_tokens
FROM `project.dataset.vw_token_usage`
WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY user_pseudonym
ORDER BY total_tokens DESC
LIMIT 10
```

**Use Case**: Identify high-usage patterns for capacity planning without exposing user identities.

---

### Example 2: Track Specific User (With Hash Lookup)

```sql
-- Step 1: Get user's pseudonym (requires cleartext access OR user provides email)
DECLARE target_pseudonym STRING;
SET target_pseudonym = TO_HEX(SHA256('alice@example.com'));

-- Step 2: Query all activity for this user
SELECT
  day,
  model,
  total_input_tokens,
  total_output_tokens,
  total_cached_tokens
FROM `project.dataset.vw_token_usage`
WHERE user_pseudonym = target_pseudonym
ORDER BY day DESC
```

**Use Case**: Customer support investigating a specific user's usage patterns.

---

### Example 3: Detect Unusual Activity (Anomaly Detection)

```sql
-- Find users with sudden spikes in token usage (no PII needed)
WITH daily_usage AS (
  SELECT
    user_pseudonym,
    day,
    SUM(grand_total_tokens) as tokens_used
  FROM `project.dataset.vw_token_usage`
  WHERE day >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
  GROUP BY user_pseudonym, day
),
avg_usage AS (
  SELECT
    user_pseudonym,
    AVG(tokens_used) as avg_tokens,
    STDDEV(tokens_used) as stddev_tokens
  FROM daily_usage
  GROUP BY user_pseudonym
)
SELECT
  d.user_pseudonym,
  d.day,
  d.tokens_used,
  a.avg_tokens,
  ROUND((d.tokens_used - a.avg_tokens) / NULLIF(a.stddev_tokens, 0), 2) as z_score
FROM daily_usage d
JOIN avg_usage a ON d.user_pseudonym = a.user_pseudonym
WHERE d.tokens_used > a.avg_tokens + (3 * a.stddev_tokens)  -- 3 sigma outliers
ORDER BY z_score DESC
```

**Use Case**: Security team detecting potential abuse or compromised accounts.

---

### Example 4: GDPR Compliance - Data Deletion

```sql
-- Complete user data deletion (requires cleartext access)

-- Step 1: Verify what will be deleted
SELECT
  COUNT(*) as records_to_delete,
  MIN(timestamp) as oldest_record,
  MAX(timestamp) as newest_record
FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com';

-- Step 2: Delete user data
DELETE FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com';

-- Step 3: Verify deletion
SELECT COUNT(*)
FROM `project.dataset.gemini_raw_logs`
WHERE JSON_VALUE(labels_json, '$.user_email') = 'alice@example.com';
-- Expected: 0
```

**Use Case**: Fulfilling GDPR right to erasure request.

---

### Example 5: Create Aggregated Public Dataset

```sql
-- Create fully anonymized dataset for public research
-- (Aggregate to prevent re-identification)

CREATE OR REPLACE TABLE `project.dataset.public_telemetry_stats`
AS
SELECT
  day,
  model,
  cli_version,
  COUNT(DISTINCT user_pseudonym) as user_count,
  SUM(request_count) as total_requests,
  SUM(total_tokens) as total_tokens,
  AVG(average_duration_ms) as avg_duration_ms,
  SUM(error_count) as error_count
FROM `project.dataset.daily_metrics`
GROUP BY day, model, cli_version
HAVING user_count >= 5  -- k-anonymity: minimum 5 users per group
ORDER BY day DESC
```

**Use Case**: Share anonymized telemetry data publicly for research (no users can be identified).

---

## Summary

### Key Takeaways

1. **Pseudoanonymization is Optional**: Configured during deployment (Step 13)

2. **Two-Layer Architecture**:
   - **Raw Layer**: Cleartext PII (restricted access)
   - **Analytics Layer**: Pseudonymized (general access)

3. **SHA-256 Hashing**:
   - Deterministic: Same email → same hash
   - One-way: Cannot reverse without cleartext access
   - 64 hex characters long

4. **Access Control**:
   - Tier 1: Analytics team (pseudonymized views only)
   - Tier 2: Support team (hash lookup capability)
   - Tier 3: Security team (full cleartext access)

5. **GDPR Compliance**:
   - Reduces risk by limiting PII exposure
   - Enables data subject rights (access, deletion, portability)
   - Meets Article 32 security requirements

6. **Analytical Capabilities**:
   - Full analytics without exposing identities
   - User-specific lookup when needed (with proper access)
   - Anomaly detection and pattern analysis

---

## Additional Resources

- [GDPR Article 4(5): Pseudonymisation Definition](https://gdpr-info.eu/art-4-gdpr/)
- [BigQuery Security Best Practices](https://cloud.google.com/bigquery/docs/best-practices-security)
- [SHA-256 Hash Function](https://en.wikipedia.org/wiki/SHA-2)
- [k-Anonymity for Privacy Protection](https://en.wikipedia.org/wiki/K-anonymity)
