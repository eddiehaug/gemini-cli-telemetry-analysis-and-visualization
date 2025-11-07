# Changelog

All notable changes to the Gemini CLI Telemetry application will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added - 429 Rate Limit Error Tracking and AI-Powered Analysis (2025-11-07)

#### New Features

**1. Enhanced Quota Tracking (`vw_quota_tracking`)**

Added comprehensive 429 rate limit error tracking to the quota monitoring view:

**New Fields**:
- `hour` - Hourly timestamp for hourly aggregations
- `error_429_count_per_minute` - Count of 429 errors per minute
- `error_429_count_per_hour` - Count of 429 errors per hour (window function)
- `error_429_count_per_day` - Count of 429 errors per day (window function)
- `total_errors_per_minute` - Count of all HTTP errors (status_code >= 400) per minute

**Updated Event Filtering**:
- Changed FROM: Only `gemini_cli.api_response` events
- Changed TO: Both `gemini_cli.api_response` AND `gemini_cli.api_error` events
- Ensures 429 errors are properly counted in quota tracking

**2. AI-Powered Error Analysis (`vw_429_error_summary`)**

New view that uses Gemini via `ML.GENERATE_TEXT` to analyze rate limit errors:

**What It Does**:
- Analyzes the last 250 errors where `status_code = 429`
- Uses Gemini 2.5 Flash to identify common patterns
- Provides actionable recommendations
- Returns natural language summary of error causes

**Fields**:
- `total_429_errors` - Count of 429 errors in the sample
- `oldest_error` - Timestamp of oldest error analyzed
- `newest_error` - Timestamp of newest error analyzed
- `error_summary` - Gemini-generated analysis with:
  - Common error patterns
  - Affected models
  - Potential causes (which quotas exceeded)
  - 2-3 actionable recommendations

**3. Vertex AI Integration**

New infrastructure for BigQuery ML integration:

**Components Added**:
- `vertex_ai_setup.py` - Helper service for Vertex AI configuration
- Functions to create BigQuery connections to Vertex AI
- Automated IAM permission management
- Remote Gemini model creation (`gemini_flash_model`)

**Prerequisites**:
- Vertex AI API must be enabled: `gcloud services enable aiplatform.googleapis.com`
- Service account needs `roles/aiplatform.user`
- BigQuery connection to Vertex AI

#### Migration Path

**For New Deployments** (Step 16):
- Vertex AI setup is included in deployment wizard
- All views created automatically with 429 tracking

**For Existing Deployments**:
Run the migration script:
```bash
python add_429_tracking.py --project PROJECT_ID --dataset DATASET_NAME
```

See `ADD_429_TRACKING_README.md` for detailed migration instructions.

#### Files Modified

**Backend Services**:
- `app/backend/services/bigquery_views_service.py`:
  - Updated `create_quota_tracking_view()` (lines 547-624)
  - Added `create_429_error_summary_view()` (lines 682-776)
  - Updated `VIEW_CREATION_FUNCTIONS` and `EXPECTED_VIEW_NAMES` lists
- `app/backend/services/vertex_ai_setup.py` (NEW):
  - `create_vertex_ai_connection()`
  - `grant_vertex_ai_permissions()`
  - `create_remote_gemini_model()`
  - `setup_vertex_ai_for_bigquery()`

**Migration Tools**:
- `add_429_tracking.py` (NEW) - Migration script for existing deployments
- `ADD_429_TRACKING_README.md` (NEW) - Migration guide and documentation

**Documentation**:
- `app/CHANGELOG.md` - This entry
- `app/README.md` - Added Vertex AI setup section (pending)

#### Cost Considerations

**BigQuery**:
- Views are free (no storage cost)
- Standard query pricing applies

**ML.GENERATE_TEXT**:
- Pricing: ~$0.000125 per 1K characters
- Cost per query to `vw_429_error_summary`: ~$0.006
- On-demand only (costs only when queried)
- Estimated monthly cost (queried once/day): ~$0.18

#### Use Cases

**1. Quota Monitoring Dashboard**:
- Add 429 error count scorecards to Looker Studio
- Create time series showing 429 trends
- Correlation with RPM/RPD metrics

**2. Root Cause Analysis**:
- Query `vw_429_error_summary` when 429 errors occur
- Get AI-generated insights on error patterns
- Identify which models or users are affected

**3. Proactive Alerting**:
```sql
SELECT
  DATE(minute) as day,
  SUM(error_429_count_per_day) as total_429s
FROM `project.dataset.vw_quota_tracking`
WHERE DATE(minute) = CURRENT_DATE()
GROUP BY day
HAVING total_429s > 0
```

#### Testing

**Verified**:
- ✅ 429 errors counted correctly in `vw_quota_tracking`
- ✅ Hourly and daily aggregations using window functions
- ✅ `vw_429_error_summary` creates successfully with Vertex AI
- ✅ ML.GENERATE_TEXT integration works with Gemini
- ✅ Migration script tested on existing deployment

---

### Fixed - BigQuery Analytics Views JSON Field Extraction (2025-11-07)

#### Issue: Null Values in Looker Studio Dashboard
**Problem**: Looker Studio dashboard showed `null` for model names and missing token counts despite data existing in BigQuery raw logs.

**Root Cause**: BigQuery analytics views were using incorrect JSON field paths that didn't match the actual Gemini CLI telemetry structure:
1. **Wrong JSON function**: Used `JSON_VALUE()` which has compatibility issues with certain path syntaxes
2. **Incorrect model path**: Used `$.gen_ai.request.model` instead of `$.model`
3. **Incorrect token paths**: Used `$.input_tokens` instead of `$.input_token_count`
4. **Wrong attribute access**: Used `payload.attributes.field` instead of direct extraction from `jsonPayload_json`
5. **Wrong user email source**: Extracted from `labels_json` instead of `jsonPayload_json`

**Investigation**:
- Analyzed sample JSON payload from `gemini_raw_logs` table
- Discovered flat JSON structure with dotted keys (e.g., `"event.name"`, `"session.id"`)
- Reviewed [official Gemini CLI telemetry documentation](https://github.com/google-gemini/gemini-cli/blob/main/docs/cli/telemetry.md)
- Identified misalignment between code and OpenTelemetry specification

**Fix Applied**:

**Files Modified**:
- `app/backend/services/bigquery_service.py` (lines 251-344): Base analytics view
  - Changed `JSON_VALUE()` → `JSON_EXTRACT_SCALAR()` (BigQuery compatible)
  - Updated model extraction: `$.gen_ai.request.model` → `$.model`
  - Fixed token fields: `$.input_tokens` → `$.input_token_count`, etc.
  - Added new fields: `model_name`, `auth_type`, `prompt_id`, `event.timestamp`
  - Fixed user email: `labels_json` → `jsonPayload_json, "$['user.email']"`
  - Added GenAI semantic convention fields
  - Updated field paths for dotted keys: `$['event.name']`, `$['session.id']`

- `app/backend/services/bigquery_views_service.py`: Downstream analytics views
  - `create_conversation_analysis_view` (lines 362-386): Fixed `payload.attributes.*` → direct extraction
  - `create_cli_performance_and_resilience_view` (lines 460-482): Fixed compression token fields
  - `create_model_routing_analysis_view` (lines 509-528): Fixed routing decision fields
  - `create_user_configuration_view` (lines 615-646): Fixed config attribute extraction

**New Utility Scripts** (in project root):
- `fix_views_and_tables.py`: Automated script to delete and recreate all views/tables
  - Preserves `gemini_raw_logs` and `gemini_analytics_view`
  - Deletes and recreates 12 analytics views
  - Deletes and recreates 3 rollup/alert tables
  - Interactive confirmation with detailed progress reporting
- `FIX_VIEWS_README.md`: Comprehensive usage guide for the fix script

**Correct JSON Paths** (per official spec):
- Model: `$.model` or `$.model_name`
- Tokens: `$.input_token_count`, `$.output_token_count`, `$.total_token_count`
- Event: `$['event.name']`, `$['event.timestamp']`
- Session: `$['session.id']`, `$['installation.id']`
- User: `$['user.email']` (from `jsonPayload_json`)
- Errors: `$['error.message']`, `$['error.type']`
- Config attributes: Direct from `jsonPayload_json` (not `payload.attributes`)

**Testing**:
- Manually recreated `gemini_analytics_view` in BigQuery
- Verified model names and token counts populate correctly
- Ran automated fix script to update all downstream views
- All 12 views + 3 tables recreated successfully

**Impact**:
- ✅ Looker Studio dashboard now shows actual model names (e.g., `gemini-2.5-flash-lite`)
- ✅ Token usage metrics populate correctly
- ✅ All visualizations display proper breakdowns
- ✅ Analytics views align with OpenTelemetry specification
- ✅ Future-proof: Matches official Gemini CLI telemetry structure

**Migration Path**:
For existing deployments, run the fix script:
```bash
python fix_views_and_tables.py --project PROJECT_ID --dataset DATASET_NAME
```

---

### Fixed - Authentication UI and Backend (2025-11-07)

#### Issue 1: OAuth Button Missing in Single-Project Mode
**Problem**: When users selected OAuth authentication with single-project setup, no authentication button appeared to create Application Default Credentials.

**Root Cause**: OAuth button was only shown when `!config.useSameProjectForGemini` (cross-project mode only), but OAuth requires ADC in BOTH single-project and cross-project modes.

**Fix**:
- Updated `ConfigForm.tsx` (line 399) to show OAuth button when `config.geminiAuthMethod === 'oauth'` regardless of project setup
- Added conditional messaging:
  - Single-project: "OAuth authentication requires Application Default Credentials (ADC) for Gemini CLI."
  - Cross-project: "Cross-project setup requires separate authentication for Gemini CLI project."
- Added conditional button text:
  - Single-project: "Create Application Default Credentials"
  - Cross-project: "Authenticate Gemini CLI Project"

#### Issue 2: Duplicate OAuth Buttons in Cross-Project Mode
**Problem**: When users selected OAuth authentication with cross-project setup, two identical "Authenticate Gemini CLI Project" buttons appeared.

**Root Cause**: Two buttons existed with overlapping conditions:
- Old button (lines 262-282): Showed when `!config.useSameProjectForGemini`
- New button (lines 420-444): Showed when `config.geminiAuthMethod === 'oauth' && !config.useSameProjectForGemini`

**Fix**:
- Removed old OAuth button from Gemini CLI Project ID section (deleted lines 262-282 in `ConfigForm.tsx`)
- Kept only the new button in authentication method section with improved UX

#### Backend: ADC Command Fix
**Problem**: Backend was using incorrect gcloud command flags for creating Application Default Credentials.

**Changes to `auth_service.py`**:
1. **Switched from user authentication to ADC**:
   - Changed from: `gcloud auth login`
   - Changed to: `gcloud auth application-default login`

2. **Fixed command flags**:
   - Removed: `--brief` (not supported by ADC command)
   - Added: `--quiet` (enables non-interactive execution)

3. **Updated function documentation**:
   - Clarified that function creates Application Default Credentials (ADC)
   - Documented ADC storage location: `~/.config/gcloud/application_default_credentials.json`

4. **Updated return value**:
   - Changed `method` from `"oauth"` to `"oauth_adc"`
   - Added `adc_created: True` flag
   - Added `adc_path` to response

**Files Modified**:
- `app/frontend/src/components/ConfigForm.tsx`: OAuth button conditional logic and messaging
- `app/backend/services/auth_service.py`: ADC creation command and documentation
- `app/README.md`: Already updated in previous session with authentication method documentation

**Impact**: Users can now successfully authenticate in all four scenarios:
- Single-project + OAuth ✅
- Single-project + Vertex AI ✅
- Cross-project + OAuth ✅
- Cross-project + Vertex AI ✅

---

## Previous Changes

See git commit history for previous changes before CHANGELOG was introduced.
