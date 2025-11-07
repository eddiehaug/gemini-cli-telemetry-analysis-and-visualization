# Changelog

All notable changes to the Gemini CLI Telemetry application will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

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
