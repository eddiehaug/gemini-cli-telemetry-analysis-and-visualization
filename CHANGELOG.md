# Changelog

All notable changes to the Gemini CLI Telemetry application will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

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
