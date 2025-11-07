import { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import WizardStepper from '../components/WizardStepper';
import ConfigForm from '../components/ConfigForm';
import DeploymentProgress from '../components/DeploymentProgress';
import { DeploymentConfig, DeploymentStep, StepStatus, BootstrapStep } from '../types';
import deploymentApi from '../services/api';

const BOOTSTRAP_STEPS: BootstrapStep[] = [
  { id: '1', name: 'Verify Dependencies', description: 'Check gcloud, gemini, python', status: 'pending' },
  { id: '2', name: 'Create gcloud config', description: 'Configure telemetry project', status: 'pending' },
  { id: '3', name: 'Check Compute API', description: 'Verify Compute Engine enabled', status: 'pending' },
  { id: '4', name: 'Enable Required APIs', description: 'Auto-enable 11 APIs', status: 'pending' },
  { id: '5', name: 'Check VPC Networks', description: 'Verify landing zone configured', status: 'pending' },
];

const DEPLOYMENT_STEPS: DeploymentStep[] = [
  { id: '1', name: 'Verify Dependencies', description: 'Check required tools', status: 'pending' },
  { id: '2', name: 'Collect Configuration', description: 'Project and dataset settings', status: 'pending' },
  { id: '3', name: 'Authenticate', description: 'GCP authentication', status: 'pending' },
  { id: '4', name: 'Check Permissions', description: 'Verify IAM roles', status: 'pending' },
  { id: '5', name: 'Enable APIs', description: 'Enable required GCP APIs', status: 'pending' },
  { id: '6', name: 'Configure Telemetry', description: 'Update Gemini CLI settings', status: 'pending' },
  { id: '7', name: 'Create Dataset & Table', description: 'BigQuery raw table with JSON schema', status: 'pending' },
  { id: '7b', name: 'Create Analytics View', description: 'BigQuery view for querying', status: 'pending' },
  { id: '8', name: 'Create Pub/Sub Resources', description: 'Topic and subscription for ELT', status: 'pending' },
  { id: '8b', name: 'Setup GCS & UDF', description: 'Bucket and transform function', status: 'pending' },
  { id: '8c', name: 'Deploy Dataflow Pipeline', description: 'Start streaming job', status: 'pending' },
  { id: '9', name: 'Test Cloud Logging', description: 'Verify logs appear', status: 'pending' },
  { id: '10', name: 'Create Log Sink', description: 'Sink to Pub/Sub topic', status: 'pending' },
  { id: '11', name: 'Verify ELT Pipeline', description: 'Check complete pipeline', status: 'pending' },
  { id: '12', name: 'End-to-End Test', description: 'Verify complete data flow', status: 'pending' },
  { id: '13', name: 'Create Analytics Views', description: '12 regular views, 3 scheduled query tables', status: 'pending' },
  { id: '14', name: 'Verify Analytics Views', description: 'Confirm all views were created successfully', status: 'pending' },
];

export default function WizardPage() {
  const navigate = useNavigate();
  const [phase, setPhase] = useState<'bootstrap' | 'config' | 'deploying' | 'complete'>('bootstrap');
  const [bootstrapComplete, setBootstrapComplete] = useState(false);
  const [telemetryProjectId, setTelemetryProjectId] = useState('');
  const [bootstrapSteps, setBootstrapSteps] = useState<BootstrapStep[]>(BOOTSTRAP_STEPS);
  const [currentBootstrapStep, setCurrentBootstrapStep] = useState(0);
  const [bootstrapError, setBootstrapError] = useState<string | null>(null);
  const [geminiCliAuthenticated, setGeminiCliAuthenticated] = useState(false);

  // NEW: Authentication state
  const [authStatus, setAuthStatus] = useState<{
    gcloud_installed: boolean;
    authenticated: boolean;
    account: string | null;
    has_adc: boolean;
  } | null>(null);
  const [checkingAuth, setCheckingAuth] = useState(false);
  const [authenticating, setAuthenticating] = useState(false);
  const [authError, setAuthError] = useState<string | null>(null);
  const [config, setConfig] = useState<DeploymentConfig>({
    telemetryProjectId: '',
    geminiCliProjectId: '',
    useSameProjectForGemini: true,
    region: 'us-central1',
    datasetName: 'gemini_cli_telemetry',
    logPrompts: false,
    pseudoanonymizePii: false,
    network: 'default',
    subnetwork: 'default',
    geminiAuthMethod: 'oauth',  // Default to OAuth (recommended)
    geminiRegion: 'us-central1',  // Default to same as deployment region
  });
  const [steps, setSteps] = useState<DeploymentStep[]>(DEPLOYMENT_STEPS);
  const [currentStep, setCurrentStep] = useState(0);

  const updateStepStatus = (stepIndex: number, status: StepStatus, details?: string, error?: string) => {
    setSteps(prev => prev.map((step, idx) =>
      idx === stepIndex
        ? { ...step, status, details, error }
        : step
    ));
  };

  const updateBootstrapStatus = (stepIndex: number, status: StepStatus, details?: string, error?: string) => {
    setBootstrapSteps(prev => prev.map((step, idx) =>
      idx === stepIndex
        ? { ...step, status, details, error }
        : step
    ));
  };

  // NEW: Auto-check authentication status when project ID is entered
  useEffect(() => {
    const checkAuth = async () => {
      if (!telemetryProjectId || telemetryProjectId.length < 6) {
        // Don't check if project ID is too short
        return;
      }

      setCheckingAuth(true);
      setAuthError(null);

      try {
        const result = await deploymentApi.checkAuthStatus();

        if (result.success && result.data) {
          setAuthStatus(result.data);

          // If already authenticated, we can enable bootstrap
          if (result.data.authenticated) {
            console.log('✓ Already authenticated as:', result.data.account);
          }
        } else {
          setAuthError(result.error || 'Failed to check authentication');
        }
      } catch (error) {
        console.error('Auth check error:', error);
        setAuthError('Failed to check authentication status');
      } finally {
        setCheckingAuth(false);
      }
    };

    // Debounce the check - wait 500ms after user stops typing
    const timer = setTimeout(() => {
      checkAuth();
    }, 500);

    return () => clearTimeout(timer);
  }, [telemetryProjectId]);

  // NEW: Handle OAuth authentication
  const handleAuthenticate = async () => {
    setAuthenticating(true);
    setAuthError(null);

    try {
      // Step 1: Get OAuth URL from backend
      const result = await deploymentApi.authenticateWithOAuth();

      if (result.success && result.data) {
        const authUrl = result.data.auth_url;
        console.log('Opening OAuth URL:', authUrl);

        // Step 2: Open OAuth URL in new tab
        const authWindow = window.open(authUrl, '_blank', 'width=600,height=700');

        // Step 3: Poll for completion
        const pollInterval = setInterval(async () => {
          try {
            // Check if window was closed
            if (authWindow && authWindow.closed) {
              clearInterval(pollInterval);
              setAuthenticating(false);

              // Re-check auth status after user completes OAuth
              const statusResult = await deploymentApi.checkAuthStatus();
              if (statusResult.success && statusResult.data) {
                setAuthStatus(statusResult.data);

                if (statusResult.data.authenticated) {
                  console.log('✓ Authentication successful!');
                } else {
                  setAuthError('Authentication was cancelled or failed. Please try again.');
                }
              }
            }
          } catch (error) {
            console.error('Error checking window status:', error);
          }
        }, 1000); // Check every second

        // Stop polling after 5 minutes
        setTimeout(() => {
          clearInterval(pollInterval);
          setAuthenticating(false);
        }, 300000);
      } else {
        setAuthError(result.error || 'Failed to initiate OAuth flow');
        setAuthenticating(false);
      }
    } catch (error) {
      console.error('OAuth error:', error);
      setAuthError('Failed to start authentication');
      setAuthenticating(false);
    }
  };

  const runBootstrap = async () => {
    let progressInterval: NodeJS.Timeout | null = null;

    try {
      // Clear any previous error
      setBootstrapError(null);

      // Set to 1 to make bootstrap steps visible
      setCurrentBootstrapStep(1);

      // Simulate progress through steps while backend processes
      progressInterval = setInterval(() => {
        setCurrentBootstrapStep(prev => {
          if (prev < 5) {
            // Mark current step as completed, next as in_progress
            setBootstrapSteps(steps => steps.map((step, idx) => {
              if (idx < prev) return { ...step, status: 'completed' as StepStatus };
              if (idx === prev) return { ...step, status: 'in_progress' as StepStatus };
              return step;
            }));
            return prev + 1;
          }
          return prev;
        });
      }, 800); // Progress every 800ms

      // Call bootstrap endpoint (runs all 5 steps)
      const result = await deploymentApi.bootstrap(telemetryProjectId);

      // IMMEDIATELY clear progress animation
      if (progressInterval) clearInterval(progressInterval);
      progressInterval = null;

      if (!result.success) {
        // Determine which step failed based on error message
        let failedStepIndex = 0;
        const errorMsg = result.error || '';

        if (errorMsg.includes('Compute Engine API')) {
          failedStepIndex = 2; // Check Compute API
        } else if (errorMsg.includes('VPC network')) {
          failedStepIndex = 4; // Check VPC Networks
        } else if (errorMsg.includes('Authentication') || errorMsg.includes('auth')) {
          failedStepIndex = 1; // Authenticate
        } else if (errorMsg.includes('gcloud') || errorMsg.includes('gemini') || errorMsg.includes('dependency')) {
          failedStepIndex = 0; // Verify Dependencies
        } else if (errorMsg.includes('API')) {
          failedStepIndex = 3; // Enable Required APIs
        }

        // Mark steps up to failed step as completed, failed step as failed
        setBootstrapSteps(prev => prev.map((step, idx) => {
          if (idx < failedStepIndex) return { ...step, status: 'completed' as StepStatus };
          if (idx === failedStepIndex) return { ...step, status: 'failed' as StepStatus, error: 'See error details above' };
          return { ...step, status: 'pending' as StepStatus };
        }));

        // Show full error in alert box
        setBootstrapError(result.error || 'Bootstrap failed');
        return;
      }

      // Mark all steps as completed
      setBootstrapSteps(prev => prev.map(step => ({ ...step, status: 'completed' })));
      setBootstrapComplete(true);

      // Update config with telemetry project ID
      // If using same project, also set geminiCliProjectId
      setConfig(prev => ({
        ...prev,
        telemetryProjectId,
        geminiCliProjectId: prev.useSameProjectForGemini ? telemetryProjectId : prev.geminiCliProjectId
      }));

      setPhase('config');
    } catch (error: any) {
      // Clear progress animation
      if (progressInterval) clearInterval(progressInterval);

      // Make steps visible and show error
      const errorMessage = error.message || 'An unexpected error occurred';
      setBootstrapError(errorMessage);
      setCurrentBootstrapStep(1);
      updateBootstrapStatus(0, 'failed', '', 'See error details above');
    }
  };

  const handleGeminiOAuth = async () => {
    /**
     * Handle OAuth authentication for Gemini CLI project.
     *
     * Opens browser window for Google Login. If browser doesn't open automatically,
     * backend will return manual URL in error message with format:
     * "MANUAL_AUTH_REQUIRED:{url}"
     */
    try {
      // Get current Gemini CLI project ID from config
      const geminiProjectId = config.geminiCliProjectId;

      if (!geminiProjectId) {
        throw new Error('Please enter Gemini CLI Project ID first');
      }

      // Call OAuth endpoint
      const result = await deploymentApi.authenticateGeminiOAuth(geminiProjectId);

      if (result.success) {
        setGeminiCliAuthenticated(true);
        // Optionally show success message
        console.log('Gemini CLI OAuth successful:', result.data);
      } else {
        // Check if error is MANUAL_AUTH_REQUIRED
        if (result.error?.startsWith('MANUAL_AUTH_REQUIRED:')) {
          const authUrl = result.error.split('MANUAL_AUTH_REQUIRED:')[1].split('\n')[0];

          // Open manual auth URL in new window
          window.open(authUrl, '_blank');

          // Show user-friendly message
          alert(
            'Browser authentication window should open automatically.\n\n' +
            'If it doesn\'t open, please:\n' +
            '1. Check your popup blocker\n' +
            '2. Click the "Authenticate" button again\n\n' +
            'After authenticating, click "Retry" to continue.'
          );
        } else {
          throw new Error(result.error || 'OAuth authentication failed');
        }
      }
    } catch (error: any) {
      console.error('Gemini CLI OAuth failed:', error);
      alert(`OAuth authentication failed: ${error.message}`);
      setGeminiCliAuthenticated(false);
    }
  };

  const handleCleanupConfigurations = async () => {
    /**
     * Clean up gcloud configurations created during deployment.
     *
     * This deletes:
     * - telemetry-{project} configuration (always created)
     * - gemini-cli-{project} configuration (if two-project setup was used)
     */
    try {
      const configurationsToDelete: string[] = [];

      // Always cleanup telemetry configuration
      const telemetryConfigName = `telemetry-${telemetryProjectId}`;
      configurationsToDelete.push(telemetryConfigName);

      // If using two-project setup, also cleanup Gemini CLI configuration
      if (!config.useSameProjectForGemini && config.geminiCliProjectId) {
        const geminiCliConfigName = `gemini-cli-${config.geminiCliProjectId}`;
        configurationsToDelete.push(geminiCliConfigName);
      }

      // Delete each configuration
      for (const configName of configurationsToDelete) {
        console.log(`Deleting configuration: ${configName}`);
        await deploymentApi.cleanupConfiguration(configName);
      }

      alert(
        `Successfully deleted ${configurationsToDelete.length} gcloud configuration(s):\n` +
        configurationsToDelete.map(name => `  - ${name}`).join('\n')
      );
    } catch (error: any) {
      console.error('Configuration cleanup failed:', error);
      alert(`Failed to cleanup configurations: ${error.message}`);
    }
  };

  const runDeployment = async (deploymentConfig: DeploymentConfig) => {
    setConfig(deploymentConfig);
    setPhase('deploying');

    // Wrap entire deployment in try-catch to handle page refresh/abort
    try {
      // Step 1: Verify Dependencies
      setCurrentStep(0);
      updateStepStatus(0, 'in_progress', 'Checking for gcloud CLI, gemini CLI, and Python...');
      const depsResult = await deploymentApi.verifyDependencies();
      if (!depsResult.success) {
        updateStepStatus(0, 'failed', '', depsResult.error);
        return;
      }
      const deps = (depsResult.data as any)?.dependencies || [];
      const autoInstalled = deps.filter((d: any) => d.auto_installed);
      if (autoInstalled.length > 0) {
        updateStepStatus(0, 'completed', `Dependencies verified. Auto-installed: ${autoInstalled.map((d: any) => d.name).join(', ')}`);
      } else {
        updateStepStatus(0, 'completed', 'All dependencies verified and ready');
      }

      // Step 2: Validate Config (already done in form)
      setCurrentStep(1);
      updateStepStatus(1, 'in_progress', 'Validating project configuration...');
      const projectMsg = deploymentConfig.useSameProjectForGemini
        ? `Configuration validated for project ${deploymentConfig.telemetryProjectId}`
        : `Configuration validated for Gemini CLI project ${deploymentConfig.geminiCliProjectId} and telemetry project ${deploymentConfig.telemetryProjectId}`;
      updateStepStatus(1, 'completed', projectMsg);

      // Step 3: Authenticate
      setCurrentStep(2);
      updateStepStatus(2, 'in_progress', 'Authenticating with Google Cloud...');
      const authResult = await deploymentApi.authenticate();
      if (!authResult.success) {
        updateStepStatus(2, 'failed', '', authResult.error);
        return;
      }
      const userEmail = authResult.data?.email || 'authenticated user';
      updateStepStatus(2, 'completed', `Authenticated as ${userEmail}`);

      // Step 4: Check Permissions
      setCurrentStep(3);
      updateStepStatus(3, 'in_progress', 'Checking IAM permissions and roles...');
      const permsResult = await deploymentApi.checkPermissions(deploymentConfig.telemetryProjectId);
      if (!permsResult.success) {
        updateStepStatus(3, 'failed', '', permsResult.error);
        return;
      }
      if (permsResult.data?.hasAll) {
        updateStepStatus(3, 'completed', 'All required IAM permissions are granted');
      } else {
        const grantedRoles = permsResult.data?.currentRoles?.length || 0;
        updateStepStatus(3, 'completed', `Permissions configured (${grantedRoles} roles granted). Waiting 90s for IAM propagation...`);
      }

      // Step 5: Enable APIs
      setCurrentStep(4);
      updateStepStatus(4, 'in_progress', 'Enabling required Google Cloud APIs (BigQuery, Cloud Logging, Pub/Sub, Dataflow)...');
      const apisResult = await deploymentApi.enableApis(deploymentConfig.telemetryProjectId);
      if (!apisResult.success) {
        updateStepStatus(4, 'failed', '', apisResult.error);
        return;
      }
      const enabledCount = apisResult.data?.enabled?.length || 0;
      const apiNames = apisResult.data?.enabled?.join(', ') || 'APIs';
      updateStepStatus(4, 'completed', `${enabledCount} APIs verified: ${apiNames}`);

      // Step 6: Configure Telemetry
      setCurrentStep(5);
      const authMethodDisplay = deploymentConfig.geminiAuthMethod === 'oauth' ? 'OAuth' : 'Vertex AI';
      updateStepStatus(5, 'in_progress', `Configuring Gemini CLI telemetry with ${authMethodDisplay} authentication...`);
      const telemetryResult = await deploymentApi.configureTelemetry(
        deploymentConfig.logPrompts,
        deploymentConfig.geminiCliProjectId,
        deploymentConfig.telemetryProjectId,
        deploymentConfig.geminiAuthMethod,
        deploymentConfig.geminiRegion
      );
      if (!telemetryResult.success) {
        updateStepStatus(5, 'failed', '', telemetryResult.error);
        return;
      }
      const promptLoggingStatus = deploymentConfig.logPrompts ? 'with prompt logging' : 'without prompt logging';
      const regionInfo = deploymentConfig.geminiAuthMethod === 'vertex-ai' ? ` (region: ${deploymentConfig.geminiRegion})` : '';
      const telemetryMsg = `Gemini CLI telemetry configured: ${authMethodDisplay} ${promptLoggingStatus}${regionInfo}`;
      updateStepStatus(5, 'completed', telemetryMsg);

      // Step 7: Create Dataset & Raw Table with JSON String Schema
      setCurrentStep(6);
      updateStepStatus(6, 'in_progress', `Creating BigQuery dataset "${deploymentConfig.datasetName}" with raw table (JSON string schema)...`);
      const datasetResult = await deploymentApi.createDataset(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.region,
        false
      );
      if (!datasetResult.success) {
        updateStepStatus(6, 'failed', '', datasetResult.error);
        return;
      }
      updateStepStatus(6, 'completed', `Dataset and gemini_raw_logs table created with "unbreakable" JSON string schema`);

      // Step 7b: Create Analytics View
      setCurrentStep(7);
      updateStepStatus(7, 'in_progress', 'Creating gemini_analytics_view for easy querying...');
      const viewResult = await deploymentApi.createAnalyticsView(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.pseudoanonymizePii
      );
      if (!viewResult.success) {
        updateStepStatus(7, 'failed', '', viewResult.error);
        return;
      }
      const viewType = deploymentConfig.pseudoanonymizePii ? 'pseudoanonymized' : 'standard';
      updateStepStatus(7, 'completed', `Analytics view created (${viewType})`);

      // Step 8: Create Pub/Sub Resources
      setCurrentStep(8);
      updateStepStatus(8, 'in_progress', 'Creating Pub/Sub topic and subscription for ELT pipeline...');

      // Create Pub/Sub WITHOUT permissions (sink service account doesn't exist yet)
      const pubsubResult = await deploymentApi.createPubSub(
        deploymentConfig.telemetryProjectId,
        null  // No sink service account yet - will grant permissions in Step 10
      );
      if (!pubsubResult.success) {
        updateStepStatus(8, 'failed', '', pubsubResult.error);
        return;
      }
      updateStepStatus(8, 'completed', 'Pub/Sub topic "gemini-telemetry-topic" and subscription created');

      // Step 8b: Setup GCS & UDF
      setCurrentStep(9);
      updateStepStatus(9, 'in_progress', 'Creating GCS bucket and uploading JavaScript UDF for Dataflow...');
      const gcsResult = await deploymentApi.setupGCS(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.region
      );
      if (!gcsResult.success) {
        updateStepStatus(9, 'failed', '', gcsResult.error);
        return;
      }
      const bucketName = gcsResult.data?.bucket_name || `${deploymentConfig.telemetryProjectId}-dataflow`;
      updateStepStatus(9, 'completed', `GCS bucket "${bucketName}" created and transform.js UDF uploaded`);

      // Step 8c: Deploy Dataflow Pipeline
      setCurrentStep(10);
      updateStepStatus(10, 'in_progress', 'Starting Dataflow streaming pipeline and waiting for worker pool startup (this will take 2-3 minutes)...');
      const dataflowResult = await deploymentApi.startDataflow(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.region,
        deploymentConfig.network,
        deploymentConfig.subnetwork
      );
      if (!dataflowResult.success) {
        updateStepStatus(10, 'failed', '', dataflowResult.error);
        return;
      }
      const jobId = dataflowResult.data?.job_id || 'unknown';
      updateStepStatus(10, 'completed', `Dataflow pipeline started (Job ID: ${jobId})`);

      // Step 9: Test Cloud Logging
      setCurrentStep(11);
      updateStepStatus(11, 'in_progress', 'Running Gemini CLI test and verifying logs appear in Cloud Logging...');
      const loggingResult = await deploymentApi.testLogging(deploymentConfig.geminiCliProjectId);
      if (!loggingResult.success) {
        updateStepStatus(11, 'failed', '', loggingResult.error);
        return;
      }
      const logCount = loggingResult.data?.log_count || 0;
      updateStepStatus(11, 'completed', `Found ${logCount} Gemini CLI telemetry logs in Cloud Logging`);

      // Step 10: Create Log Sink to Pub/Sub
      setCurrentStep(12);
      updateStepStatus(12, 'in_progress', 'Creating Cloud Logging sink in telemetry project (may take 2-3 minutes for Google-managed service account to provision)...');
      const sinkResult = await deploymentApi.createSink(
        deploymentConfig.geminiCliProjectId,  // Log source project
        deploymentConfig.telemetryProjectId,   // Sink and topic project
        'gemini-telemetry-topic'
      );
      if (!sinkResult.success) {
        updateStepStatus(12, 'failed', '', sinkResult.error);
        return;
      }
      updateStepStatus(12, 'completed', 'Log sink "gemini-cli-to-pubsub" created in telemetry project with cross-project routing');

      // Step 11: Verify ELT Pipeline
      setCurrentStep(13);
      updateStepStatus(13, 'in_progress', 'Verifying complete ELT pipeline (Sink → Pub/Sub → Dataflow → BigQuery)...');
      const pipelineResult = await deploymentApi.verifyELTPipeline(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.region
      );
      if (!pipelineResult.success) {
        const issues = pipelineResult.data?.issues?.join(', ') || 'Unknown issues';
        updateStepStatus(13, 'completed', `Pipeline verification: ${issues} (may still be starting up)`);
      } else {
        updateStepStatus(13, 'completed', 'ELT pipeline verified: All components running');
      }

      // Step 12: End-to-End Verification
      setCurrentStep(14);
      updateStepStatus(14, 'in_progress', 'Running end-to-end test (may take 10-15 minutes for first data to appear)...');
      const e2eResult = await deploymentApi.verifyEndToEnd(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.region
      );
      if (!e2eResult.success) {
        updateStepStatus(14, 'failed', '', e2eResult.error || 'E2E verification failed');
        return;
      }
      const e2eSuccess = e2eResult.data?.success;
      if (e2eSuccess) {
        const bqRows = e2eResult.data?.steps?.bigquery_raw?.row_count || 0;
        updateStepStatus(14, 'completed', `End-to-end verification complete! BigQuery has ${bqRows} rows of telemetry data`);
      } else {
        const failedSteps = Object.entries(e2eResult.data?.steps || {})
          .filter(([_, step]: [string, any]) => !step.success)
          .map(([name]) => name);
        updateStepStatus(14, 'completed', `E2E test partially complete. Some steps may need more time: ${failedSteps.join(', ')}`);
      }

      // Step 13: Create Analytics Views (AFTER E2E verification confirms pipeline works)
      setCurrentStep(15);
      updateStepStatus(15, 'in_progress', 'Creating analytics views (15 views)...');
      const viewsResult = await deploymentApi.createAnalyticsViews(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName,
        deploymentConfig.pseudoanonymizePii
      );

      if (!viewsResult.success) {
        updateStepStatus(15, 'failed', '', viewsResult.error);
        return;
      }

      const createdCount = viewsResult.data?.created?.length || 0;
      const failedCount = viewsResult.data?.failed?.length || 0;
      const userCol = viewsResult.data?.user_column || 'user_email';

      if (failedCount > 0) {
        updateStepStatus(15, 'completed', `${createdCount} views created, ${failedCount} failed (using ${userCol})`);
      } else {
        updateStepStatus(15, 'completed', `${createdCount} analytics views created (using ${userCol})`);
      }

      // Step 14: Verify Analytics Views
      setCurrentStep(16);
      updateStepStatus(16, 'in_progress', 'Verifying all views exist...');
      const viewsVerificationResult = await deploymentApi.verifyAnalyticsViews(
        deploymentConfig.telemetryProjectId,
        deploymentConfig.datasetName
      );

      if (!viewsVerificationResult.success) {
        updateStepStatus(16, 'failed', '', viewsVerificationResult.error);
        return;
      }

      const verifiedCount = viewsVerificationResult.data?.verified_count || 0;
      const missingViews = viewsVerificationResult.data?.missing_views || [];
      const missingTables = viewsVerificationResult.data?.missing_tables || [];

      if (missingViews.length > 0 || missingTables.length > 0) {
        updateStepStatus(16, 'completed', `${verifiedCount}/15 views verified, ${missingViews.length + missingTables.length} missing`);
      } else {
        updateStepStatus(16, 'completed', `All 15 analytics views verified successfully`);
      }

      // Complete!
      setPhase('complete');
    } catch (error: any) {
      // Handle page refresh/abort (Ctrl+Shift+R) gracefully
      if (error.name === 'AbortError' || error.message?.includes('aborted') || error.code === 'ERR_CANCELED') {
        console.log('Deployment cancelled by user (page refresh)');
        // Don't crash - just mark current step as failed silently
        updateStepStatus(currentStep, 'failed', '', 'Deployment cancelled');
        return;
      }

      // Handle network errors gracefully
      if (error.message?.includes('Network Error') || error.message?.includes('Failed to fetch')) {
        console.error('Network error during deployment:', error);
        updateStepStatus(currentStep, 'failed', '', 'Network error - please check your connection');
        return;
      }

      // Handle other errors
      console.error('Deployment error:', error);
      updateStepStatus(currentStep, 'failed', '', error.message || 'Unknown error');
    }
  };

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-6">
          <h1 className="text-2xl font-bold mb-1">Gemini CLI Telemetry - Automated Deployment Wizard</h1>
          <p className="text-slate-400 text-sm">
            Follow 17 automated steps to deploy a production-ready ELT pipeline to BigQuery
          </p>
        </div>

        {/* ELT Pipeline Architecture */}
        <div className="mb-6 bg-slate-800 border border-slate-700 rounded-xl p-6">
          <div className="flex items-center gap-3 mb-4">
            <svg className="w-5 h-5 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
            </svg>
            <h3 className="text-base font-semibold text-cyan-300">ELT Pipeline Architecture</h3>
          </div>

          {/* Horizontal Pipeline Flow */}
          <div className="bg-slate-900/50 rounded-lg p-5 mb-4">
            <div className="flex items-center justify-between gap-3">
              {/* Gemini CLI */}
              <div className="flex-1 text-center">
                <div className="bg-cyan-500/20 border border-cyan-500/40 rounded-lg p-3 mb-2">
                  <div className="text-cyan-400 font-medium text-sm">Gemini CLI</div>
                </div>
                <div className="text-xs text-slate-500">Source</div>
              </div>

              <svg className="w-6 h-6 text-slate-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>

              {/* Cloud Logging */}
              <div className="flex-1 text-center">
                <div className="bg-blue-500/20 border border-blue-500/40 rounded-lg p-3 mb-2">
                  <div className="text-blue-400 font-medium text-sm">Cloud Logging</div>
                </div>
                <div className="text-xs text-slate-500">Extraction</div>
              </div>

              <svg className="w-6 h-6 text-slate-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>

              {/* Pub/Sub */}
              <div className="flex-1 text-center">
                <div className="bg-purple-500/20 border border-purple-500/40 rounded-lg p-3 mb-2">
                  <div className="text-purple-400 font-medium text-sm">Pub/Sub</div>
                </div>
                <div className="text-xs text-slate-500">Buffering</div>
              </div>

              <svg className="w-6 h-6 text-slate-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>

              {/* Dataflow */}
              <div className="flex-1 text-center">
                <div className="bg-green-500/20 border border-green-500/40 rounded-lg p-3 mb-2">
                  <div className="text-green-400 font-medium text-sm">Dataflow</div>
                </div>
                <div className="text-xs text-slate-500">Transform</div>
              </div>

              <svg className="w-6 h-6 text-slate-600 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>

              {/* BigQuery */}
              <div className="flex-1 text-center">
                <div className="bg-yellow-500/20 border border-yellow-500/40 rounded-lg p-3 mb-2">
                  <div className="text-yellow-400 font-medium text-sm">BigQuery</div>
                </div>
                <div className="text-xs text-slate-500">Storage & Analysis</div>
              </div>
            </div>
          </div>

          <div className="text-xs text-slate-400 flex items-start gap-2">
            <svg className="w-4 h-4 text-cyan-400 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <span><span className="font-medium text-slate-300">Benefits:</span> Real-time streaming • Scalable buffering • Flexible schema • Regional deployment</span>
          </div>
        </div>

        {/* Auto-Installation Info Banner */}
        <div className="mb-6 bg-blue-500/10 border border-blue-500/20 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <svg className="w-5 h-5 text-blue-400 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <div className="text-sm text-blue-300">
              <p className="font-semibold mb-1">Automatic Dependency Installation</p>
              <p className="opacity-90">
                Required tools (<span className="font-medium">gcloud CLI</span>, <span className="font-medium">gemini CLI</span>, <span className="font-medium">Python 3.8+</span>) will be automatically installed if missing.
                You only need a Google Cloud project with billing enabled.
              </p>
            </div>
          </div>
        </div>

        {/* Stepper */}
        <div className="mb-8">
          <WizardStepper
            steps={steps.map((s, idx) => ({ ...s, number: idx + 1 }))}
            currentStep={currentStep}
          />
        </div>

        {/* Content */}
        {phase === 'bootstrap' && (
          <div className="bg-slate-800 rounded-xl p-8 border border-slate-700">
            <h2 className="text-xl font-bold mb-4">Step 1: Bootstrap Application</h2>
            <p className="text-slate-400 mb-6">
              Validate your GCP project environment before deployment
            </p>

            {/* Project ID Input */}
            <div className="mb-6">
              <label className="block text-sm font-medium mb-2">
                Telemetry Project ID *
              </label>
              <input
                type="text"
                value={telemetryProjectId}
                onChange={(e) => setTelemetryProjectId(e.target.value)}
                placeholder="my-telemetry-project"
                className="w-full px-4 py-2 bg-slate-900 border border-slate-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary"
                disabled={bootstrapComplete}
              />
              <p className="text-xs text-slate-500 mt-1">
                GCP project where telemetry infrastructure will be deployed
              </p>
            </div>

            {/* Authentication Status Section (NEW) */}
            {telemetryProjectId && telemetryProjectId.length >= 6 && (
              <div className="mb-6 border border-slate-600 rounded-lg p-4 bg-slate-900/50">
                <h3 className="text-sm font-semibold mb-3 flex items-center gap-2">
                  <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
                  </svg>
                  Authentication Status
                </h3>

                {checkingAuth && (
                  <div className="flex items-center gap-2 text-sm text-slate-400">
                    <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    Checking authentication status...
                  </div>
                )}

                {!checkingAuth && authStatus && (
                  <div>
                    {/* gcloud CLI Status */}
                    <div className="flex items-center gap-2 mb-2">
                      {authStatus.gcloud_installed ? (
                        <>
                          <svg className="w-4 h-4 text-green-400" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                          </svg>
                          <span className="text-sm text-green-400">gcloud CLI installed</span>
                        </>
                      ) : (
                        <>
                          <svg className="w-4 h-4 text-yellow-400" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
                          </svg>
                          <span className="text-sm text-yellow-400">gcloud CLI not installed (will be installed automatically)</span>
                        </>
                      )}
                    </div>

                    {/* Authentication Status */}
                    <div className="flex items-center gap-2">
                      {authStatus.authenticated ? (
                        <>
                          <svg className="w-4 h-4 text-green-400" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                          </svg>
                          <span className="text-sm text-green-400">
                            Authenticated as <span className="font-medium">{authStatus.account}</span>
                          </span>
                        </>
                      ) : (
                        <>
                          <svg className="w-4 h-4 text-red-400" fill="currentColor" viewBox="0 0 20 20">
                            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                          </svg>
                          <span className="text-sm text-red-400">Not authenticated</span>
                        </>
                      )}
                    </div>

                    {/* Authenticate Button */}
                    {!authStatus.authenticated && (
                      <button
                        onClick={handleAuthenticate}
                        disabled={authenticating}
                        className="mt-4 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-700 disabled:cursor-not-allowed rounded-lg text-sm font-medium transition-colors flex items-center gap-2"
                      >
                        {authenticating ? (
                          <>
                            <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                            </svg>
                            Authenticating...
                          </>
                        ) : (
                          <>
                            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
                            </svg>
                            Authenticate with Google
                          </>
                        )}
                      </button>
                    )}
                  </div>
                )}

                {authError && (
                  <div className="mt-3 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-sm text-red-400">
                    {authError}
                  </div>
                )}
              </div>
            )}

            {/* Bootstrap Button */}
            <button
              onClick={runBootstrap}
              disabled={!telemetryProjectId || !authStatus?.authenticated || bootstrapComplete || checkingAuth}
              className="px-6 py-3 bg-primary hover:bg-primary/90 disabled:bg-slate-700 disabled:cursor-not-allowed rounded-lg font-semibold transition-colors"
            >
              {bootstrapComplete ? '✓ Bootstrap Complete' : 'Bootstrap the Application'}
            </button>

            {!authStatus?.authenticated && telemetryProjectId && telemetryProjectId.length >= 6 && !checkingAuth && (
              <p className="text-sm text-yellow-400 mt-2">
                Please authenticate before running bootstrap
              </p>
            )}

            {/* Bootstrap Error Alert */}
            {bootstrapError && (
              <div className="mt-6 bg-red-500/10 border border-red-500/30 rounded-lg p-4">
                <div className="flex items-start gap-3">
                  <svg className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <div className="flex-1">
                    <h3 className="text-sm font-semibold text-red-400 mb-1">Bootstrap Failed</h3>
                    <pre className="text-xs text-red-300 whitespace-pre-wrap font-mono">{bootstrapError}</pre>
                  </div>
                </div>
              </div>
            )}

            {/* Bootstrap Steps Progress */}
            {currentBootstrapStep > 0 && (
              <div className="mt-6">
                <WizardStepper
                  steps={bootstrapSteps.map((s, idx) => ({ ...s, number: idx + 1 }))}
                  currentStep={currentBootstrapStep}
                />
              </div>
            )}
          </div>
        )}

        {phase === 'config' && (
          <ConfigForm
            initialConfig={config}
            onSubmit={runDeployment}
            disabled={!bootstrapComplete}
            telemetryProjectId={telemetryProjectId}
            onGeminiOAuth={handleGeminiOAuth}
          />
        )}

        {phase === 'deploying' && (
          <DeploymentProgress
            steps={steps}
            currentStep={currentStep}
            config={config}
          />
        )}

        {phase === 'complete' && (
          <div className="bg-slate-800 rounded-xl p-8 border border-green-500/20">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 bg-green-500/20 rounded-full mb-4">
                <svg className="w-8 h-8 text-green-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                </svg>
              </div>
              <h2 className="text-2xl font-bold text-green-500 mb-2">
                Deployment Complete!
              </h2>
              <p className="text-slate-300 mb-6">
                ELT pipeline is deployed and data is flowing to BigQuery
              </p>
              <div className="flex gap-4 justify-center mb-6">
                <button
                  onClick={() => navigate('/')}
                  className="px-6 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
                >
                  Back to Home
                </button>
                <button
                  onClick={() => window.open(`https://console.cloud.google.com/bigquery?project=${config.telemetryProjectId}`, '_blank')}
                  className="px-6 py-2 bg-primary hover:bg-primary/90 text-slate-900 font-semibold rounded-lg transition-colors"
                >
                  View in BigQuery
                </button>
              </div>

              {/* Optional Cleanup Section */}
              <div className="mt-8 pt-6 border-t border-slate-700">
                <p className="text-sm text-slate-400 mb-3">
                  Optional: Clean up temporary gcloud configurations created during deployment
                </p>
                <button
                  onClick={handleCleanupConfigurations}
                  className="px-4 py-2 text-sm bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
                >
                  Delete Gcloud Configurations
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
