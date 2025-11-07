// Deployment configuration
export interface DeploymentConfig {
  telemetryProjectId: string;         // GCP project for telemetry infrastructure (RENAMED)
  geminiCliProjectId: string;         // GCP project for Gemini CLI (RENAMED from inferenceProjectId)
  useSameProjectForGemini: boolean;   // Whether to use same project (RENAMED from useSameProjectForBoth)
  region: string;
  datasetName: string;
  logPrompts: boolean;
  pseudoanonymizePii: boolean;
  network: string;
  subnetwork: string;
  geminiAuthMethod: 'oauth' | 'vertex-ai';  // Gemini CLI authentication method
  geminiRegion: string;                      // Region for Gemini API calls (separate from deployment region)
}

// Deployment step status
export type StepStatus = 'pending' | 'in_progress' | 'completed' | 'failed';

// Deployment step
export interface DeploymentStep {
  id: string;
  name: string;
  description: string;
  status: StepStatus;
  error?: string;
  details?: string;
}

// API response types
export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// Dependency check result
export interface DependencyCheck {
  name: string;
  installed: boolean;
  version?: string;
  path?: string;
}

// Permission check result
export interface PermissionCheck {
  hasAll: boolean;
  missingRoles: string[];
  currentRoles: string[];
}

// API enablement result
export interface ApiEnablement {
  success: boolean;
  enabled: string[];
  failed: string[];
}

// Deployment state
export interface DeploymentState {
  deploymentId: string;
  status: 'idle' | 'deploying' | 'completed' | 'failed';
  currentStep: number;
  steps: DeploymentStep[];
  config: DeploymentConfig;
  createdResources: {
    dataset?: string;
    table?: string;
    sink?: string;
  };
}

// Authentication status result (NEW)
export interface AuthStatusResult {
  gcloud_installed: boolean;
  authenticated: boolean;
  account: string | null;
  has_adc: boolean;
}

// OAuth flow result (NEW)
export interface OAuthFlowResult {
  auth_url: string;
  message: string;
}

// Bootstrap result type
export interface BootstrapResult {
  dependencies: DependencyCheck[];
  account: string;  // Active gcloud account
  telemetry_config_name: string;
  telemetry_config_status: string;
  compute_api_enabled: boolean;
  apis_enabled: {
    enabled: string[];
    failed: string[];
  };
  networks_ok: boolean;
  network_count: number;
}

// Bootstrap step type
export interface BootstrapStep {
  id: string;
  name: string;
  description: string;
  status: StepStatus;
  details?: string;
  error?: string;
}
