import axios from 'axios';
import type {
  ApiResponse,
  DeploymentConfig,
  DeploymentState,
  DependencyCheck,
  PermissionCheck,
  ApiEnablement,
  BootstrapResult,
} from '../types';

const api = axios.create({
  baseURL: '/api',
  headers: {
    'Content-Type': 'application/json',
  },
});

// Deployment steps
export const deploymentApi = {
  // NEW: Bootstrap endpoint
  bootstrap: async (projectId: string): Promise<ApiResponse<BootstrapResult>> => {
    const response = await api.post('/bootstrap', { projectId });
    return response.data;
  },

  // NEW: Gemini CLI OAuth authentication
  authenticateGeminiOAuth: async (projectId: string): Promise<ApiResponse> => {
    const response = await api.post('/authenticate-gemini-oauth', { projectId });
    return response.data;
  },

  // Step 1: Verify dependencies
  verifyDependencies: async (): Promise<ApiResponse<DependencyCheck[]>> => {
    const response = await api.post('/verify-dependencies');
    return response.data;
  },

  // Step 2: Collect user input (validation only)
  validateConfig: async (config: DeploymentConfig): Promise<ApiResponse> => {
    const response = await api.post('/validate-config', config);
    return response.data;
  },

  // Step 3: Authenticate
  authenticate: async (): Promise<ApiResponse> => {
    const response = await api.post('/authenticate');
    return response.data;
  },

  // Step 4: Check permissions
  checkPermissions: async (projectId: string): Promise<ApiResponse<PermissionCheck>> => {
    const response = await api.post('/check-permissions', { projectId });
    return response.data;
  },

  // Step 5: Enable APIs
  enableApis: async (projectId: string): Promise<ApiResponse<ApiEnablement>> => {
    const response = await api.post('/enable-apis', { projectId });
    return response.data;
  },

  // Step 6: Configure telemetry
  configureTelemetry: async (
    logPrompts: boolean,
    geminiCliProjectId: string,
    telemetryProjectId: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/configure-telemetry', {
      logPrompts,
      geminiCliProjectId,
      telemetryProjectId
    });
    return response.data;
  },

  // Step 7: Create dataset & raw table
  createDataset: async (
    projectId: string,
    datasetName: string,
    region: string,
    skipTableCreation: boolean = false
  ): Promise<ApiResponse> => {
    const response = await api.post('/create-dataset', {
      projectId,
      datasetName,
      region,
      skipTableCreation
    });
    return response.data;
  },

  // Step 7b: Create analytics view
  createAnalyticsView: async (
    projectId: string,
    datasetName: string,
    pseudoanonymizePii: boolean
  ): Promise<ApiResponse> => {
    const response = await api.post('/create-analytics-view', {
      projectId,
      datasetName,
      pseudoanonymizePii
    });
    return response.data;
  },

  // Step 8: Create Pub/Sub resources (topic & subscription)
  createPubSub: async (
    projectId: string,
    sinkServiceAccount: string | null
  ): Promise<ApiResponse> => {
    const response = await api.post('/create-pubsub', {
      projectId,
      sinkServiceAccount
    });
    return response.data;
  },

  // Step 8b: Setup GCS bucket & UDF
  setupGCS: async (
    projectId: string,
    region: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/setup-gcs', {
      projectId,
      region
    });
    return response.data;
  },

  // Step 8c: Start Dataflow streaming pipeline
  startDataflow: async (
    projectId: string,
    datasetName: string,
    region: string,
    network?: string,
    subnetwork?: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/start-dataflow', {
      projectId,
      datasetName,
      region,
      network,
      subnetwork
    });
    return response.data;
  },

  // Step 9: Test Cloud Logging
  testLogging: async (projectId: string): Promise<ApiResponse> => {
    const response = await api.post('/test-logging', { projectId });
    return response.data;
  },

  // Step 10: Create log sink to Pub/Sub topic
  createSink: async (
    geminiCliProjectId: string,
    telemetryProjectId: string,
    topicName: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/create-sink', {
      geminiCliProjectId,
      telemetryProjectId,
      topicName
    });
    return response.data;
  },

  // Step 11: Verify ELT pipeline
  verifyELTPipeline: async (
    projectId: string,
    datasetName: string,
    region: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/verify-elt-pipeline', {
      projectId,
      datasetName,
      region
    });
    return response.data;
  },

  // Step 12: End-to-end verification
  verifyEndToEnd: async (
    projectId: string,
    datasetName: string,
    region: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/verify-end-to-end', {
      projectId,
      datasetName,
      region
    });
    return response.data;
  },

  // Step 13: Create all analytics views
  createAnalyticsViews: async (
    projectId: string,
    datasetName: string,
    pseudoanonymizePii: boolean
  ): Promise<ApiResponse> => {
    const response = await api.post('/create-analytics-views', {
      projectId,
      datasetName,
      pseudoanonymizePii
    });
    return response.data;
  },

  // Step 14: Verify analytics views
  verifyAnalyticsViews: async (
    projectId: string,
    datasetName: string
  ): Promise<ApiResponse> => {
    const response = await api.post('/verify-analytics-views', {
      projectId,
      datasetName
    });
    return response.data;
  },

  // Verify sink (used internally)
  verifySink: async (projectId: string, sinkName: string): Promise<ApiResponse> => {
    const response = await api.post('/verify-sink', { projectId, sinkName });
    return response.data;
  },

  // Get deployment status
  getStatus: async (deploymentId: string): Promise<ApiResponse<DeploymentState>> => {
    const response = await api.get(`/status/${deploymentId}`);
    return response.data;
  },

  // Full deployment
  startDeployment: async (config: DeploymentConfig): Promise<ApiResponse<{ deploymentId: string }>> => {
    const response = await api.post('/deploy', config);
    return response.data;
  },

  // Get networks and subnets for a project and region
  // MODIFIED: Explicitly named parameter for clarity - always telemetry project
  getNetworksAndSubnets: async (
    telemetryProjectId: string,  // Always telemetry project
    region: string
  ): Promise<ApiResponse<{ networks: Array<{ name: string; selfLink: string }>; subnets: Array<{ name: string; network: string; region: string; ipCidrRange: string }> }>> => {
    const response = await api.post('/get-networks-and-subnets', {
      projectId: telemetryProjectId,
      region
    });
    return response.data;
  },

  // Cleanup: Delete gcloud configuration
  cleanupConfiguration: async (configName: string): Promise<ApiResponse> => {
    const response = await api.post('/cleanup-configuration', { configName });
    return response.data;
  },
};

export default deploymentApi;
