import { useState, useEffect } from 'react';
import { DeploymentConfig } from '../types';
import deploymentApi from '../services/api';

interface ConfigFormProps {
  initialConfig: DeploymentConfig;
  onSubmit: (config: DeploymentConfig) => void;
  disabled?: boolean;  // NEW - form disabled until bootstrap complete
  telemetryProjectId: string;  // NEW - passed from parent (read-only)
  onGeminiOAuth?: () => Promise<void>;  // NEW - OAuth callback
}

interface Network {
  name: string;
  selfLink: string;
}

interface Subnet {
  name: string;
  network: string;
  region: string;
  ipCidrRange: string;
}

export default function ConfigForm({ initialConfig, onSubmit, disabled = false, telemetryProjectId, onGeminiOAuth }: ConfigFormProps) {
  const [config, setConfig] = useState<DeploymentConfig>(initialConfig);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [networks, setNetworks] = useState<Network[]>([]);
  const [subnets, setSubnets] = useState<Subnet[]>([]);
  const [loadingNetworks, setLoadingNetworks] = useState(false);
  const [networkError, setNetworkError] = useState<string>('');
  const [geminiAuthStatus, setGeminiAuthStatus] = useState<string | null>(null);

  // Fetch networks from TELEMETRY project (not inference project)
  useEffect(() => {
    const fetchNetworksAndSubnets = async () => {
      // Only fetch if enabled, telemetry project ID and region are filled
      if (disabled || !telemetryProjectId || !config.region) {
        return;
      }

      // Basic project ID validation before making API call
      if (!/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(telemetryProjectId)) {
        return;
      }

      setLoadingNetworks(true);
      setNetworkError('');

      try {
        const result = await deploymentApi.getNetworksAndSubnets(telemetryProjectId, config.region);

        if (result.success && result.data) {
          setNetworks(result.data.networks || []);
          setSubnets(result.data.subnets || []);

          // If current network/subnet aren't in the lists, set to first available option
          if (result.data.networks.length > 0) {
            const networkExists = result.data.networks.some(n => n.name === config.network);
            if (!networkExists) {
              updateConfig('network', result.data.networks[0].name);
            }
          }

          if (result.data.subnets.length > 0) {
            const subnetExists = result.data.subnets.some(s => s.name === config.subnetwork);
            if (!subnetExists) {
              updateConfig('subnetwork', result.data.subnets[0].name);
            }
          }
        } else {
          setNetworkError(result.error || 'Failed to fetch networks and subnets');
          // Fallback to default
          setNetworks([{ name: 'default', selfLink: '' }]);
          setSubnets([{ name: 'default', network: 'default', region: config.region, ipCidrRange: '' }]);
        }
      } catch (error: any) {
        console.error('Error fetching networks and subnets:', error);
        setNetworkError(error.message || 'Failed to fetch networks');
        // Fallback to default
        setNetworks([{ name: 'default', selfLink: '' }]);
        setSubnets([{ name: 'default', network: 'default', region: config.region, ipCidrRange: '' }]);
      } finally {
        setLoadingNetworks(false);
      }
    };

    fetchNetworksAndSubnets();
  }, [telemetryProjectId, config.region, disabled]);

  // Auto-sync geminiRegion with deployment region when region changes
  useEffect(() => {
    if (config.region && !config.geminiRegion) {
      // Auto-fill geminiRegion with deployment region if not already set
      updateConfig('geminiRegion', config.region);
    }
  }, [config.region]);

  const validateForm = (): boolean => {
    const newErrors: Record<string, string> = {};

    // Validate Gemini CLI project ID (if not using same project)
    if (!config.useSameProjectForGemini && !config.geminiCliProjectId.trim()) {
      newErrors.geminiCliProjectId = 'Gemini CLI Project ID is required';
    } else if (!config.useSameProjectForGemini && !/^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(config.geminiCliProjectId)) {
      newErrors.geminiCliProjectId = 'Invalid project ID format';
    }

    // Telemetry project ID validation (always from parent - read-only)
    if (!telemetryProjectId.trim()) {
      newErrors.telemetryProjectId = 'Telemetry Project ID is required (from bootstrap)';
    }

    if (!config.datasetName.trim()) {
      newErrors.datasetName = 'Dataset name is required';
    } else if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(config.datasetName)) {
      newErrors.datasetName = 'Invalid dataset name (alphanumeric and underscores only)';
    }

    if (!config.region) {
      newErrors.region = 'Region is required';
    }

    if (!config.network.trim()) {
      newErrors.network = 'VPC network name is required';
    } else if (!/^[a-z][-a-z0-9]*$/.test(config.network)) {
      newErrors.network = 'Invalid network name (lowercase, hyphens, start with letter)';
    }

    if (!config.subnetwork.trim()) {
      newErrors.subnetwork = 'Subnetwork name is required';
    } else if (!/^[a-z][-a-z0-9]*$/.test(config.subnetwork)) {
      newErrors.subnetwork = 'Invalid subnetwork name (lowercase, hyphens, start with letter)';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (validateForm()) {
      onSubmit(config);
    }
  };

  const updateConfig = (field: keyof DeploymentConfig, value: string | boolean) => {
    setConfig(prev => ({ ...prev, [field]: value }));
    // Clear error for this field when user starts typing
    if (errors[field]) {
      setErrors(prev => {
        const newErrors = { ...prev };
        delete newErrors[field];
        return newErrors;
      });
    }
  };

  // Handler for "use same project" toggle
  const handleUseSameProjectToggle = (checked: boolean) => {
    if (checked) {
      // When enabling "use same project", use telemetry project for Gemini CLI too
      setConfig(prev => ({
        ...prev,
        useSameProjectForGemini: true,
        geminiCliProjectId: telemetryProjectId
      }));
    } else {
      // When disabling "use same project", clear Gemini CLI project ID
      setConfig(prev => ({
        ...prev,
        useSameProjectForGemini: false,
        geminiCliProjectId: ''
      }));
    }
  };

  // Handler for Gemini CLI project ID changes
  const handleGeminiCliProjectIdChange = (value: string) => {
    updateConfig('geminiCliProjectId', value);
  };

  return (
    <form onSubmit={handleSubmit} className="bg-slate-800 rounded-xl p-8 border border-slate-700 relative">
      {/* Disabled Overlay */}
      {disabled && (
        <div className="absolute inset-0 bg-slate-900/50 backdrop-blur-sm rounded-xl flex items-center justify-center z-10">
          <p className="text-lg text-slate-400">Complete bootstrap first</p>
        </div>
      )}

      <h2 className="text-2xl font-bold mb-6">Configuration</h2>

      <div className="space-y-6">
        {/* Telemetry Project ID - Read-only */}
        <div>
          <label htmlFor="telemetryProjectId" className="block text-sm font-medium text-slate-300 mb-2">
            Telemetry Project ID *
          </label>
          <input
            type="text"
            id="telemetryProjectId"
            value={telemetryProjectId}
            disabled={true}
            className="w-full px-4 py-2 bg-slate-900/50 border border-slate-600 rounded-lg cursor-not-allowed opacity-75"
          />
          <p className="mt-1 text-xs text-slate-400">
            Infrastructure deployment target (locked after bootstrap)
          </p>
        </div>

        {/* Use Same Project Toggle */}
        <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-4">
          <div className="flex items-start">
            <input
              type="checkbox"
              id="useSameProjectForGemini"
              checked={config.useSameProjectForGemini}
              onChange={(e) => handleUseSameProjectToggle(e.target.checked)}
              disabled={disabled}
              className="mt-1 w-4 h-4 text-primary bg-slate-900 border-slate-600 rounded focus:ring-primary focus:ring-2"
            />
            <div className="ml-3">
              <label htmlFor="useSameProjectForGemini" className="text-sm font-medium text-slate-300 cursor-pointer">
                Use same project for Gemini CLI inference?
              </label>
              <p className="text-xs text-slate-400 mt-1">
                <span className="font-medium text-blue-400">Recommended for most users.</span> Uncheck if you need separate billing or different quotas.
              </p>
            </div>
          </div>
        </div>

        {/* Gemini CLI Project (conditional) */}
        {!config.useSameProjectForGemini && (
          <div className="space-y-4">
            <div>
              <label htmlFor="geminiCliProjectId" className="block text-sm font-medium text-slate-300 mb-2">
                Gemini CLI Project ID *
              </label>
              <input
                type="text"
                id="geminiCliProjectId"
                value={config.geminiCliProjectId}
                onChange={(e) => handleGeminiCliProjectIdChange(e.target.value)}
                disabled={disabled}
                className={`w-full px-4 py-2 bg-slate-900 border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
                  errors.geminiCliProjectId
                    ? 'border-red-500 focus:ring-red-500'
                    : 'border-slate-600 focus:ring-primary'
                }`}
                placeholder="my-gemini-cli-project"
              />
              {errors.geminiCliProjectId && (
                <p className="mt-1 text-sm text-red-400">{errors.geminiCliProjectId}</p>
              )}
              <p className="mt-1 text-xs text-slate-400">
                Project where Gemini CLI will run and incur API costs
              </p>
            </div>
          </div>
        )}

        {/* Region */}
        <div>
          <label htmlFor="region" className="block text-sm font-medium text-slate-300 mb-2">
            Region *
          </label>
          <select
            id="region"
            value={config.region}
            onChange={(e) => updateConfig('region', e.target.value)}
            className={`w-full px-4 py-2 bg-slate-900 border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
              errors.region
                ? 'border-red-500 focus:ring-red-500'
                : 'border-slate-600 focus:ring-primary'
            }`}
          >
            {/* Multi-regions */}
            <optgroup label="Multi-regions">
              <option value="US">US (United States)</option>
              <option value="EU">EU (European Union)</option>
            </optgroup>

            {/* Americas */}
            <optgroup label="Americas">
              <option value="us-central1">us-central1 (Iowa) 🌱 Low CO2</option>
              <option value="us-east1">us-east1 (South Carolina)</option>
              <option value="us-east4">us-east4 (Northern Virginia)</option>
              <option value="us-east5">us-east5 (Columbus, Ohio)</option>
              <option value="us-south1">us-south1 (Dallas) 🌱 Low CO2</option>
              <option value="us-west1">us-west1 (Oregon) 🌱 Low CO2</option>
              <option value="us-west2">us-west2 (Los Angeles)</option>
              <option value="us-west3">us-west3 (Salt Lake City)</option>
              <option value="us-west4">us-west4 (Las Vegas)</option>
              <option value="northamerica-northeast1">northamerica-northeast1 (Montréal) 🌱 Low CO2</option>
              <option value="northamerica-northeast2">northamerica-northeast2 (Toronto) 🌱 Low CO2</option>
              <option value="northamerica-south1">northamerica-south1 (Mexico)</option>
              <option value="southamerica-east1">southamerica-east1 (São Paulo) 🌱 Low CO2</option>
              <option value="southamerica-west1">southamerica-west1 (Santiago) 🌱 Low CO2</option>
            </optgroup>

            {/* Europe */}
            <optgroup label="Europe">
              <option value="europe-central2">europe-central2 (Warsaw)</option>
              <option value="europe-north1">europe-north1 (Finland) 🌱 Low CO2</option>
              <option value="europe-north2">europe-north2 (Stockholm) 🌱 Low CO2</option>
              <option value="europe-southwest1">europe-southwest1 (Madrid) 🌱 Low CO2</option>
              <option value="europe-west1">europe-west1 (Belgium) 🌱 Low CO2</option>
              <option value="europe-west2">europe-west2 (London) 🌱 Low CO2</option>
              <option value="europe-west3">europe-west3 (Frankfurt)</option>
              <option value="europe-west4">europe-west4 (Netherlands) 🌱 Low CO2</option>
              <option value="europe-west6">europe-west6 (Zürich) 🌱 Low CO2</option>
              <option value="europe-west8">europe-west8 (Milan)</option>
              <option value="europe-west9">europe-west9 (Paris) 🌱 Low CO2</option>
              <option value="europe-west10">europe-west10 (Berlin)</option>
              <option value="europe-west12">europe-west12 (Turin)</option>
            </optgroup>

            {/* Asia Pacific */}
            <optgroup label="Asia Pacific">
              <option value="asia-east1">asia-east1 (Taiwan)</option>
              <option value="asia-east2">asia-east2 (Hong Kong)</option>
              <option value="asia-northeast1">asia-northeast1 (Tokyo)</option>
              <option value="asia-northeast2">asia-northeast2 (Osaka)</option>
              <option value="asia-northeast3">asia-northeast3 (Seoul)</option>
              <option value="asia-south1">asia-south1 (Mumbai)</option>
              <option value="asia-south2">asia-south2 (Delhi)</option>
              <option value="asia-southeast1">asia-southeast1 (Singapore)</option>
              <option value="asia-southeast2">asia-southeast2 (Jakarta)</option>
              <option value="australia-southeast1">australia-southeast1 (Sydney)</option>
              <option value="australia-southeast2">australia-southeast2 (Melbourne)</option>
            </optgroup>

            {/* Middle East */}
            <optgroup label="Middle East">
              <option value="me-central1">me-central1 (Doha)</option>
              <option value="me-central2">me-central2 (Dammam)</option>
              <option value="me-west1">me-west1 (Tel Aviv)</option>
            </optgroup>

            {/* Africa */}
            <optgroup label="Africa">
              <option value="africa-south1">africa-south1 (Johannesburg)</option>
            </optgroup>
          </select>
          {errors.region && (
            <p className="mt-1 text-sm text-red-400">{errors.region}</p>
          )}
          <p className="mt-1 text-xs text-slate-400">
            Choose a region close to your location for better performance. 🌱 indicates low CO2 regions.
          </p>
        </div>

        {/* Gemini CLI Authentication Method - Only show after region selected */}
        {config.region && (
          <div className="border border-slate-600 rounded-lg p-4 bg-slate-800/50">
            <label className="block text-sm font-medium text-slate-300 mb-3">
              Gemini CLI Authentication Method *
            </label>

            {/* OAuth Radio Button */}
            <div className="mb-3">
              <label className="flex items-center cursor-pointer">
                <input
                  type="radio"
                  name="geminiAuthMethod"
                  value="oauth"
                  checked={config.geminiAuthMethod === 'oauth'}
                  onChange={(e) => updateConfig('geminiAuthMethod', e.target.value)}
                  className="w-4 h-4 text-primary bg-slate-900 border-slate-600 focus:ring-primary focus:ring-2"
                />
                <span className="ml-2 text-sm text-slate-200 font-medium">OAuth (Recommended)</span>
              </label>
              <p className="ml-6 text-xs text-slate-400 mt-1">
                Browser-based authentication with Google. Uses Application Default Credentials.
              </p>
            </div>

            {/* Vertex AI Radio Button */}
            <div className="mb-3">
              <label className="flex items-center cursor-pointer">
                <input
                  type="radio"
                  name="geminiAuthMethod"
                  value="vertex-ai"
                  checked={config.geminiAuthMethod === 'vertex-ai'}
                  onChange={(e) => updateConfig('geminiAuthMethod', e.target.value)}
                  className="w-4 h-4 text-primary bg-slate-900 border-slate-600 focus:ring-primary focus:ring-2"
                />
                <span className="ml-2 text-sm text-slate-200 font-medium">Vertex AI</span>
              </label>
              <p className="ml-6 text-xs text-slate-400 mt-1">
                Uses Google Cloud Application Default Credentials (ADC) with Vertex AI API.
              </p>
            </div>

            {/* OAuth Authentication Button - Required for OAuth mode */}
            {config.geminiAuthMethod === 'oauth' && (
              <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/20 rounded-lg">
                <p className="text-xs text-blue-300 mb-2">
                  {config.useSameProjectForGemini
                    ? 'OAuth authentication requires Application Default Credentials (ADC) for Gemini CLI.'
                    : 'Cross-project setup requires separate authentication for Gemini CLI project.'}
                </p>
                {onGeminiOAuth && (
                  <button
                    type="button"
                    onClick={onGeminiOAuth}
                    disabled={!config.useSameProjectForGemini && !config.geminiCliProjectId}
                    className="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-slate-700 disabled:cursor-not-allowed rounded-lg text-sm font-medium transition-colors flex items-center gap-2"
                  >
                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 7a2 2 0 012 2m4 0a6 6 0 01-7.743 5.743L11 17H9v2H7v2H4a1 1 0 01-1-1v-2.586a1 1 0 01.293-.707l5.964-5.964A6 6 0 1121 9z" />
                    </svg>
                    {config.useSameProjectForGemini
                      ? 'Create Application Default Credentials'
                      : 'Authenticate Gemini CLI Project'}
                  </button>
                )}
                {geminiAuthStatus && (
                  <p className="mt-2 text-xs text-green-400">
                    ✓ {geminiAuthStatus}
                  </p>
                )}
              </div>
            )}

            {/* Vertex AI Region Selection */}
            {config.geminiAuthMethod === 'vertex-ai' && (
              <div className="mt-4">
                <label htmlFor="geminiRegion" className="block text-sm font-medium text-slate-300 mb-2">
                  Gemini API Region *
                </label>
                <select
                  id="geminiRegion"
                  value={config.geminiRegion}
                  onChange={(e) => updateConfig('geminiRegion', e.target.value)}
                  className="w-full px-4 py-2 bg-slate-900 border border-slate-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary"
                >
                  <option value="us-central1">us-central1 (Iowa)</option>
                  <option value="us-east1">us-east1 (South Carolina)</option>
                  <option value="us-east4">us-east4 (Northern Virginia)</option>
                  <option value="us-west1">us-west1 (Oregon)</option>
                  <option value="us-west2">us-west2 (Los Angeles)</option>
                  <option value="us-west4">us-west4 (Las Vegas)</option>
                  <option value="europe-west1">europe-west1 (Belgium)</option>
                  <option value="europe-west2">europe-west2 (London)</option>
                  <option value="europe-west3">europe-west3 (Frankfurt)</option>
                  <option value="europe-west4">europe-west4 (Netherlands)</option>
                  <option value="asia-northeast1">asia-northeast1 (Tokyo)</option>
                  <option value="asia-northeast3">asia-northeast3 (Seoul)</option>
                  <option value="asia-southeast1">asia-southeast1 (Singapore)</option>
                </select>
                <p className="mt-1 text-xs text-slate-400">
                  Region for Gemini API calls. Can differ from deployment infrastructure region.
                </p>

                {/* Status indicator */}
                <div className="mt-3 p-3 bg-green-500/10 border border-green-500/20 rounded-lg">
                  <p className="text-xs text-green-300">
                    ✓ Will use Vertex AI with Application Default Credentials
                  </p>
                  <p className="text-xs text-green-300 mt-1">
                    ✓ Region: {config.geminiRegion}
                  </p>
                  <p className="text-xs text-green-300 mt-1">
                    ✓ Project: {config.geminiCliProjectId || config.telemetryProjectId}
                  </p>
                </div>
              </div>
            )}
          </div>
        )}

        {/* VPC Network */}
        <div>
          <label htmlFor="network" className="block text-sm font-medium text-slate-300 mb-2">
            VPC Network Name *
          </label>
          <select
            id="network"
            value={config.network}
            onChange={(e) => updateConfig('network', e.target.value)}
            disabled={loadingNetworks || networks.length === 0}
            className={`w-full px-4 py-2 bg-slate-900 border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
              errors.network
                ? 'border-red-500 focus:ring-red-500'
                : 'border-slate-600 focus:ring-primary'
            } ${loadingNetworks ? 'opacity-50 cursor-not-allowed' : ''}`}
          >
            {loadingNetworks ? (
              <option>Loading networks...</option>
            ) : networks.length === 0 ? (
              <option>Enter project ID and region first</option>
            ) : (
              networks.map((network) => (
                <option key={network.name} value={network.name}>
                  {network.name}
                </option>
              ))
            )}
          </select>
          {errors.network && (
            <p className="mt-1 text-sm text-red-400">{errors.network}</p>
          )}
          {networkError && (
            <p className="mt-1 text-sm text-yellow-400">{networkError}</p>
          )}
          <p className="mt-1 text-xs text-slate-400">
            VPC network for Dataflow workers. Networks are automatically fetched from your project.
          </p>
        </div>

        {/* Subnetwork */}
        <div>
          <label htmlFor="subnetwork" className="block text-sm font-medium text-slate-300 mb-2">
            Subnetwork Name *
          </label>
          <select
            id="subnetwork"
            value={config.subnetwork}
            onChange={(e) => updateConfig('subnetwork', e.target.value)}
            disabled={loadingNetworks || subnets.length === 0}
            className={`w-full px-4 py-2 bg-slate-900 border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
              errors.subnetwork
                ? 'border-red-500 focus:ring-red-500'
                : 'border-slate-600 focus:ring-primary'
            } ${loadingNetworks ? 'opacity-50 cursor-not-allowed' : ''}`}
          >
            {loadingNetworks ? (
              <option>Loading subnets...</option>
            ) : subnets.length === 0 ? (
              <option>Enter project ID and region first</option>
            ) : (
              subnets.map((subnet) => (
                <option key={subnet.name} value={subnet.name}>
                  {subnet.name} ({subnet.ipCidrRange})
                </option>
              ))
            )}
          </select>
          {errors.subnetwork && (
            <p className="mt-1 text-sm text-red-400">{errors.subnetwork}</p>
          )}
          <p className="mt-1 text-xs text-slate-400">
            Subnetwork in {config.region}. Subnets are automatically fetched from your project.
          </p>
        </div>

        {/* Dataset Name */}
        <div>
          <label htmlFor="datasetName" className="block text-sm font-medium text-slate-300 mb-2">
            BigQuery Dataset Name *
          </label>
          <input
            type="text"
            id="datasetName"
            value={config.datasetName}
            onChange={(e) => updateConfig('datasetName', e.target.value)}
            className={`w-full px-4 py-2 bg-slate-900 border rounded-lg focus:outline-none focus:ring-2 transition-colors ${
              errors.datasetName
                ? 'border-red-500 focus:ring-red-500'
                : 'border-slate-600 focus:ring-primary'
            }`}
            placeholder="gemini_cli_telemetry"
          />
          {errors.datasetName && (
            <p className="mt-1 text-sm text-red-400">{errors.datasetName}</p>
          )}
          <p className="mt-1 text-xs text-slate-400">
            Alphanumeric and underscores only, must start with a letter or underscore
          </p>
        </div>

        {/* Log Prompts */}
        <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
          <div className="flex items-start">
            <input
              type="checkbox"
              id="logPrompts"
              checked={config.logPrompts}
              onChange={(e) => updateConfig('logPrompts', e.target.checked)}
              className="mt-1 w-4 h-4 text-primary bg-slate-900 border-slate-600 rounded focus:ring-primary focus:ring-2"
            />
            <div className="ml-3">
              <label htmlFor="logPrompts" className="text-sm font-medium text-slate-300 cursor-pointer">
                Log prompts and responses
              </label>
              <p className="text-xs text-slate-400 mt-1">
                Include full prompt text and model responses in telemetry data.
                <span className="text-yellow-400 font-medium"> Warning:</span> This may include sensitive information.
              </p>
            </div>
          </div>
        </div>

        {/* Pseudoanonymize PII */}
        <div className="bg-slate-900/50 rounded-lg p-4 border border-slate-700">
          <div className="flex items-start">
            <input
              type="checkbox"
              id="pseudoanonymizePii"
              checked={config.pseudoanonymizePii}
              onChange={(e) => updateConfig('pseudoanonymizePii', e.target.checked)}
              className="mt-1 w-4 h-4 text-primary bg-slate-900 border-slate-600 rounded focus:ring-primary focus:ring-2"
            />
            <div className="ml-3">
              <label htmlFor="pseudoanonymizePii" className="text-sm font-medium text-slate-300 cursor-pointer">
                Pseudoanonymize user identifiers (GDPR compliance)
              </label>
              <p className="text-xs text-slate-400 mt-1">
                Hash email addresses and user IDs with SHA256 in all analytics views.
                <span className="text-green-400 font-medium"> Recommended</span> for production deployments.
                Cannot be reversed to protect user privacy.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* Actions */}
      <div className="mt-8 flex gap-4">
        <button
          type="submit"
          disabled={disabled}
          className="flex-1 bg-primary hover:bg-primary/90 disabled:bg-slate-700 disabled:cursor-not-allowed text-slate-900 font-semibold py-3 px-6 rounded-lg transition-colors"
        >
          Start Deployment
        </button>
      </div>
    </form>
  );
}
