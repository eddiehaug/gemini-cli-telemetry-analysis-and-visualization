import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, RefreshCw } from 'lucide-react';
import WizardStepper from '../components/WizardStepper';
import DeploymentProgress from '../components/DeploymentProgress';
import { DeploymentState } from '../types';
import deploymentApi from '../services/api';

export default function StatusPage() {
  const { deploymentId } = useParams<{ deploymentId: string }>();
  const navigate = useNavigate();
  const [deployment, setDeployment] = useState<DeploymentState | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string>('');

  const fetchStatus = async () => {
    if (!deploymentId) return;

    try {
      setLoading(true);
      const result = await deploymentApi.getStatus(deploymentId);
      if (result.success && result.data) {
        setDeployment(result.data);
      } else {
        setError(result.error || 'Failed to fetch deployment status');
      }
    } catch (err: any) {
      setError(err.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchStatus();

    // Poll for updates if deployment is in progress
    const interval = setInterval(() => {
      if (deployment?.status === 'deploying') {
        fetchStatus();
      }
    }, 3000);

    return () => clearInterval(interval);
  }, [deploymentId, deployment?.status]);

  if (loading && !deployment) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="w-8 h-8 text-primary animate-spin mx-auto mb-4" />
          <p className="text-slate-400">Loading deployment status...</p>
        </div>
      </div>
    );
  }

  if (error || !deployment) {
    return (
      <div className="min-h-screen flex items-center justify-center p-6">
        <div className="max-w-md w-full bg-red-500/10 border border-red-500/20 rounded-lg p-6">
          <h2 className="text-xl font-bold text-red-400 mb-2">Error</h2>
          <p className="text-red-300 mb-4">{error || 'Deployment not found'}</p>
          <button
            onClick={() => navigate('/')}
            className="flex items-center gap-2 text-primary hover:text-primary/90 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen p-6">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <div className="mb-8">
          <button
            onClick={() => navigate('/')}
            className="flex items-center gap-2 text-slate-400 hover:text-slate-300 transition-colors mb-4"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Home
          </button>
          <h1 className="text-3xl font-bold mb-2">Deployment Status</h1>
          <p className="text-slate-400">
            Deployment ID: <span className="font-mono text-sm">{deploymentId}</span>
          </p>
        </div>

        {/* Stepper */}
        <div className="mb-8">
          <WizardStepper
            steps={deployment.steps.map((s, idx) => ({ ...s, number: idx + 1 }))}
            currentStep={deployment.currentStep}
          />
        </div>

        {/* Progress */}
        {deployment.status === 'deploying' && (
          <DeploymentProgress
            steps={deployment.steps}
            currentStep={deployment.currentStep}
            config={deployment.config}
          />
        )}

        {/* Completed */}
        {deployment.status === 'completed' && (
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
                Gemini CLI telemetry is now configured and data is flowing to BigQuery
              </p>

              {/* Created Resources */}
              {deployment.createdResources && (
                <div className="bg-slate-900/50 rounded-lg p-4 mb-6 text-left">
                  <h3 className="font-semibold mb-3">Created Resources</h3>
                  <div className="space-y-2 text-sm">
                    {deployment.createdResources.dataset && (
                      <div>
                        <span className="text-slate-400">Dataset:</span>
                        <span className="ml-2 text-slate-200 font-mono">
                          {deployment.createdResources.dataset}
                        </span>
                      </div>
                    )}
                    {deployment.createdResources.table && (
                      <div>
                        <span className="text-slate-400">Table:</span>
                        <span className="ml-2 text-slate-200 font-mono">
                          {deployment.createdResources.table}
                        </span>
                      </div>
                    )}
                    {deployment.createdResources.sink && (
                      <div>
                        <span className="text-slate-400">Sink:</span>
                        <span className="ml-2 text-slate-200 font-mono">
                          {deployment.createdResources.sink}
                        </span>
                      </div>
                    )}
                  </div>
                </div>
              )}

              <div className="flex gap-4 justify-center">
                <button
                  onClick={() => navigate('/')}
                  className="px-6 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
                >
                  Back to Home
                </button>
                <button
                  onClick={() => window.open(`https://console.cloud.google.com/bigquery?project=${deployment.config.telemetryProjectId}`, '_blank')}
                  className="px-6 py-2 bg-primary hover:bg-primary/90 text-slate-900 font-semibold rounded-lg transition-colors"
                >
                  View in BigQuery
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Failed */}
        {deployment.status === 'failed' && (
          <div className="bg-slate-800 rounded-xl p-8 border border-red-500/20">
            <div className="text-center">
              <div className="inline-flex items-center justify-center w-16 h-16 bg-red-500/20 rounded-full mb-4">
                <svg className="w-8 h-8 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </div>
              <h2 className="text-2xl font-bold text-red-500 mb-2">
                Deployment Failed
              </h2>
              <p className="text-slate-300 mb-6">
                The deployment encountered an error. Please review the logs and try again.
              </p>
              <div className="flex gap-4 justify-center">
                <button
                  onClick={() => navigate('/')}
                  className="px-6 py-2 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors"
                >
                  Back to Home
                </button>
                <button
                  onClick={() => navigate('/wizard')}
                  className="px-6 py-2 bg-primary hover:bg-primary/90 text-slate-900 font-semibold rounded-lg transition-colors"
                >
                  Try Again
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
