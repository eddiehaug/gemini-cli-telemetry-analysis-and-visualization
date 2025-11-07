import { AlertCircle, CheckCircle, Loader2, XCircle } from 'lucide-react';
import { DeploymentStep, DeploymentConfig } from '../types';

interface DeploymentProgressProps {
  steps: DeploymentStep[];
  currentStep: number;
  config: DeploymentConfig;
}

export default function DeploymentProgress({ steps, config }: DeploymentProgressProps) {
  const completedSteps = steps.filter(s => s.status === 'completed').length;
  const failedSteps = steps.filter(s => s.status === 'failed').length;
  const progress = (completedSteps / steps.length) * 100;

  const getStatusIcon = (step: DeploymentStep) => {
    switch (step.status) {
      case 'completed':
        return <CheckCircle className="w-5 h-5 text-green-500" />;
      case 'failed':
        return <XCircle className="w-5 h-5 text-red-500" />;
      case 'in_progress':
        return <Loader2 className="w-5 h-5 text-primary animate-spin" />;
      default:
        return <div className="w-5 h-5 rounded-full border-2 border-slate-600" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-slate-800 rounded-xl p-6 border border-slate-700">
        <h2 className="text-2xl font-bold mb-4">Deployment in Progress</h2>

        {/* Progress Bar */}
        <div className="mb-4">
          <div className="flex justify-between text-sm text-slate-400 mb-2">
            <span>Progress</span>
            <span>{completedSteps} of {steps.length} steps completed</span>
          </div>
          <div className="w-full h-2 bg-slate-700 rounded-full overflow-hidden">
            <div
              className="h-full bg-primary transition-all duration-500 ease-out"
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>

        {/* Config Summary */}
        <div className="grid grid-cols-2 gap-4 text-sm">
          <div>
            <span className="text-slate-400">Project:</span>
            <span className="ml-2 text-slate-200 font-mono">{config.telemetryProjectId}</span>
          </div>
          <div>
            <span className="text-slate-400">Region:</span>
            <span className="ml-2 text-slate-200">{config.region}</span>
          </div>
          <div>
            <span className="text-slate-400">Dataset:</span>
            <span className="ml-2 text-slate-200 font-mono">{config.datasetName}</span>
          </div>
          <div>
            <span className="text-slate-400">Log Prompts:</span>
            <span className="ml-2 text-slate-200">{config.logPrompts ? 'Yes' : 'No'}</span>
          </div>
        </div>
      </div>

      {/* Step Details */}
      <div className="bg-slate-800 rounded-xl p-6 border border-slate-700">
        <h3 className="text-lg font-semibold mb-4">Deployment Steps</h3>

        <div className="space-y-3">
          {steps.map((step, index) => (
            <div
              key={step.id}
              className={`p-4 rounded-lg border transition-all ${
                step.status === 'in_progress'
                  ? 'bg-primary/10 border-primary/30'
                  : step.status === 'completed'
                  ? 'bg-slate-900/50 border-slate-700'
                  : step.status === 'failed'
                  ? 'bg-red-500/10 border-red-500/30'
                  : 'bg-slate-900/30 border-slate-700/50'
              }`}
            >
              <div className="flex items-start gap-3">
                {/* Icon */}
                <div className="flex-shrink-0 mt-0.5">
                  {getStatusIcon(step)}
                </div>

                {/* Content */}
                <div className="flex-1 min-w-0">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div
                        className={`font-medium ${
                          step.status === 'in_progress'
                            ? 'text-primary'
                            : step.status === 'completed'
                            ? 'text-green-400'
                            : step.status === 'failed'
                            ? 'text-red-400'
                            : 'text-slate-400'
                        }`}
                      >
                        {step.name}
                      </div>
                      <div className="text-sm text-slate-400 mt-0.5">
                        {step.description}
                      </div>
                    </div>
                    <span className="text-xs text-slate-500 font-mono">
                      {index + 1}/{steps.length}
                    </span>
                  </div>

                  {/* Details */}
                  {step.details && (
                    <div className="mt-2 text-sm text-slate-300 flex items-start gap-2">
                      <AlertCircle className="w-4 h-4 flex-shrink-0 mt-0.5 text-slate-400" />
                      <span>{step.details}</span>
                    </div>
                  )}

                  {/* Error */}
                  {step.error && (
                    <div className="mt-2 p-3 bg-red-500/10 border border-red-500/20 rounded text-sm text-red-300">
                      <div className="font-medium mb-1">Error:</div>
                      <div className="font-mono text-xs">{step.error}</div>
                    </div>
                  )}

                  {/* In Progress Indicator */}
                  {step.status === 'in_progress' && (
                    <div className="mt-2 flex items-center gap-2 text-sm text-primary">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      <span>Processing...</span>
                    </div>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Failed Message */}
      {failedSteps > 0 && (
        <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-4">
          <div className="flex items-start gap-3">
            <XCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
            <div>
              <div className="font-semibold text-red-300">Deployment Failed</div>
              <div className="text-sm text-red-300/90 mt-1">
                The deployment encountered an error. Please review the error message above and try again.
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
