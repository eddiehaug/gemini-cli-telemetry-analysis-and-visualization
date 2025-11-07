import { Check, X, Loader2 } from 'lucide-react';
import { DeploymentStep } from '../types';

interface WizardStepperProps {
  steps: (DeploymentStep & { number: number })[];
  currentStep: number;
}

export default function WizardStepper({ steps, currentStep }: WizardStepperProps) {
  const getStepIcon = (step: DeploymentStep, index: number) => {
    if (step.status === 'completed') {
      return <Check className="w-4 h-4" />;
    }
    if (step.status === 'failed') {
      return <X className="w-4 h-4" />;
    }
    if (step.status === 'in_progress') {
      return <Loader2 className="w-4 h-4 animate-spin" />;
    }
    return <span className="text-xs font-semibold">{index + 1}</span>;
  };

  const getStepColor = (step: DeploymentStep, index: number) => {
    if (step.status === 'completed') {
      return 'bg-green-500 border-green-500 text-white';
    }
    if (step.status === 'failed') {
      return 'bg-red-500 border-red-500 text-white';
    }
    if (step.status === 'in_progress') {
      return 'bg-primary border-primary text-slate-900';
    }
    if (index <= currentStep) {
      return 'bg-slate-700 border-slate-600 text-slate-300';
    }
    return 'bg-slate-800 border-slate-700 text-slate-500';
  };

  const getConnectorColor = (index: number) => {
    if (index < currentStep) {
      if (steps[index].status === 'completed') {
        return 'bg-green-500';
      }
      if (steps[index].status === 'failed') {
        return 'bg-red-500';
      }
      return 'bg-slate-600';
    }
    return 'bg-slate-700';
  };

  // Split steps into two rows (8 per row for 15 steps)
  const midpoint = Math.ceil(steps.length / 2);
  const firstRow = steps.slice(0, midpoint);
  const secondRow = steps.slice(midpoint);

  const renderStepRow = (rowSteps: typeof steps, startIndex: number) => (
    <div className="flex items-start justify-between gap-2">
      {rowSteps.map((step, relIndex) => {
        const index = startIndex + relIndex;
        return (
          <div key={step.id} className="flex flex-col items-center relative flex-1 min-w-0">
            {/* Connector Line */}
            {relIndex < rowSteps.length - 1 && (
              <div
                className={`absolute top-4 left-1/2 w-full h-0.5 transition-colors ${getConnectorColor(index)}`}
                style={{ transform: 'translateY(-50%)' }}
              />
            )}

            {/* Step Circle */}
            <div className="relative z-10">
              <div
                className={`w-8 h-8 rounded-full border-2 flex items-center justify-center transition-all text-xs ${getStepColor(step, index)}`}
              >
                {getStepIcon(step, index)}
              </div>
            </div>

            {/* Step Info */}
            <div className="mt-2 text-center px-1">
              <div
                className={`text-xs font-medium transition-colors leading-tight ${
                  step.status === 'in_progress'
                    ? 'text-primary'
                    : step.status === 'completed'
                    ? 'text-green-400'
                    : step.status === 'failed'
                    ? 'text-red-400'
                    : index <= currentStep
                    ? 'text-slate-300'
                    : 'text-slate-500'
                }`}
              >
                {step.name}
              </div>

              {/* Error message */}
              {step.error && (
                <div className="text-xs text-red-400 mt-1 break-words max-w-[150px]">
                  {step.error}
                </div>
              )}

              {/* Details message */}
              {step.details && !step.error && (
                <div className="text-xs text-slate-400 mt-1 break-words max-w-[150px]">
                  {step.details}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );

  return (
    <div className="relative">
      {/* Desktop: Two-row layout */}
      <div className="hidden md:block space-y-6">
        {renderStepRow(firstRow, 0)}
        {renderStepRow(secondRow, midpoint)}
      </div>

      {/* Mobile view - vertical list */}
      <div className="md:hidden space-y-3">
        {steps.map((step, index) => (
          <div
            key={step.id}
            className={`flex items-start gap-3 p-3 rounded-lg border ${
              step.status === 'in_progress'
                ? 'bg-primary/10 border-primary/30'
                : step.status === 'completed'
                ? 'bg-green-500/10 border-green-500/30'
                : step.status === 'failed'
                ? 'bg-red-500/10 border-red-500/30'
                : 'bg-slate-800 border-slate-700'
            }`}
          >
            <div
              className={`w-8 h-8 rounded-full border-2 flex items-center justify-center flex-shrink-0 ${getStepColor(step, index)}`}
            >
              {getStepIcon(step, index)}
            </div>
            <div className="flex-1 min-w-0">
              <div
                className={`text-sm font-medium ${
                  step.status === 'in_progress'
                    ? 'text-primary'
                    : step.status === 'completed'
                    ? 'text-green-400'
                    : step.status === 'failed'
                    ? 'text-red-400'
                    : 'text-slate-300'
                }`}
              >
                {step.name}
              </div>
              <div className="text-xs text-slate-500 mt-0.5">{step.description}</div>
              {step.details && (
                <div className="text-xs text-slate-400 mt-1">{step.details}</div>
              )}
              {step.error && (
                <div className="text-xs text-red-400 mt-1">{step.error}</div>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
