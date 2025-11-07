import { useNavigate } from 'react-router-dom';
import { Terminal, ArrowRight, Sparkles, Database, Network, Workflow } from 'lucide-react';

export default function HomePage() {
  const navigate = useNavigate();

  const features = [
    {
      icon: Database,
      title: 'BigQuery Analytics',
      description: 'Unbreakable JSON schema with 15+ analytics views'
    },
    {
      icon: Network,
      title: 'Real-time Streaming',
      description: 'Pub/Sub → Dataflow → BigQuery pipeline'
    },
    {
      icon: Workflow,
      title: 'Automated Setup',
      description: 'One-click deployment of complete infrastructure'
    }
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      {/* Background decoration */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-1/4 left-1/4 w-96 h-96 bg-primary/5 rounded-full blur-3xl"></div>
        <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-cyan-500/5 rounded-full blur-3xl"></div>
      </div>

      <div className="relative min-h-screen flex items-center justify-center p-6">
        <div className="max-w-5xl w-full">
          {/* Header */}
          <div className="text-center mb-12">
            <div className="inline-flex items-center justify-center w-24 h-24 bg-gradient-to-br from-primary/20 to-cyan-500/20 rounded-3xl mb-6 border border-primary/20 shadow-lg shadow-primary/10">
              <Terminal className="w-12 h-12 text-primary" />
            </div>
            <h1 className="text-5xl font-bold mb-4 bg-gradient-to-r from-white via-slate-200 to-slate-400 bg-clip-text text-transparent">
              Gemini CLI Telemetry
            </h1>
            <p className="text-xl text-slate-400 mb-3">
              Automated ELT Pipeline Deployment
            </p>
            <div className="inline-flex items-center gap-2 px-4 py-2 bg-primary/10 border border-primary/20 rounded-full text-primary text-sm">
              <Sparkles className="w-4 h-4" />
              <span>Production-Ready Infrastructure in 10-15 Minutes</span>
            </div>
          </div>

          {/* Feature Cards */}
          <div className="grid md:grid-cols-3 gap-6 mb-12">
            {features.map((feature, idx) => (
              <div
                key={idx}
                className="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl p-6 hover:border-primary/30 transition-all hover:shadow-lg hover:shadow-primary/5"
              >
                <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center mb-4">
                  <feature.icon className="w-6 h-6 text-primary" />
                </div>
                <h3 className="text-lg font-semibold text-white mb-2">
                  {feature.title}
                </h3>
                <p className="text-slate-400 text-sm">
                  {feature.description}
                </p>
              </div>
            ))}
          </div>

          {/* Main Content Grid */}
          <div className="grid md:grid-cols-2 gap-6 mb-12">
            {/* ELT Pipeline Architecture */}
            <div className="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl p-6">
              <div className="flex items-start gap-3 mb-4">
                <div className="flex-shrink-0 w-10 h-10 bg-cyan-500/10 rounded-lg flex items-center justify-center">
                  <svg className="w-5 h-5 text-cyan-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-white mb-2">Pipeline Architecture</h3>
                  <p className="text-sm text-slate-400">End-to-end data flow</p>
                </div>
              </div>
              <div className="bg-slate-900/50 rounded-lg p-4 font-mono text-sm border border-slate-700/50">
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 bg-cyan-400 rounded-full"></div>
                  <span className="text-cyan-400">Gemini CLI</span>
                </div>
                <div className="text-slate-600 ml-4 text-xs">↓</div>
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 bg-blue-400 rounded-full"></div>
                  <span className="text-blue-400">Cloud Logging</span>
                </div>
                <div className="text-slate-600 ml-4 text-xs">↓ via sink</div>
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 bg-purple-400 rounded-full"></div>
                  <span className="text-purple-400">Pub/Sub Topic</span>
                </div>
                <div className="text-slate-600 ml-4 text-xs">↓ streaming</div>
                <div className="flex items-center gap-2 mb-1">
                  <div className="w-2 h-2 bg-green-400 rounded-full"></div>
                  <span className="text-green-400">Dataflow Pipeline</span>
                </div>
                <div className="text-slate-600 ml-4 text-xs">↓ transform</div>
                <div className="flex items-center gap-2">
                  <div className="w-2 h-2 bg-yellow-400 rounded-full"></div>
                  <span className="text-yellow-400">BigQuery Table</span>
                </div>
              </div>
            </div>

            {/* What Gets Deployed */}
            <div className="bg-slate-800/50 backdrop-blur-sm border border-slate-700/50 rounded-xl p-6">
              <div className="flex items-start gap-3 mb-4">
                <div className="flex-shrink-0 w-10 h-10 bg-blue-500/10 rounded-lg flex items-center justify-center">
                  <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                </div>
                <div>
                  <h3 className="text-lg font-semibold text-white mb-2">Infrastructure Components</h3>
                  <p className="text-sm text-slate-400">17 automated deployment steps</p>
                </div>
              </div>
              <ul className="space-y-2">
                {[
                  'BigQuery dataset & analytics views',
                  'Pub/Sub topic & subscription',
                  'Dataflow streaming pipeline',
                  'Cloud Storage bucket & UDF',
                  'Cloud Logging sink',
                  'Gemini CLI configuration'
                ].map((item, idx) => (
                  <li key={idx} className="flex items-start gap-2 text-sm">
                    <svg className="w-4 h-4 text-primary flex-shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span className="text-slate-300">{item}</span>
                  </li>
                ))}
              </ul>
            </div>
          </div>

          {/* Prerequisites */}
          <div className="bg-gradient-to-r from-blue-500/10 to-cyan-500/10 border border-blue-500/20 rounded-xl p-6 mb-8">
            <div className="flex items-start gap-4">
              <div className="flex-shrink-0 w-10 h-10 bg-blue-500/20 rounded-lg flex items-center justify-center">
                <svg className="w-5 h-5 text-blue-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
              </div>
              <div className="flex-1">
                <h3 className="text-lg font-semibold text-blue-300 mb-3">Getting Started</h3>
                <p className="text-slate-300 mb-4">
                  All you need is a <span className="font-semibold text-white">Google Cloud project with billing enabled</span>. The wizard handles everything else:
                </p>
                <div className="grid sm:grid-cols-2 gap-3 text-sm">
                  <div className="flex items-center gap-2 text-slate-300">
                    <svg className="w-4 h-4 text-primary flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span>Auto-installs dependencies</span>
                  </div>
                  <div className="flex items-center gap-2 text-slate-300">
                    <svg className="w-4 h-4 text-primary flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span>Enables required APIs</span>
                  </div>
                  <div className="flex items-center gap-2 text-slate-300">
                    <svg className="w-4 h-4 text-primary flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span>Configures authentication</span>
                  </div>
                  <div className="flex items-center gap-2 text-slate-300">
                    <svg className="w-4 h-4 text-primary flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                      <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
                    </svg>
                    <span>Verifies end-to-end pipeline</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* CTA */}
          <button
            onClick={() => navigate('/wizard')}
            className="w-full bg-gradient-to-r from-primary to-cyan-500 hover:from-primary/90 hover:to-cyan-500/90 text-slate-900 font-semibold py-5 px-8 rounded-xl transition-all flex items-center justify-center group shadow-lg shadow-primary/20 hover:shadow-xl hover:shadow-primary/30"
          >
            <span className="text-lg">Start Deployment Wizard</span>
            <ArrowRight className="ml-3 w-6 h-6 group-hover:translate-x-1 transition-transform" />
          </button>

          {/* Footer */}
          <div className="text-center mt-6 text-sm text-slate-500">
            <p>Estimated deployment time: <span className="text-slate-400 font-medium">10-15 minutes</span></p>
            <p className="mt-1">Includes Dataflow pipeline startup and end-to-end verification</p>
          </div>
        </div>
      </div>
    </div>
  );
}
