import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import WizardPage from './pages/WizardPage';
import StatusPage from './pages/StatusPage';

function App() {
  return (
    <Router>
      <div className="min-h-screen bg-slate-950">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/wizard" element={<WizardPage />} />
          <Route path="/status/:deploymentId" element={<StatusPage />} />
        </Routes>
      </div>
    </Router>
  );
}

export default App;
