#!/bin/bash
# Setup script for Gemini CLI Telemetry Deployment App

set -e

echo "🚀 Setting up Gemini CLI Telemetry Deployment App..."
echo ""

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Node.js
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 18+ first."
    exit 1
fi
echo "✓ Node.js $(node --version)"

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.9+ first."
    exit 1
fi
echo "✓ Python $(python3 --version)"

# Check gcloud
if ! command -v gcloud &> /dev/null; then
    echo "⚠️  gcloud CLI is not installed. You'll need it to deploy."
else
    echo "✓ gcloud CLI installed"
fi

# Check gemini
if ! command -v gemini &> /dev/null; then
    echo "⚠️  gemini CLI is not installed. You'll need it to deploy."
else
    echo "✓ gemini CLI installed"
fi

echo ""
echo "📦 Installing frontend dependencies..."
cd frontend
npm install
cd ..

echo ""
echo "📦 Installing backend dependencies..."
cd backend
python3 -m pip install -r requirements.txt
cd ..

echo ""
echo "✅ Setup complete!"
echo ""
echo "To run the app:"
echo "  ./run.sh"
echo ""
echo "Or run frontend and backend separately:"
echo "  Frontend: cd frontend && npm run dev"
echo "  Backend:  cd backend && python3 main.py"
