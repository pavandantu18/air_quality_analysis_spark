#!/bin/bash
set -euxo pipefail

echo "✅ Starting setup.sh"

echo "🧰 Installing dependencies..."
apt-get update && apt-get install -y \
  openjdk-11-jdk \
  procps \
  curl \
  python3-pip \
  python3-venv \
  unzip

echo "🐍 Ensuring Python is accessible..."
python3 --version || echo "Python missing!"
pip3 --version || echo "pip missing!"

echo "📦 Installing Python requirements..."
pip3 install --upgrade pip
pip3 install -r /workspaces/air_quality_analysis_spark/requirements.txt || {
  echo "⚠️ pip install failed!"; exit 1;
}

echo "📥 Running ingestion script..."
python3 /workspaces/air_quality_analysis_spark/ingestion/download_from_s3.py || {
  echo "⚠️ Python script failed!"; exit 1;
}

echo "✅ setup.sh completed."
