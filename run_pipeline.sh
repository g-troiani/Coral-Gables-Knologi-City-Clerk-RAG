#!/bin/bash

# This script is the main entry point for running the unified pipeline.
# It ensures the Python virtual environment is activated and executes the main orchestrator.

# Navigate to the script's directory to ensure correct relative paths
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
  echo "🐍 Activating Python virtual environment..."
  source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "🐍 Activating Python virtual environment..."
    source .venv/bin/activate
fi

# Run the main pipeline orchestrator, forwarding all script arguments
echo "🚀 Executing main pipeline..."
python -m graph_rag_stages.main_pipeline "$@" 