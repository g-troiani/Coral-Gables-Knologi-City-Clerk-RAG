#!/bin/bash

echo "🚀 Starting GraphRAG Query UI..."
echo "================================"

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo "🔄 Activating virtual environment..."
    if [ -d "venv" ]; then
        source venv/bin/activate
    else
        echo "❌ Virtual environment 'venv' not found. Please create it first."
        exit 1
    fi
fi

# Check for required environment variables
if [ -z "$OPENAI_API_KEY" ]; then
    echo "❌ Error: OPENAI_API_KEY not set"
    echo "Please run: export OPENAI_API_KEY='your-key-here'"
    exit 1
fi

# Install dash-bootstrap-components if not already installed
python3 -m pip install dash-bootstrap-components --quiet

# Run the UI
python3 graphrag_query_ui.py 