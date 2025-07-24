#!/bin/bash

# Debug NER Extraction Test Runner
# Ensures virtual environment is activated and runs the debug script

echo "🐛 DEBUG NER EXTRACTION TEST"
echo "=============================="

# Navigate to the script's directory
cd "$(dirname "$0")"

# Check if simple_ner_env virtual environment exists and activate it
if [ -d "simple_ner_env" ]; then
    echo "🐍 Activating simple_ner_env virtual environment..."
    source simple_ner_env/bin/activate
elif [ -d "venv" ]; then
    echo "🐍 Activating venv virtual environment..."
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "🐍 Activating .venv virtual environment..."
    source .venv/bin/activate
else
    echo "⚠️  No virtual environment found. Please activate manually."
fi

# Verify Python version
echo "🔍 Python version: $(python3 --version)"
echo "📂 Working directory: $(pwd)"

# Run the debug script
echo ""
echo "🚀 Starting debug NER extraction..."
echo ""

python3 debug_ner_extraction.py

echo ""
echo "✅ Debug script completed. Check the logs and debug_ner_test/ directory for results." 