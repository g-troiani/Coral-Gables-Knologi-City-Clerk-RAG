#!/bin/bash

# run_aggregator.sh
# Script to run the API response aggregator

echo "🚀 Starting API Response Aggregation..."

# Check if virtual environment exists
if [ ! -d "simple_ner_env" ]; then
    echo "❌ Virtual environment 'simple_ner_env' not found!"
    echo "Please create the virtual environment first."
    exit 1
fi

# Activate virtual environment
echo "📦 Activating virtual environment..."
source simple_ner_env/bin/activate

# Run the aggregator
echo "🔄 Running API response aggregator..."
python3 aggregate_api_responses.py

echo "✅ Aggregation complete!"
echo "📁 Check the 2019 folder for AGGREGATE_*.md files" 