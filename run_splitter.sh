#!/bin/bash

# run_splitter.sh
# Script to run the large file splitter

echo "🚀 Starting Large File Splitter..."

# Check if virtual environment exists
if [ ! -d "simple_ner_env" ]; then
    echo "❌ Virtual environment 'simple_ner_env' not found!"
    echo "Please create the virtual environment first."
    exit 1
fi

# Activate virtual environment
echo "📦 Activating virtual environment..."
source simple_ner_env/bin/activate

# Run the splitter
echo "🔄 Running large file splitter..."
python3 split_large_files.py

echo "✅ File splitting complete!"
echo "📁 Check the 2019 folder for split files." 