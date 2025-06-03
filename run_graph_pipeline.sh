#!/bin/bash

# Graph Pipeline Runner Script
# Activates virtual environment and runs the graph pipeline

set -e  # Exit on any error

echo "🚀 Starting Graph Pipeline..."
echo "📍 Working directory: $(pwd)"

# Check if virtual environment exists
if [ ! -d "city_clerk_rag" ]; then
    echo "❌ Virtual environment 'city_clerk_rag' not found!"
    echo "Please create it first with: python3 -m venv city_clerk_rag"
    exit 1
fi

echo "🔧 Activating virtual environment..."
source city_clerk_rag/bin/activate

echo "🐍 Using Python: $(which python3)"
echo "📦 Python version: $(python3 --version)"

# Verify gremlinpython is installed
echo "🔍 Checking gremlinpython installation..."
if python3 -c "import gremlin_python; print('✅ gremlinpython is available')" 2>/dev/null; then
    echo "✅ gremlinpython import successful"
else
    echo "❌ gremlinpython not found, installing..."
    pip3 install "gremlinpython>=3.7.3"
fi

# Test the specific import from the script
echo "🧪 Testing specific imports..."
if python3 -c "from gremlin_python.driver import client, serializer; print('✅ Specific imports successful')" 2>/dev/null; then
    echo "✅ All required imports working"
else
    echo "❌ Import test failed"
    exit 1
fi

echo "▶️  Running graph pipeline..."
python3 scripts/graph_pipeline.py

echo "🎉 Graph pipeline completed!" 