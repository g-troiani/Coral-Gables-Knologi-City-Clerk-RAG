#!/bin/bash

# Graph Visualizer Runner Script
# Sets up virtual environment and runs the interactive graph visualization web app

set -e  # Exit on any error

echo "🎨 Starting Graph Visualizer..."
echo "📍 Working directory: $(pwd)"

# Check if virtual environment exists, create if not
if [ ! -d "city_clerk_rag" ]; then
    echo "📦 Virtual environment 'city_clerk_rag' not found, creating it..."
    python3 -m venv city_clerk_rag
    echo "✅ Virtual environment created successfully!"
else
    echo "✅ Virtual environment 'city_clerk_rag' found"
fi

echo "🔧 Activating virtual environment..."
source city_clerk_rag/bin/activate

echo "🐍 Using Python: $(which python3)"
echo "📦 Python version: $(python3 --version)"

# Install/upgrade pip
echo "�� Upgrading pip..."
python3 -m pip install --upgrade pip

# Install base requirements
echo "📋 Installing base requirements..."
python3 -m pip install -r requirements.txt

# Install additional visualization dependencies using Python subprocess method
echo "🎨 Installing visualization dependencies..."
python3 -c "import subprocess; import sys; subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'dash>=2.14.0', 'plotly>=5.17.0', 'networkx>=3.2.0', 'dash-table>=5.0.0', 'pandas>=2.0.0', '--force-reinstall'])"

# Verify critical imports
echo "🔍 Verifying critical imports..."

# Test gremlin_python
if python3 -c "from gremlin_python.driver import client, serializer; print('✅ gremlinpython is working')" 2>/dev/null; then
    echo "✅ Gremlin Python imports successful"
else
    echo "❌ gremlinpython import failed, installing specific version..."
    python3 -m pip install "gremlinpython>=3.7.3"
fi

# Test dash and plotly
if python3 -c "import dash, plotly.graph_objects, networkx; print('✅ Visualization libraries working')" 2>/dev/null; then
    echo "✅ Dash and Plotly imports successful"
else
    echo "❌ Visualization library imports failed"
    exit 1
fi

# Test config import
if python3 -c "from config import validate_config; print('✅ Config module working')" 2>/dev/null; then
    echo "✅ Configuration imports successful"
else
    echo "❌ Configuration import failed"
    exit 1
fi

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  Warning: .env file not found!"
    echo "🔧 Creating template .env file..."
    cat > .env << EOF
# Azure Cosmos DB Gremlin Configuration
COSMOS_KEY=your_actual_cosmos_key_here
COSMOS_ENDPOINT=wss://aida-graph-db.gremlin.cosmos.azure.com:443
COSMOS_DATABASE=cgGraph
COSMOS_CONTAINER=cityClerk
COSMOS_PARTITION_KEY=partitionKey
COSMOS_PARTITION_VALUE=demo
EOF
    echo "📝 Template .env file created. Please edit it with your actual credentials before running again."
    echo "   Required: COSMOS_KEY=your_actual_cosmos_key_here"
    exit 1
else
    echo "✅ .env file found"
fi

# Test configuration
echo "🔧 Validating configuration..."
if python3 -c "from config import validate_config; exit(0 if validate_config() else 1)" 2>/dev/null; then
    echo "✅ Configuration validation successful"
else
    echo "❌ Configuration validation failed"
    echo "   Please check your .env file and ensure all required variables are set"
    exit 1
fi

# Check if the graph_visualizer.py file exists
if [ ! -f "graph_visualizer.py" ]; then
    echo "❌ graph_visualizer.py not found in current directory"
    exit 1
fi

echo ""
echo "🚀 Starting Graph Visualizer Web Application..."
echo "🌐 The application will be available at: http://localhost:8050"
echo "📱 Open your web browser and navigate to the URL above"
echo "🔄 Press Ctrl+C to stop the server"
echo ""

# Run the graph visualizer
python3 graph_visualizer.py 