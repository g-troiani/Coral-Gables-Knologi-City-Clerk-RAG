#!/bin/bash
# Run Single Meeting Graph Viewer

echo "🚀 Starting Single Meeting Graph Viewer..."
echo "📅 This viewer focuses on visualizing one meeting day at a time"
echo "=================================================="

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "❌ Virtual environment not found. Please run: python3 -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Activate virtual environment and run the viewer
source venv/bin/activate && python3 single_meeting_graph_viewer.py 