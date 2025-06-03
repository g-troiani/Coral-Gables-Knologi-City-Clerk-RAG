#!/bin/bash
# Run City Clerk Graph Pipeline

echo "🚀 Starting City Clerk Graph Pipeline"
echo "=================================="

# Activate virtual environment if it exists
if [ -d "city_clerk_rag" ]; then
    echo "Activating virtual environment..."
    source city_clerk_rag/bin/activate
fi

# Run the pipeline
python3 scripts/graph_pipeline.py "$@"

echo "=================================="
echo "✅ Pipeline execution complete" 