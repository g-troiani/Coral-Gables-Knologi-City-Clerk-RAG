#!/bin/bash

echo "Checking embedding configuration in settings.yaml:"
cat graphrag_data/settings.yaml | grep -A 10 -B 2 "embedding"

echo ""
echo "Checking GraphRAG logs for embedding errors:"

# Look for embedding-related errors
find graphrag_data -name "*.log" -exec grep -l "embedding" {} \; 2>/dev/null | head -5 | xargs grep -i "error\|fail\|warn" | grep -i "embed"

echo ""
echo "Checking indexing engine log:"
# Check indexing engine log
if [ -f graphrag_data/logs/indexing-engine.log ]; then
    tail -100 graphrag_data/logs/indexing-engine.log | grep -i "embed"
fi 