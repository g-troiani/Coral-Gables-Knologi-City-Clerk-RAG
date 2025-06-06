#!/bin/bash

# Run query test with explicit venv Python
cd /Users/gianmariatroiani/Documents/knologi/graph_database
source venv/bin/activate
python3 -m graphrag query --root graphrag_data --method local --query "Mayor Vince Lago" 