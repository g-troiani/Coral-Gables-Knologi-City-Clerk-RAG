#!/bin/bash
# Convenient wrapper for running the graph pipeline scripts

# Set the correct Python path
export PYTHONPATH=scripts

# Function to run graph pipeline
run_pipeline() {
    python3 scripts/graph_pipeline.py "$@"
}

# Function to run database clear
run_clear() {
    python3 scripts/clear_database.py "$@"
}

# Check which script to run based on first argument
case "$1" in
    "pipeline")
        shift
        run_pipeline "$@"
        ;;
    "clear")
        shift
        run_clear "$@"
        ;;
    *)
        echo "Usage: $0 {pipeline|clear} [options]"
        echo ""
        echo "Examples:"
        echo "  $0 pipeline --help"
        echo "  $0 pipeline --date '01.15.2024'"
        echo "  $0 clear --help"
        echo "  $0 clear --cosmos"
        exit 1
        ;;
esac 