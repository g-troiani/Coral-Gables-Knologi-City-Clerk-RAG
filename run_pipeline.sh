#!/bin/bash

# This script is the main entry point for running the unified pipeline.
# It ensures the Python virtual environment is activated and executes the main orchestrator.
# Enhanced with logging functionality to capture pipeline runs.

# Navigate to the script's directory to ensure correct relative paths
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p logs

# Generate timestamp for log file
TIMESTAMP=$(date "+%Y-%m-%d_%H-%M-%S")
LOG_FILE="logs/pipeline_run_${TIMESTAMP}.md"
START_TIME=$(date "+%Y-%m-%d %H:%M:%S")

echo "📝 Logging pipeline run to: $LOG_FILE"

# Create markdown header for the log file
cat > "$LOG_FILE" << EOF
# Pipeline Run Log

**Date:** $(date "+%Y-%m-%d %H:%M:%S")  
**Log File:** \`$LOG_FILE\`  
**Working Directory:** \`$(pwd)\`  
**User:** \`$(whoami)\`  
**Arguments:** \`$@\`  

---

## Pipeline Output

\`\`\`
EOF

# Function to capture exit code and finalize log
finalize_log() {
    local exit_code=$1
    local end_time=$(date "+%Y-%m-%d %H:%M:%S")
    
    # Close the code block and add footer
    cat >> "$LOG_FILE" << EOF
\`\`\`

---

## Run Summary

- **Start Time:** $START_TIME
- **End Time:** $end_time
- **Exit Code:** $exit_code
- **Status:** $([ $exit_code -eq 0 ] && echo "✅ SUCCESS" || echo "❌ FAILED")

EOF
    
    echo ""
    echo "📋 Pipeline run logged to: $LOG_FILE"
    echo "📊 Exit code: $exit_code"
}

# Activate virtual environment if it exists
if [ -d "venv" ]; then
  echo "🐍 Activating Python virtual environment..." | tee -a "$LOG_FILE"
  source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "🐍 Activating Python virtual environment..." | tee -a "$LOG_FILE"
    source .venv/bin/activate
fi

# Run the main pipeline orchestrator, forwarding all script arguments
echo "🚀 Executing main pipeline..." | tee -a "$LOG_FILE"

# Capture both stdout and stderr, and get exit code
{
    python -m scripts.graph_rag_stages.main_pipeline "$@" 2>&1
    echo $? > /tmp/pipeline_exit_code_$$
} | tee -a "$LOG_FILE"

# Get the exit code
EXIT_CODE=$(cat /tmp/pipeline_exit_code_$$)
rm -f /tmp/pipeline_exit_code_$$

# Finalize the log file
finalize_log $EXIT_CODE

# Exit with the same code as the pipeline
exit $EXIT_CODE 