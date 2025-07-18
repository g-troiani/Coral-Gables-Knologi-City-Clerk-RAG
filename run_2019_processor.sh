#!/bin/bash

# run_2019_processor.sh
# Script to run the 2019 document processor in virtual environment (parallelized version)

echo "🚀 Starting 2019 Document Processor (Parallelized)..."

# Check if virtual environment exists
if [ ! -d "simple_ner_env" ]; then
    echo "❌ Virtual environment 'simple_ner_env' not found!"
    echo "Please create the virtual environment first."
    exit 1
fi

# Activate virtual environment
echo "📦 Activating virtual environment..."
source simple_ner_env/bin/activate

# Check if required packages are installed
echo "🔍 Checking required packages..."
python3 -c "import openai, dotenv, tqdm, asyncio; print('✅ Required packages available')" 2>/dev/null || {
    echo "❌ Missing required packages. Installing..."
    pip install openai python-dotenv tqdm
}

# Parse command line arguments
TEST_MODE=""
TEST_LIMIT=""
CONCURRENT=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            TEST_MODE="--test"
            shift
            ;;
        --test-limit)
            TEST_LIMIT="--test-limit $2"
            shift 2
            ;;
        --concurrent)
            CONCURRENT="--concurrent $2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --test              Run in test mode (process only a few files per folder)"
            echo "  --test-limit N      Number of files to process per folder in test mode (default: 3)"
            echo "  --concurrent N      Maximum concurrent API calls (default: 5)"
            echo "  -h, --help         Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run the processor with arguments
echo "🔄 Running parallelized 2019 document processor..."
if [ -n "$TEST_MODE" ]; then
    echo "🧪 Running in TEST MODE"
fi

python3 process_2019_documents.py $TEST_MODE $TEST_LIMIT $CONCURRENT

echo "✅ 2019 document processing completed!" 