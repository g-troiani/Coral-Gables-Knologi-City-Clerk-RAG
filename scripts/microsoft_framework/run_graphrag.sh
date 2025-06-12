#!/bin/bash

# City Clerk GraphRAG Pipeline Runner
# This script helps you set up and run the GraphRAG pipeline

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if virtual environment exists and activate it
setup_venv() {
    if [ ! -d "venv" ]; then
        print_status "Creating virtual environment..."
        python3 -m venv venv
    fi
    
    print_status "Activating virtual environment..."
    source venv/bin/activate
    
    print_status "Installing/upgrading dependencies..."
    pip install --upgrade pip
    pip install -r requirements.txt
}

# Function to check environment variables
check_env() {
    if [ -z "$OPENAI_API_KEY" ]; then
        print_error "OPENAI_API_KEY environment variable is not set"
        echo "Please set it with:"
        echo "export OPENAI_API_KEY='your-api-key-here'"
        echo ""
        echo "Or create a .env file with:"
        echo "OPENAI_API_KEY=your-api-key-here"
        exit 1
    fi
    
    print_success "Environment variables are set"
}

# Function to check for extracted documents
check_documents() {
    docs_dir="city_clerk_documents/extracted_text"
    
    if [ ! -d "$docs_dir" ] || [ -z "$(ls -A $docs_dir/*.json 2>/dev/null)" ]; then
        print_warning "No extracted documents found in $docs_dir"
        print_status "You need to run the document extraction pipeline first"
        echo ""
        echo "To extract documents, run:"
        echo "python3 scripts/graph_pipeline.py"
        echo ""
        read -p "Do you want to continue anyway? (y/N): " continue_anyway
        if [[ ! $continue_anyway =~ ^[Yy]$ ]]; then
            exit 1
        fi
    else
        doc_count=$(ls -1 $docs_dir/*.json 2>/dev/null | wc -l)
        print_success "Found $doc_count extracted documents"
    fi
}

# Function to run the main pipeline
run_pipeline() {
    print_status "Running GraphRAG pipeline..."
    python3 scripts/microsoft_framework/run_graphrag_pipeline.py
}

# Function to run interactive queries
run_queries() {
    print_status "Starting interactive query session..."
    python3 scripts/microsoft_framework/test_queries.py
}

# Function to show results
show_results() {
    output_dir="graphrag_data/output"
    
    if [ ! -d "$output_dir" ]; then
        print_error "GraphRAG output directory not found"
        print_status "Please run the pipeline first"
        return 1
    fi
    
    print_status "GraphRAG Results Summary"
    echo "=========================="
    
    # Check for output files
    for file in entities.parquet relationships.parquet communities.parquet community_reports.parquet; do
        file_path="$output_dir/$file"
        if [ -f "$file_path" ]; then
            size=$(du -h "$file_path" | cut -f1)
            print_success "$file ($size)"
        else
            print_warning "$file (not found)"
        fi
    done
    
    echo ""
    print_status "Files are located in: $output_dir"
    print_status "You can now run queries with: ./run_graphrag.sh query"
}

# Function to show usage
show_usage() {
    echo "🚀 City Clerk GraphRAG Pipeline Runner"
    echo ""
    echo "USAGE:"
    echo "  ./run_graphrag.sh [command]"
    echo ""
    echo "COMMANDS:"
    echo "  setup     - Set up virtual environment and install dependencies"
    echo "  run       - Run the complete GraphRAG pipeline (default)"
    echo "  query     - Start interactive query session"
    echo "  results   - Show results summary"
    echo "  clean     - Clean up GraphRAG data"
    echo "  help      - Show this help message"
    echo ""
    echo "EXAMPLES:"
    echo "  ./run_graphrag.sh setup    # Set up environment"
    echo "  ./run_graphrag.sh run      # Run full pipeline"
    echo "  ./run_graphrag.sh query    # Test queries interactively"
    echo ""
    echo "PREREQUISITES:"
    echo "  1. Set OPENAI_API_KEY environment variable"
    echo "  2. Have extracted city clerk documents in city_clerk_documents/extracted_text/"
    echo ""
}

# Function to clean up
clean_data() {
    if [ -d "graphrag_data" ]; then
        print_warning "This will delete all GraphRAG data"
        read -p "Are you sure? (y/N): " confirm
        if [[ $confirm =~ ^[Yy]$ ]]; then
            rm -rf graphrag_data
            print_success "GraphRAG data cleaned"
        fi
    else
        print_status "No GraphRAG data to clean"
    fi
}

# Main script logic
main() {
    # Change to script directory
    cd "$(dirname "$0")"
    
    case "${1:-run}" in
        "setup")
            print_status "Setting up GraphRAG environment..."
            setup_venv
            print_success "Setup complete!"
            ;;
        "run")
            print_status "Running complete GraphRAG pipeline..."
            setup_venv
            check_env
            check_documents
            run_pipeline
            ;;
        "query")
            print_status "Starting query session..."
            setup_venv
            run_queries
            ;;
        "results")
            show_results
            ;;
        "clean")
            clean_data
            ;;
        "help"|"-h"|"--help")
            show_usage
            ;;
        *)
            print_error "Unknown command: $1"
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 