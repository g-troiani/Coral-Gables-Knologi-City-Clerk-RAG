#!/bin/bash

# Pipeline Log Management Script
# Helps view, search, and manage pipeline run logs

LOGS_DIR="logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

show_help() {
    echo "Pipeline Log Management Script"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  list                 List all pipeline run logs"
    echo "  latest               Show the most recent log"
    echo "  view <filename>      View a specific log file"
    echo "  search <pattern>     Search for pattern in all logs"
    echo "  failed               Show logs of failed pipeline runs"
    echo "  cleanup [days]       Remove logs older than X days (default: 30)"
    echo "  stats                Show statistics about pipeline runs"
    echo "  help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 list"
    echo "  $0 latest"
    echo "  $0 view pipeline_run_2024-01-15_10-30-00.md"
    echo "  $0 search \"ERROR\""
    echo "  $0 cleanup 7"
}

list_logs() {
    if [ ! -d "$LOGS_DIR" ]; then
        echo -e "${RED}No logs directory found${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Pipeline Run Logs:${NC}"
    echo "====================="
    
    local count=0
    for log in "$LOGS_DIR"/pipeline_run_*.md; do
        if [ -f "$log" ]; then
            local filename=$(basename "$log")
            local size=$(ls -lh "$log" | awk '{print $5}')
            local date=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$log" 2>/dev/null || stat -c "%y" "$log" 2>/dev/null | cut -d' ' -f1-2)
            
            # Check if it was successful or failed
            local status=""
            if grep -q "✅ SUCCESS" "$log" 2>/dev/null; then
                status="${GREEN}SUCCESS${NC}"
            elif grep -q "❌ FAILED" "$log" 2>/dev/null; then
                status="${RED}FAILED${NC}"
            else
                status="${YELLOW}UNKNOWN${NC}"
            fi
            
            printf "%-40s %s %8s [%s]\n" "$filename" "$date" "$size" "$status"
            ((count++))
        fi
    done
    
    if [ $count -eq 0 ]; then
        echo -e "${YELLOW}No pipeline logs found${NC}"
    else
        echo ""
        echo -e "${BLUE}Total logs: $count${NC}"
    fi
}

show_latest() {
    local latest=$(ls -t "$LOGS_DIR"/pipeline_run_*.md 2>/dev/null | head -n1)
    
    if [ -z "$latest" ]; then
        echo -e "${RED}No pipeline logs found${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Latest Pipeline Log: $(basename "$latest")${NC}"
    echo "==============================================="
    cat "$latest"
}

view_log() {
    local filename="$1"
    
    if [ -z "$filename" ]; then
        echo -e "${RED}Please specify a log filename${NC}"
        return 1
    fi
    
    local filepath="$LOGS_DIR/$filename"
    
    if [ ! -f "$filepath" ]; then
        echo -e "${RED}Log file not found: $filepath${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Viewing: $filename${NC}"
    echo "================================"
    cat "$filepath"
}

search_logs() {
    local pattern="$1"
    
    if [ -z "$pattern" ]; then
        echo -e "${RED}Please specify a search pattern${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Searching for: '$pattern'${NC}"
    echo "============================"
    
    grep -n --color=always "$pattern" "$LOGS_DIR"/pipeline_run_*.md 2>/dev/null || {
        echo -e "${YELLOW}No matches found${NC}"
        return 1
    }
}

show_failed() {
    echo -e "${RED}Failed Pipeline Runs:${NC}"
    echo "===================="
    
    local found=false
    for log in "$LOGS_DIR"/pipeline_run_*.md; do
        if [ -f "$log" ] && grep -q "❌ FAILED" "$log" 2>/dev/null; then
            local filename=$(basename "$log")
            local date=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" "$log" 2>/dev/null || stat -c "%y" "$log" 2>/dev/null | cut -d' ' -f1-2)
            echo "📁 $filename ($date)"
            found=true
        fi
    done
    
    if [ "$found" = false ]; then
        echo -e "${GREEN}No failed pipeline runs found! 🎉${NC}"
    fi
}

cleanup_logs() {
    local days=${1:-30}
    
    echo -e "${YELLOW}Cleaning up logs older than $days days...${NC}"
    
    local count=0
    find "$LOGS_DIR" -name "pipeline_run_*.md" -type f -mtime +$days -print0 | while IFS= read -r -d '' file; do
        echo "Removing: $(basename "$file")"
        rm "$file"
        ((count++))
    done
    
    echo -e "${GREEN}Cleanup completed${NC}"
}

show_stats() {
    if [ ! -d "$LOGS_DIR" ]; then
        echo -e "${RED}No logs directory found${NC}"
        return 1
    fi
    
    echo -e "${BLUE}Pipeline Run Statistics:${NC}"
    echo "======================="
    
    local total=0
    local success=0
    local failed=0
    local unknown=0
    
    for log in "$LOGS_DIR"/pipeline_run_*.md; do
        if [ -f "$log" ]; then
            ((total++))
            if grep -q "✅ SUCCESS" "$log" 2>/dev/null; then
                ((success++))
            elif grep -q "❌ FAILED" "$log" 2>/dev/null; then
                ((failed++))
            else
                ((unknown++))
            fi
        fi
    done
    
    echo "📊 Total runs: $total"
    echo -e "✅ Successful: ${GREEN}$success${NC}"
    echo -e "❌ Failed: ${RED}$failed${NC}"
    echo -e "❓ Unknown: ${YELLOW}$unknown${NC}"
    
    if [ $total -gt 0 ]; then
        local success_rate=$((success * 100 / total))
        echo ""
        echo -e "📈 Success rate: ${GREEN}${success_rate}%${NC}"
    fi
}

# Main script logic
case "$1" in
    "list")
        list_logs
        ;;
    "latest")
        show_latest
        ;;
    "view")
        view_log "$2"
        ;;
    "search")
        search_logs "$2"
        ;;
    "failed")
        show_failed
        ;;
    "cleanup")
        cleanup_logs "$2"
        ;;
    "stats")
        show_stats
        ;;
    "help"|"")
        show_help
        ;;
    *)
        echo -e "${RED}Unknown command: $1${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac 