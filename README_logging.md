# Pipeline Logging System

This document describes the comprehensive logging system for the City Clerk Knowledge Graph Pipeline.

## Overview

The pipeline now automatically captures all terminal output for each run and saves it as structured markdown files in the `logs/` directory. This enables easy tracking, debugging, and analysis of pipeline runs.

## Files

### Core Scripts
- `run_pipeline.sh` - Enhanced pipeline runner with logging
- `manage_logs.sh` - Log management and viewing utility

### Log Storage
- `logs/` - Directory containing all pipeline run logs
- `logs/pipeline_run_YYYY-MM-DD_HH-MM-SS.md` - Individual log files

## Using the Pipeline with Logging

### Running the Pipeline
```bash
./run_pipeline.sh
```

The pipeline will automatically:
1. Create a timestamped log file in `logs/`
2. Display real-time output to terminal
3. Capture all stdout and stderr to the log file
4. Add metadata and run summary to the log

### Log File Structure

Each log file contains:
```markdown
# Pipeline Run Log

**Date:** 2025-07-14 10:30:39  
**Log File:** `logs/pipeline_run_2025-07-14_10-30-39.md`  
**Working Directory:** `/Users/username/project`  
**User:** `username`  
**Arguments:** `--option value`  

---

## Pipeline Output

```
[Full pipeline output here]
```

---

## Run Summary

- **Start Time:** 2025-07-14 10:30:39
- **End Time:** 2025-07-14 10:35:42
- **Exit Code:** 0
- **Status:** ✅ SUCCESS
```

## Log Management Commands

Use `./manage_logs.sh` with the following commands:

### List All Logs
```bash
./manage_logs.sh list
```
Shows all pipeline runs with timestamps, file sizes, and status.

### View Latest Log
```bash
./manage_logs.sh latest
```
Displays the most recent pipeline run log.

### View Specific Log
```bash
./manage_logs.sh view pipeline_run_2025-07-14_10-30-39.md
```
Shows the contents of a specific log file.

### Search Logs
```bash
./manage_logs.sh search "ERROR"
./manage_logs.sh search "✅ SUCCESS"
./manage_logs.sh search "Graph building completed"
```
Searches for patterns across all log files with line numbers.

### Show Failed Runs
```bash
./manage_logs.sh failed
```
Lists only pipeline runs that failed.

### View Statistics
```bash
./manage_logs.sh stats
```
Shows overall pipeline run statistics including success rate.

### Clean Up Old Logs
```bash
./manage_logs.sh cleanup        # Remove logs older than 30 days
./manage_logs.sh cleanup 7      # Remove logs older than 7 days
```

## Log File Naming Convention

Log files use the format: `pipeline_run_YYYY-MM-DD_HH-MM-SS.md`

Examples:
- `pipeline_run_2025-07-14_10-30-39.md`
- `pipeline_run_2025-07-14_15-45-22.md`

## Status Detection

The system automatically detects pipeline run status:
- ✅ **SUCCESS** - Pipeline completed with exit code 0
- ❌ **FAILED** - Pipeline completed with non-zero exit code  
- ❓ **UNKNOWN** - Pipeline was interrupted or didn't complete

## Benefits

1. **Complete Audit Trail** - Every pipeline run is documented
2. **Easy Debugging** - Full output captured with timestamps
3. **Historical Analysis** - Track success rates and performance over time
4. **Quick Access** - Simple commands to find and view specific runs
5. **Automated Cleanup** - Remove old logs to save disk space

## Integration with Development Workflow

### Debugging Failed Runs
```bash
./manage_logs.sh failed          # Find failed runs
./manage_logs.sh view <filename>  # View specific failure
./manage_logs.sh search "ERROR"   # Find error patterns
```

### Performance Monitoring
```bash
./manage_logs.sh stats           # Check success rates
./manage_logs.sh search "completed" # Find completion times
```

### Regular Maintenance
```bash
./manage_logs.sh cleanup 14      # Keep 2 weeks of logs
```

## Advanced Usage

### Custom Search Patterns
```bash
./manage_logs.sh search "Graph building"
./manage_logs.sh search "263 nodes"
./manage_logs.sh search "LLM corrections"
./manage_logs.sh search "Cosmos DB"
```

### Viewing Logs in External Tools
Since logs are markdown files, you can:
- View them in any markdown editor
- Use `cat`, `less`, or `more` to view in terminal
- Open in VS Code or other editors for syntax highlighting
- Process with markdown tools for reporting

## Troubleshooting

### Log File Not Created
- Check that `logs/` directory exists
- Verify script permissions: `chmod +x run_pipeline.sh`
- Ensure sufficient disk space

### Incomplete Log Files
- Pipeline may have been interrupted (Ctrl+C)
- Check system resources (memory, disk space)
- Look for background processes

### Status Shows as "UNKNOWN"
- Pipeline didn't complete normally
- Check the end of the log file for errors
- May indicate system interruption or crash

## File Locations

```
graph_database/
├── run_pipeline.sh          # Enhanced pipeline runner
├── manage_logs.sh           # Log management utility
├── logs/                    # Log storage directory
│   ├── pipeline_run_2025-07-14_10-30-39.md
│   ├── pipeline_run_2025-07-14_11-15-42.md
│   └── ...
└── README_logging.md        # This documentation
``` 