#!/usr/bin/env python3
"""
Quick status checker for GraphRAG process
"""
import os
import time
from pathlib import Path
from datetime import datetime

def check_status():
    base_dir = Path("/Users/gianmariatroiani/Documents/knologi/graph_database/graphrag_data")
    logs_dir = base_dir / "logs"
    output_dir = base_dir / "output"
    
    print(f"🔍 GraphRAG Status Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Check if process is running
    import subprocess
    try:
        result = subprocess.run(['pgrep', '-f', 'monitor_graphrag.py'], 
                              capture_output=True, text=True)
        if result.stdout.strip():
            print("✅ GraphRAG monitor process is RUNNING")
            print(f"   Process ID: {result.stdout.strip()}")
        else:
            print("❌ GraphRAG monitor process is NOT running")
    except:
        print("❓ Cannot determine process status")
    
    # Check latest monitor log
    if logs_dir.exists():
        monitor_logs = list(logs_dir.glob("graphrag_monitor_*.log"))
        if monitor_logs:
            latest_log = max(monitor_logs, key=lambda x: x.stat().st_mtime)
            print(f"📋 Latest monitor log: {latest_log.name}")
            
            # Show last few lines
            try:
                with open(latest_log, 'r') as f:
                    lines = f.readlines()
                    if lines:
                        print("📖 Last 3 log entries:")
                        for line in lines[-3:]:
                            print(f"   {line.strip()}")
            except:
                pass
    
    # Check output files
    print(f"\n📁 Output Files Status:")
    expected_files = [
        "entities.parquet", "relationships.parquet", "communities.parquet",
        "community_reports.parquet", "text_units.parquet"
    ]
    
    if output_dir.exists():
        for file_name in expected_files:
            file_path = output_dir / file_name
            if file_path.exists():
                size = file_path.stat().st_size
                mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                print(f"   ✅ {file_name}: {size:,} bytes (modified: {mod_time.strftime('%H:%M:%S')})")
            else:
                print(f"   ⏳ {file_name}: Not created yet")
    else:
        print("   ❌ Output directory doesn't exist yet")
    
    # Check GraphRAG engine log
    engine_log = logs_dir / "indexing-engine.log"
    if engine_log.exists():
        mod_time = datetime.fromtimestamp(engine_log.stat().st_mtime)
        print(f"\n📊 GraphRAG engine log last updated: {mod_time.strftime('%H:%M:%S')}")
        
        # Check for errors in recent lines
        try:
            with open(engine_log, 'r') as f:
                lines = f.readlines()
                recent_lines = lines[-10:] if len(lines) > 10 else lines
                errors = [line for line in recent_lines if 'ERROR' in line.upper() or 'FAILED' in line.upper()]
                if errors:
                    print("⚠️  Recent errors found:")
                    for error in errors[-2:]:  # Show last 2 errors
                        print(f"   {error.strip()}")
                else:
                    print("✅ No recent errors detected")
        except:
            pass
    
    print("\n" + "=" * 60)
    print("💡 To check status again, run: python3 check_status.py")
    print("🛑 To stop the process: pkill -f monitor_graphrag.py")

if __name__ == "__main__":
    check_status() 