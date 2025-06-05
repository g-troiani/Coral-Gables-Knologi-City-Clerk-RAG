#!/usr/bin/env python3
"""
Live GraphRAG Progress Monitor
Shows real-time progress and output file updates
"""
import time
import os
from pathlib import Path
from datetime import datetime

def monitor_live():
    print("🔍 GraphRAG Live Monitor")
    print("=" * 50)
    print("Press Ctrl+C to stop monitoring")
    print("=" * 50)
    
    last_sizes = {}
    
    try:
        while True:
            os.system('clear' if os.name == 'posix' else 'cls')
            
            print(f"🕐 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 50)
            
            # Check process status
            import subprocess
            try:
                result = subprocess.run(['pgrep', '-f', 'graphrag'], capture_output=True, text=True)
                if result.stdout.strip():
                    print("✅ GraphRAG process is RUNNING")
                    pids = result.stdout.strip().split('\n')
                    for pid in pids:
                        print(f"   PID: {pid}")
                else:
                    print("❌ GraphRAG process is NOT running")
            except:
                print("❓ Cannot check process status")
            
            print("\n📁 Output Files:")
            output_dir = Path("output")
            if output_dir.exists():
                files = list(output_dir.glob("*.parquet"))
                files.extend(output_dir.glob("*.json"))
                
                total_size = 0
                for file_path in sorted(files):
                    size = file_path.stat().st_size
                    total_size += size
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    # Check if file size changed
                    status = ""
                    if file_path.name in last_sizes:
                        if size > last_sizes[file_path.name]:
                            status = "📈 UPDATED"
                        elif size == last_sizes[file_path.name]:
                            status = "✅"
                    else:
                        status = "🆕 NEW"
                    
                    print(f"   {status} {file_path.name}: {size:,} bytes ({mod_time.strftime('%H:%M:%S')})")
                    last_sizes[file_path.name] = size
                
                print(f"\n📦 Total size: {total_size:,} bytes")
            else:
                print("   ❌ Output directory not found")
            
            # Show recent log entries
            print("\n📋 Recent Activity:")
            try:
                with open("graphrag_run.log", 'r') as f:
                    lines = f.readlines()
                    recent = lines[-3:] if len(lines) >= 3 else lines
                    for line in recent:
                        clean_line = line.strip()
                        if clean_line and "GraphRAG:" in clean_line:
                            # Extract just the GraphRAG part
                            graphrag_part = clean_line.split("GraphRAG: ", 1)[-1]
                            print(f"   {graphrag_part[:80]}{'...' if len(graphrag_part) > 80 else ''}")
            except:
                print("   Cannot read log file")
            
            print(f"\n🔄 Refreshing in 5 seconds... (Ctrl+C to stop)")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped.")

if __name__ == "__main__":
    monitor_live() 