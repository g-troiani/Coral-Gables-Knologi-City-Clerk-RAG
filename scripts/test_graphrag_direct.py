#!/usr/bin/env python3
"""Test GraphRAG queries directly."""

import subprocess
import sys
from pathlib import Path

def test_query(query: str):
    """Test a query directly."""
    
    graphrag_root = Path("graphrag_data")
    
    cmd = [
        sys.executable, "-m", "graphrag", "query",
        "--root", str(graphrag_root),
        "--method", "local",
        "--query", query
    ]
    
    print(f"\n🔍 Query: {query}")
    print("-" * 40)
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    print("STDOUT:")
    print(result.stdout)
    print("\nSTDERR:")
    print(result.stderr)
    
    # Also try with different methods
    for method in ["global", "drift"]:
        cmd[-3] = method
        print(f"\n🔄 Trying with {method} method...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.stdout.strip():
            print(f"✅ Got result with {method}")
            print(result.stdout[:500])
            break

if __name__ == "__main__":
    test_query("What is agenda item E-1?")
    test_query("Tell me about E-1")
    test_query("What is ordinance 2024-01?") 