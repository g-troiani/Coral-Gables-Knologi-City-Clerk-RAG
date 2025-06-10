#!/usr/bin/env python3
"""Run GraphRAG commands directly without subprocess."""

import sys
import os

def run_graphrag_index(root_dir, verbose=True):
    """Run GraphRAG indexing directly."""
    # Set up arguments
    sys.argv = [
        'graphrag', 'index',
        '--root', str(root_dir)
    ]
    if verbose:
        sys.argv.append('--verbose')
    
    # Import and run GraphRAG
    try:
        from graphrag.cli import app
        app()
    except ImportError:
        print("❌ GraphRAG not found in current environment")
        print(f"Python: {sys.executable}")
        print(f"Path: {sys.path}")
        raise

if __name__ == "__main__":
    if len(sys.argv) > 1:
        root = sys.argv[1]
    else:
        root = "graphrag_data"
    
    run_graphrag_index(root) 