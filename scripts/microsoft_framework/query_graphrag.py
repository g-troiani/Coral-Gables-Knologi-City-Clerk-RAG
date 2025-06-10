#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

# Use venv Python
venv_python = Path("venv/bin/python3")
if not venv_python.exists():
    print("Error: venv not found!")
    sys.exit(1)

# Get query from command line
query = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "Who is Mayor Lago?"

# Run query
cmd = [
    str(venv_python),
    "-m", "graphrag", "query",
    "--root", "graphrag_data",
    "--method", "local",
    "--query", query
]

result = subprocess.run(cmd, capture_output=True, text=True)
print(result.stdout) 