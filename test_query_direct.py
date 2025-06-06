import subprocess
import sys
import os
from pathlib import Path

# Ensure we use the venv Python
venv_python = Path("venv/bin/python3")
if not venv_python.exists():
    print(f"Error: {venv_python} not found!")
    sys.exit(1)

# Test query
cmd = [
    str(venv_python),
    "-m", "graphrag", "query",
    "--root", "graphrag_data",
    "--method", "local",
    "--query", "Mayor Vince Lago"
]

print(f"Running: {' '.join(cmd)}")
result = subprocess.run(cmd, capture_output=True, text=True)

print("\n=== STDOUT ===")
print(result.stdout)
print("\n=== STDERR ===")
print(result.stderr) 