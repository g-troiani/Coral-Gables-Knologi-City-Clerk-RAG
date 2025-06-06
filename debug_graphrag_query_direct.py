import subprocess
import os

# Set up environment
os.environ['GRAPHRAG_ROOT'] = 'graphrag_data'

# Run a simple query with verbose output
cmd = [
    'python3', '-m', 'graphrag', 'query',
    '--root', 'graphrag_data',
    '--method', 'local',
    '--query', 'Mayor Vince Lago',  # Use exact entity name
    '--verbose'
]

result = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr) 