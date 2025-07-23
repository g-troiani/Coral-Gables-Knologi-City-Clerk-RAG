# debug_env.py
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env file
load_dotenv()

print("🔍 Checking environment variables:\n")

# Check for all possible Cosmos/Gremlin related variables
possible_vars = [
    'COSMOS_ENDPOINT',
    'COSMOS_KEY',
    'COSMOS_DATABASE',
    'COSMOS_CONTAINER',
    'GREMLIN_ENDPOINT',
    'COSMOS_GREMLIN_ENDPOINT',
    'AZURE_COSMOS_ENDPOINT',
    'COSMOS_DB_ENDPOINT',
    'COSMOS_DB_KEY',
    'COSMOS_CONNECTION_STRING'
]

print("Environment variables found:")
for var in possible_vars:
    value = os.getenv(var)
    if value:
        # Mask sensitive data
        if 'KEY' in var or 'CONNECTION' in var:
            print(f"  {var}: ***SET*** (hidden)")
        else:
            print(f"  {var}: {value[:50]}..." if len(value) > 50 else f"  {var}: {value}")

print("\n📄 All COSMOS/GREMLIN related variables:")
for key, value in os.environ.items():
    if 'COSMOS' in key.upper() or 'GREMLIN' in key.upper():
        if 'KEY' in key or 'PASSWORD' in key:
            print(f"  {key}: ***SET***")
        else:
            print(f"  {key}: {value[:50]}..." if len(value) > 50 else f"  {key}: {value}") 