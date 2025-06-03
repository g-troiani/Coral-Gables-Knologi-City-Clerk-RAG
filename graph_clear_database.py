#!/usr/bin/env python3
"""
Clear Cosmos DB Graph Database
This script will clear all vertices and edges from the graph database.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add scripts directory to path
script_dir = Path(__file__).parent / 'scripts'
sys.path.append(str(script_dir))

from graph_stages.cosmos_db_client import CosmosGraphClient

async def clear_database():
    """Clear the entire graph database."""
    print('🗑️  Clearing Cosmos DB graph database...')
    print('⚠️  This will delete ALL vertices and edges!')
    
    try:
        async with CosmosGraphClient() as client:
            await client.clear_graph()
        print('✅ Graph database cleared successfully!')
        return True
        
    except Exception as e:
        print(f'❌ Error clearing database: {e}')
        return False

if __name__ == "__main__":
    success = asyncio.run(clear_database())
    if not success:
        sys.exit(1) 