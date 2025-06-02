#!/usr/bin/env python3
# File: scripts/clear_database.py

"""
Clear Cosmos DB Graph Database
This script will clear all vertices and edges from the graph database.
"""

import asyncio
import sys
import os
from pathlib import Path

# Add scripts directory to path
script_dir = Path(__file__).parent
sys.path.append(str(script_dir))

from graph_stages.cosmos_db_client import CosmosGraphClient


async def clear_database():
    """Clear the entire graph database."""
    print('🗑️  Clearing Cosmos DB graph database...')
    print('⚠️  This will delete ALL vertices and edges!')
    
    client = None
    try:
        # Create and connect client
        client = CosmosGraphClient()
        await client.connect()
        
        print('🗑️  Clearing entire graph...')
        await client.clear_graph()
        
        print('✅ Graph database cleared successfully!')
        return True
        
    except Exception as e:
        print(f'❌ Error clearing database: {e}')
        return False
    
    finally:
        # Ensure proper cleanup
        if client:
            await client.close()


def main():
    """Main entry point with proper async handling."""
    # Create new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Run the async function
        success = loop.run_until_complete(clear_database())
        
        # Exit with appropriate code
        if not success:
            sys.exit(1)
            
    finally:
        # Clean up the loop
        loop.close()


if __name__ == "__main__":
    main() 