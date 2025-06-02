#!/usr/bin/env python3
# File: scripts/clear_database_sync.py

"""
Synchronous version of database clearing script.
"""

import os
from gremlin_python.driver import client, serializer
from dotenv import load_dotenv

load_dotenv()


def clear_database_sync():
    """Clear database using synchronous Gremlin client."""
    endpoint = os.getenv("COSMOS_ENDPOINT")
    key = os.getenv("COSMOS_KEY")
    database = os.getenv("COSMOS_DATABASE", "cgGraph")
    container = os.getenv("COSMOS_CONTAINER", "cityClerk")
    
    if not all([endpoint, key, database, container]):
        print("❌ Missing required Cosmos DB configuration")
        return False
    
    print('🗑️  Clearing Cosmos DB graph database...')
    print('⚠️  This will delete ALL vertices and edges!')
    
    # Create client
    gremlin_client = client.Client(
        f"{endpoint}/gremlin",
        "g",
        username=f"/dbs/{database}/colls/{container}",
        password=key,
        message_serializer=serializer.GraphSONSerializersV2d0()
    )
    
    try:
        # Clear all vertices (edges are automatically removed)
        print('🗑️  Dropping all vertices...')
        result = gremlin_client.submit("g.V().drop()")
        
        # Consume the result
        for _ in result:
            pass
        
        print('✅ Graph database cleared successfully!')
        return True
        
    except Exception as e:
        print(f'❌ Error clearing database: {e}')
        return False
        
    finally:
        # Clean up
        gremlin_client.close()


if __name__ == "__main__":
    success = clear_database_sync()
    if not success:
        exit(1) 