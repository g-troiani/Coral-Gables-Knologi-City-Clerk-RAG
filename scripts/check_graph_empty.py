#!/usr/bin/env python3
"""
Check if the graph database is empty
"""

import sys
import asyncio
sys.path.append('scripts')

from graph_stages.cosmos_db_client import CosmosGraphClient

async def check_graph_empty():
    """Check if the graph database is empty."""
    
    # Connect to graph
    client = CosmosGraphClient()
    
    try:
        print("🔍 Checking graph database status...")
        
        # Count all vertices
        vertex_result = await client._execute_query("g.V().count()")
        vertex_count = vertex_result[0] if vertex_result else 0
        
        # Count all edges
        edge_result = await client._execute_query("g.E().count()")
        edge_count = edge_result[0] if edge_result else 0
        
        print(f"📊 Graph Database Status:")
        print(f"   🔵 Vertices: {vertex_count}")
        print(f"   🔗 Edges: {edge_count}")
        
        if vertex_count == 0 and edge_count == 0:
            print("✅ Graph database is completely empty!")
        else:
            print("⚠️  Graph database still contains data:")
            
            if vertex_count > 0:
                # Show some sample vertices
                samples = await client._execute_query("g.V().limit(5).valueMap()")
                print(f"   Sample vertices: {len(samples)} found")
                for i, sample in enumerate(samples[:3]):
                    print(f"     {i+1}. {sample}")
                
            if edge_count > 0:
                # Show some sample edges
                edge_samples = await client._execute_query("g.E().limit(5).valueMap()")
                print(f"   Sample edges: {len(edge_samples)} found")
        
    except Exception as e:
        print(f"❌ Error checking graph: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(check_graph_empty()) 