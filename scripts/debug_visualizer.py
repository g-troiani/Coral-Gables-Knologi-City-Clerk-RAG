#!/usr/bin/env python3
"""
Debug script to check visualizer data loading
"""

import sys
import asyncio
import json
sys.path.append('scripts')

from graph_stages.cosmos_db_client import CosmosGraphClient

async def debug_visualizer_data():
    """Debug what data the visualizer should be loading."""
    
    client = CosmosGraphClient()
    
    try:
        print("🔍 Debugging visualizer data loading...")
        
        # Check what node labels exist
        print("\n1. Checking node labels in database:")
        labels_result = await client._execute_query("g.V().label().dedup()")
        print(f"   Node labels found: {labels_result}")
        
        # Check specific counts by label
        for label in labels_result:
            count_result = await client._execute_query(f"g.V().hasLabel('{label}').count()")
            count = count_result[0] if count_result else 0
            print(f"   {label}: {count} nodes")
        
        # Test the exact query the visualizer uses
        print("\n2. Testing visualizer query:")
        visualizer_query = "g.V().valueMap(true)"
        vertices = await client._execute_query(visualizer_query)
        print(f"   Query returned {len(vertices)} vertices")
        
        # Show sample vertex data
        if vertices:
            print("\n3. Sample vertex data (first 3):")
            for i, vertex in enumerate(vertices[:3]):
                print(f"   Vertex {i+1}:")
                print(f"     ID: {vertex.get('id')}")
                print(f"     Label: {vertex.get('label')}")
                print(f"     Properties: {list(vertex.keys())}")
                
                # Show specific properties for agenda items
                if vertex.get('label') == 'AgendaItem':
                    code = vertex.get('code', ['Unknown'])[0] if isinstance(vertex.get('code'), list) else vertex.get('code', 'Unknown')
                    title = vertex.get('title', ['Unknown'])[0] if isinstance(vertex.get('title'), list) else vertex.get('title', 'Unknown')
                    print(f"     Code: {code}")
                    print(f"     Title: {title[:50]}...")
        
        # Check edges
        print("\n4. Testing edge query:")
        edge_query = """
            g.E().project('source','target','label')
            .by(outV().id())
            .by(inV().id())
            .by(label())
        """
        edges = await client._execute_query(edge_query)
        print(f"   Query returned {len(edges)} edges")
        
        if edges:
            print("   Sample edges (first 3):")
            for i, edge in enumerate(edges[:3]):
                print(f"     Edge {i+1}: {edge['source']} -[{edge['label']}]-> {edge['target']}")
        
        # Check if there are Meeting nodes specifically
        print("\n5. Checking for Meeting nodes:")
        meeting_result = await client._execute_query("g.V().hasLabel('Meeting').valueMap(true)")
        print(f"   Found {len(meeting_result)} Meeting nodes")
        if meeting_result:
            for i, meeting in enumerate(meeting_result):
                date = meeting.get('date', ['Unknown'])[0] if isinstance(meeting.get('date'), list) else meeting.get('date', 'Unknown')
                print(f"     Meeting {i+1}: {date}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(debug_visualizer_data()) 