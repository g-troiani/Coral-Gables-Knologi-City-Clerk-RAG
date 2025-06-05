#!/usr/bin/env python3
"""
Quick test to verify URLs are stored in graph database
"""

import sys
import asyncio
import json
sys.path.append('scripts')

from graph_stages.cosmos_db_client import CosmosGraphClient

async def test_graph_urls():
    """Test that URLs are stored in the graph database."""
    
    # Connect to graph
    client = CosmosGraphClient()
    
    try:
        # Query for E-1 item
        query = "g.V().hasLabel('AgendaItem').has('code', 'E-1').limit(1)"
        result = await client.query_vertices(query)
        
        if result:
            node = result[0]
            print(f"✅ Found E-1 node: {node.get('id')}")
            print(f"📋 Node properties: {list(node.keys())}")
            
            # Check for URLs
            if 'urls_json' in node:
                urls_json = node['urls_json'][0]['value']
                urls = json.loads(urls_json)
                print(f"🔗 URLs found: {len(urls)}")
                for url in urls:
                    print(f"   - {url.get('url', 'No URL')}")
                    print(f"     Text: {url.get('text', 'No text')}")
                    print(f"     Page: {url.get('page', 'No page')}")
            else:
                print("❌ No urls_json property found")
                
            # Check has_urls flag
            if 'has_urls' in node:
                has_urls = node['has_urls'][0]['value']
                print(f"🏷️  Has URLs flag: {has_urls}")
            else:
                print("❌ No has_urls property found")
        else:
            print("❌ E-1 node not found")
            
        # Query for all agenda items with URLs
        query = "g.V().hasLabel('AgendaItem').has('has_urls', true).count()"
        count_result = await client.query_vertices(query)
        if count_result:
            count = count_result[0]
            print(f"\n📊 Total agenda items with URLs: {count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(test_graph_urls()) 