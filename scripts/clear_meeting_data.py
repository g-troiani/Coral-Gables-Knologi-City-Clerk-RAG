#!/usr/bin/env python3
"""
Clear data for specific meetings without affecting the entire graph.
"""
import asyncio
import argparse
from pathlib import Path
from graph_stages.cosmos_db_client import CosmosGraphClient

async def clear_meeting_data(meeting_date: str):
    """Clear all data related to a specific meeting."""
    # Convert date format
    meeting_date_dashes = meeting_date.replace('.', '-')
    meeting_id = f"meeting-{meeting_date_dashes}"
    
    async with CosmosGraphClient() as cosmos:
        # First, get all connected nodes
        query = f"""
        g.V('{meeting_id}')
          .union(
            __.identity(),
            __.out('HAS_SECTION'),
            __.out('HAS_SECTION').out('CONTAINS_ITEM'),
            __.out('HAS_SECTION').out('CONTAINS_ITEM').out('REFERENCES_DOCUMENT')
          )
          .dedup()
          .id()
        """
        
        try:
            node_ids = await cosmos._execute_query(query)
            
            if node_ids:
                print(f"Found {len(node_ids)} nodes to delete for meeting {meeting_date}")
                
                # Delete all nodes (edges are automatically removed)
                for node_id in node_ids:
                    delete_query = f"g.V('{node_id}').drop()"
                    await cosmos._execute_query(delete_query)
                
                print(f"✅ Cleared all data for meeting {meeting_date}")
            else:
                print(f"No data found for meeting {meeting_date}")
                
        except Exception as e:
            print(f"❌ Error clearing meeting data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meeting_date", help="Meeting date (e.g., 01.09.2024)")
    args = parser.parse_args()
    
    asyncio.run(clear_meeting_data(args.meeting_date)) 