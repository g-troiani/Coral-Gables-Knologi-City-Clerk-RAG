#!/usr/bin/env python3
import asyncio
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient

async def test_delete():
    print("TEST DELETE SCRIPT")
    cosmos_client = CosmosGraphClient()
    async with cosmos_client:
        # Get counts
        v_result = await cosmos_client._execute_query("g.V().count()")
        e_result = await cosmos_client._execute_query("g.E().count()")
        
        print(f"Raw v_result: {v_result}, type: {type(v_result)}")
        print(f"Raw e_result: {e_result}, type: {type(e_result)}")
        
        # Extract values from NESTED lists
        v_count = v_result[0][0] if v_result and v_result[0] else 0
        e_count = e_result[0][0] if e_result and e_result[0] else 0
        
        print(f"Extracted counts: {v_count} vertices, {e_count} edges")
        
        if v_count > 0 or e_count > 0:
            print("Calling clear_graph()...")
            await cosmos_client.clear_graph()

if __name__ == "__main__":
    asyncio.run(test_delete()) 