# Try using GraphRAG's Python API directly
import asyncio
from pathlib import Path

async def test_direct_query():
    # Import GraphRAG query module
    try:
        from graphrag.query.api import query
        
        result = await query(
            root_dir=Path("graphrag_data"),
            method="local",
            query="List all entities",
            reporter=None
        )
        print("Direct query result:", result)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

asyncio.run(test_direct_query()) 