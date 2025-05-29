#!/usr/bin/env python3
"""Test script for the graph pipeline."""
import asyncio
import logging
from pathlib import Path
from graph_pipeline import GraphPipeline

logging.basicConfig(level=logging.INFO)

async def test_single_date():
    """Test processing a single date."""
    base_dir = Path("city_clerk_documents/global/City Commissions 2024")
    
    if not base_dir.exists():
        print(f"Error: Base directory not found: {base_dir}")
        return
    
    pipeline = GraphPipeline(base_dir)
    
    # Test with a specific date
    test_date = "06.11.2024"
    
    print(f"Testing pipeline with date: {test_date}")
    await pipeline.initialize()
    
    try:
        await pipeline.process_batch(test_date)
        print("Test completed successfully!")
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await pipeline.cosmos_client.close()

if __name__ == "__main__":
    asyncio.run(test_single_date()) 