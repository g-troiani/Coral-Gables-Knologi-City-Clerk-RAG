import pandas as pd
import subprocess
import os
import asyncio
from pathlib import Path

print("GraphRAG Query System Debug")
print("=" * 50)

# 1. Test if we can query entities directly
print("\n1. Testing direct entity queries...")
entities = pd.read_parquet('graphrag_data/output/entities.parquet')

# Check for mayor-related entities
mayor_entities = entities[entities['title'].str.contains('MAYOR', case=False, na=False)]
print(f"Mayor entities: {len(mayor_entities)}")
print(mayor_entities[['title', 'type', 'description']].head())

# Check for E-1
e1_entities = entities[entities['title'].str.contains('E-1', case=False, na=False) | 
                       entities['description'].str.contains('E-1', case=False, na=False)]
print(f"\nE-1 related entities: {len(e1_entities)}")
if len(e1_entities) > 0:
    print(e1_entities[['title', 'type', 'description']].head())

# 2. Check vector store configuration
print("\n\n2. Checking vector store configuration...")
lancedb_path = 'graphrag_data/output/lancedb'
if os.path.exists(lancedb_path):
    print(f"LanceDB contents:")
    for item in os.listdir(lancedb_path):
        print(f"  - {item}")
else:
    print("LanceDB directory doesn't exist!")

# 3. Test GraphRAG query directly with verbose output
print("\n\n3. Testing GraphRAG query with verbose output...")
os.environ['GRAPHRAG_ROOT'] = 'graphrag_data'

cmd = [
    'python3', '-m', 'graphrag', 'query',
    '--root', 'graphrag_data',
    '--method', 'local',
    '--query', 'Mayor Vince Lago',  # Use exact entity name
    '--verbose'
]

result = subprocess.run(cmd, capture_output=True, text=True)
print("STDOUT:", result.stdout)
print("STDERR:", result.stderr)

# 4. Try using GraphRAG's Python API directly
print("\n\n4. Testing GraphRAG Python API directly...")

async def test_direct_query():
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

print("\n" + "=" * 50)
print("GraphRAG Query System Debug Complete") 