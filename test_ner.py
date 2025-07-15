# test_ner.py
import asyncio
import json
from pathlib import Path
from scripts.graph_rag_stages.phase2_building.ner.ner_extractor import NERExtractor

async def test_single_chunk():
    extractor = NERExtractor(Path("simple_ner_graph"))
    
    # Read a chunk
    chunk_files = list(Path("simple_ner_graph/document_chunks/").glob("*.txt"))
    if not chunk_files:
        print("No chunk files found!")
        return
    
    chunk_file = chunk_files[0]  # Use first available chunk
    print(f"Testing chunk: {chunk_file.name}")
    
    with open(chunk_file, 'r') as f:
        content = f.read()
    
    # Extract chunk text (skip metadata header)
    if "---" in content:
        _, chunk_text = content.split("---", 1)
        chunk_text = chunk_text.strip()
    else:
        chunk_text = content
    
    # Extract entities
    entities = await extractor._extract_entities_llm(chunk_text)
    
    print(json.dumps(entities, indent=2))

if __name__ == "__main__":
    asyncio.run(test_single_chunk()) 