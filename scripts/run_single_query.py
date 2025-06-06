import asyncio
from pathlib import Path
import sys

# Add project root to path to allow imports
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.graphrag_breakdown import CityClerkQueryEngine

async def main(query: str):
    """Runs a single query against the GraphRAG engine."""
    print(f"Asking GraphRAG: {query}")
    print("-" * 30)
    
    engine = CityClerkQueryEngine(project_root / 'graphrag_data')
    result = await engine.query(query)
    
    print("\\n--> Answer from GraphRAG:")
    print(result['answer'])
    print("\\n" + "=" * 50)
    print("Source Documents:")
    if result.get('source_documents'):
        for doc in result['source_documents']:
            print(f"- {doc['document_id']}")
    else:
        print("- No source documents found in the response.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        user_query = " ".join(sys.argv[1:])
        asyncio.run(main(user_query))
    else:
        print("Usage: python3 run_single_query.py 'Your question here'") 