import asyncio
import logging
from pathlib import Path
from scripts.graph_rag_stages.phase3_querying.ner import SimpleNERQueryEngine

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def test_queries():
    """Test both NER pipeline query and agent graph query."""
    
    # Initialize the SimpleNERQueryEngine
    log.info("Initializing SimpleNERQueryEngine...")
    simple_ner_engine = SimpleNERQueryEngine(Path("simple_ner_graph"))
    
    # Test queries
    test_cases = [
        "What ordinances were passed in 2014?",
        "Find all resolutions related to public works",
        "Show me agenda items from January 2024",
        "What was discussed in the most recent city council meeting?",
    ]
    
    # Test NER pipeline query
    log.info("\nTesting NER Pipeline Query:")
    for query in test_cases:
        log.info(f"\nExecuting NER query: {query}")
        try:
            result = await simple_ner_engine.query(query, top_k=5)
            log.info(f"Answer: {result.get('answer', 'No answer found')}")
        except Exception as e:
            log.error(f"Error executing NER query: {e}")
    
    # Test agent graph query
    log.info("\nTesting Agent Graph Query:")
    for query in test_cases:
        log.info(f"\nExecuting agent graph query: {query}")
        try:
            result = await simple_ner_engine.graph_query(query)
            log.info(f"Answer: {result.get('answer', 'No answer found')}")
        except Exception as e:
            log.error(f"Error executing agent graph query: {e}")

if __name__ == "__main__":
    asyncio.run(test_queries()) 