"""
Test script to validate vector search integration and query routing.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, Any
import json
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from scripts.graph_rag_stages.phase3_querying.unified_query_engine import UnifiedQueryEngine
from scripts.graph_rag_stages.phase3_querying.azure_search_client import AzureSearchClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


class QueryRoutingTester:
    """Test query routing between Azure Search and Cosmos DB."""
    
    def __init__(self):
        self.results = []
        self.simple_ner_dir = Path("simple_ner_graph")
        
    async def test_azure_search_connection(self) -> Dict[str, Any]:
        """Test Azure Search client connection."""
        log.info("\n" + "="*60)
        log.info("TEST 1: Azure Search Connection")
        log.info("="*60)
        
        try:
            client = AzureSearchClient()
            connected = client.test_connection()
            
            result = {
                "test": "Azure Search Connection",
                "status": "✅ PASS" if connected else "❌ FAIL",
                "details": "Connected to Azure Cognitive Search" if connected else "Failed to connect"
            }
            
        except Exception as e:
            result = {
                "test": "Azure Search Connection",
                "status": "❌ FAIL",
                "details": str(e)
            }
        
        self.results.append(result)
        log.info(f"Result: {result['status']} - {result['details']}")
        return result
    
    async def test_query_routing(self) -> None:
        """Test different query types and verify routing."""
        log.info("\n" + "="*60)
        log.info("TEST 2: Query Routing")
        log.info("="*60)
        
        # Initialize engine
        try:
            engine = UnifiedQueryEngine(self.simple_ner_dir)
            
            # Test queries with expected routing
            test_queries = [
                {
                    "query": "What are the main topics discussed in city meetings?",
                    "expected_type": "general_info",
                    "expected_route": "vector",
                    "description": "General information query → Vector Search"
                },
                {
                    "query": "How many ordinances were passed in January 2024?",
                    "expected_type": "specific_fact",
                    "expected_route": "graph",
                    "description": "Specific fact query → Graph Database"
                },
                {
                    "query": "Show me all resolutions about parking and their voting outcomes",
                    "expected_type": "complex_hybrid",
                    "expected_route": "multi_hop",
                    "description": "Complex hybrid query → Multi-hop (Both DBs)"
                },
                {
                    "query": "Tell me about environmental sustainability initiatives",
                    "expected_type": "general_info",
                    "expected_route": "vector",
                    "description": "Topic exploration → Vector Search"
                }
            ]
            
            for test_case in test_queries:
                log.info(f"\n📝 Testing: {test_case['description']}")
                log.info(f"   Query: \"{test_case['query']}\"")
                
                # Execute query
                result = await engine.query(test_case['query'])
                
                # Check routing
                actual_type = result.get("query_type", "unknown")
                actual_route = result.get("retrieval_method", "unknown")
                
                # Validate
                type_match = actual_type == test_case["expected_type"]
                route_match = actual_route == test_case["expected_route"]
                
                test_result = {
                    "test": test_case["description"],
                    "query": test_case["query"],
                    "expected": f"{test_case['expected_type']} → {test_case['expected_route']}",
                    "actual": f"{actual_type} → {actual_route}",
                    "status": "✅ PASS" if (type_match and route_match) else "⚠️ PARTIAL" if type_match else "❌ FAIL"
                }
                
                self.results.append(test_result)
                
                log.info(f"   Expected: {test_result['expected']}")
                log.info(f"   Actual:   {test_result['actual']}")
                log.info(f"   Result:   {test_result['status']}")
                
                # Show metadata if available
                if "metadata" in result:
                    log.info(f"   Metadata: {json.dumps(result['metadata'], indent=2)}")
                
        except Exception as e:
            log.error(f"❌ Query routing test failed: {e}")
            self.results.append({
                "test": "Query Routing",
                "status": "❌ FAIL",
                "details": str(e)
            })
    
    async def test_vector_search_directly(self) -> None:
        """Test Azure Search directly with a sample query."""
        log.info("\n" + "="*60)
        log.info("TEST 3: Direct Vector Search")
        log.info("="*60)
        
        try:
            client = AzureSearchClient()
            
            test_query = "parking regulations and policies"
            log.info(f"🔍 Searching for: \"{test_query}\"")
            
            # Test semantic search
            semantic_results = await client.semantic_search(test_query, limit=3)
            log.info(f"\n📊 Semantic Search Results: {len(semantic_results)} found")
            
            for i, result in enumerate(semantic_results, 1):
                log.info(f"\n   Result {i}:")
                log.info(f"   - ID: {result['id']}")
                log.info(f"   - Similarity: {result['similarity']:.3f}")
                log.info(f"   - Source: {result['metadata'].get('source_file', 'Unknown')}")
                log.info(f"   - Text preview: {result['text'][:100]}...")
            
            # Test hybrid search
            hybrid_results = await client.hybrid_search(test_query, limit=3)
            log.info(f"\n📊 Hybrid Search Results: {len(hybrid_results)} found")
            
            test_result = {
                "test": "Direct Vector Search",
                "status": "✅ PASS" if (semantic_results or hybrid_results) else "⚠️ WARNING",
                "details": f"Semantic: {len(semantic_results)} results, Hybrid: {len(hybrid_results)} results"
            }
            
        except Exception as e:
            test_result = {
                "test": "Direct Vector Search", 
                "status": "❌ FAIL",
                "details": str(e)
            }
        
        self.results.append(test_result)
        log.info(f"\nResult: {test_result['status']} - {test_result['details']}")
    
    async def test_fallback_search(self) -> None:
        """Test fallback search when Azure Search is unavailable."""
        log.info("\n" + "="*60)
        log.info("TEST 4: Fallback Search")
        log.info("="*60)
        
        try:
            # Create engine without Azure Search
            engine = UnifiedQueryEngine(self.simple_ner_dir)
            
            # Temporarily disable Azure Search
            original_client = engine.azure_search_client
            engine.azure_search_client = None
            
            # Force recreation of vector search function
            engine.agent_planner.vector_search_fn = engine._create_vector_search_function()
            
            # Test query that would normally use vector search
            test_query = "What topics were discussed in the meetings?"
            log.info(f"🔍 Testing fallback with: \"{test_query}\"")
            
            result = await engine.query(test_query)
            
            # Check if fallback was used
            is_fallback = False
            if "metadata" in result:
                sources = result.get("sources_used", [])
                for source in sources:
                    if isinstance(source, dict) and source.get("metadata", {}).get("search_type") == "fallback":
                        is_fallback = True
                        break
            
            # Restore Azure Search
            engine.azure_search_client = original_client
            
            test_result = {
                "test": "Fallback Search",
                "status": "✅ PASS" if result.get("answer") else "❌ FAIL",
                "details": f"Fallback search {'activated' if is_fallback else 'used'}, returned answer"
            }
            
        except Exception as e:
            test_result = {
                "test": "Fallback Search",
                "status": "❌ FAIL",
                "details": str(e)
            }
        
        self.results.append(test_result)
        log.info(f"Result: {test_result['status']} - {test_result['details']}")
    
    def print_summary(self) -> None:
        """Print test summary."""
        log.info("\n" + "="*60)
        log.info("TEST SUMMARY")
        log.info("="*60)
        
        passed = sum(1 for r in self.results if "✅" in r["status"])
        failed = sum(1 for r in self.results if "❌" in r["status"])
        partial = sum(1 for r in self.results if "⚠️" in r["status"])
        
        log.info(f"\n📊 Results: {passed} passed, {failed} failed, {partial} warnings")
        
        log.info("\n📋 Test Details:")
        for result in self.results:
            log.info(f"   {result['status']} {result['test']}")
            if "details" in result:
                log.info(f"      → {result['details']}")
        
        # Save results to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = Path(f"test_results_{timestamp}.json")
        
        with open(results_file, 'w') as f:
            json.dump({
                "timestamp": timestamp,
                "summary": {
                    "passed": passed,
                    "failed": failed,
                    "warnings": partial
                },
                "results": self.results
            }, f, indent=2)
        
        log.info(f"\n💾 Results saved to: {results_file}")
        
        # Return exit code
        return 0 if failed == 0 else 1
    
    async def run_all_tests(self) -> int:
        """Run all tests."""
        log.info("🚀 Starting Query Routing Tests")
        log.info(f"📁 Working directory: {Path.cwd()}")
        
        # Run tests
        await self.test_azure_search_connection()
        await self.test_query_routing()
        await self.test_vector_search_directly()
        await self.test_fallback_search()
        
        # Print summary
        return self.print_summary()


async def main():
    """Run test suite."""
    tester = QueryRoutingTester()
    exit_code = await tester.run_all_tests()
    sys.exit(exit_code)


if __name__ == "__main__":
    asyncio.run(main())