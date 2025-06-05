#!/usr/bin/env python3
"""
Test script for querying the city clerk system
Demonstrates both graph database and RAG query capabilities
"""

import os
import sys
import asyncio
from dotenv import load_dotenv

# Add the current directory to Python path
sys.path.append('.')
sys.path.append('scripts')

load_dotenv()

class QueryTester:
    """Test both graph and RAG query systems"""
    
    def __init__(self):
        self.graph_client = None
        self.rag_available = False
        
    async def setup_graph_client(self):
        """Setup graph database client"""
        try:
            from scripts.graph_stages.cosmos_db_client import CosmosGraphClient
            self.graph_client = CosmosGraphClient()
            print("✅ Graph database client connected")
        except Exception as e:
            print(f"❌ Failed to connect to graph database: {e}")
            
    def setup_rag_client(self):
        """Check RAG system availability"""
        try:
            # Check if required environment variables are set
            required_vars = ['OPENAI_API_KEY', 'SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY']
            missing = [var for var in required_vars if not os.getenv(var)]
            
            if missing:
                print(f"❌ RAG system missing: {', '.join(missing)}")
                return False
                
            print("✅ RAG system environment variables found")
            self.rag_available = True
            return True
        except Exception as e:
            print(f"❌ RAG system check failed: {e}")
            return False
    
    async def test_graph_queries(self):
        """Test various graph database queries"""
        if not self.graph_client:
            print("⚠️  Graph client not available - skipping graph tests")
            return
            
        print("\n🔍 Testing Graph Database Queries")
        print("=" * 50)
        
        # Test queries
        test_queries = [
            ("Count all vertices", "g.V().count()"),
            ("Count all edges", "g.E().count()"),
            ("List vertex types", "g.V().label().dedup()"),
            ("List edge types", "g.E().label().dedup()"),
            ("Sample meetings", "g.V().hasLabel('Meeting').limit(3).valueMap()"),
            ("Sample agenda items", "g.V().hasLabel('AgendaItem').limit(3).valueMap()"),
            ("Find people", "g.V().hasLabel('Person').limit(5).values('name')"),
            ("Meeting relationships", "g.V().hasLabel('Meeting').out().label().dedup()"),
        ]
        
        for description, query in test_queries:
            try:
                print(f"\n📊 {description}:")
                result = await self.graph_client._execute_query(query)
                
                if isinstance(result, list) and len(result) > 0:
                    if len(result) == 1 and isinstance(result[0], (int, float)):
                        print(f"   Result: {result[0]}")
                    else:
                        print(f"   Found {len(result)} items:")
                        for i, item in enumerate(result[:5]):  # Show first 5
                            if isinstance(item, dict):
                                # Show key-value pairs for complex objects
                                key_items = list(item.items())[:3]  # First 3 keys
                                print(f"     {i+1}. {dict(key_items)}")
                            else:
                                print(f"     {i+1}. {item}")
                        if len(result) > 5:
                            print(f"     ... and {len(result) - 5} more")
                else:
                    print("   No results found")
                    
            except Exception as e:
                print(f"   ❌ Query failed: {e}")
    
    def test_rag_queries(self):
        """Test RAG system queries"""
        if not self.rag_available:
            print("⚠️  RAG system not available - skipping RAG tests")
            return
            
        print("\n🤖 Testing RAG Query System")
        print("=" * 50)
        
        try:
            import requests
            import json
            
            # Test queries for city clerk documents
            test_questions = [
                "What meetings were held recently?",
                "What resolutions were passed?", 
                "Who are the city commissioners?",
                "What contracts were approved?",
                "What ordinances were discussed?",
            ]
            
            # Test if RAG server is running
            try:
                response = requests.get("http://localhost:8080/stats", timeout=5)
                if response.status_code == 200:
                    stats = response.json()
                    print(f"📈 RAG Server Status: {stats}")
                    
                    # Test actual queries
                    for question in test_questions:
                        print(f"\n❓ Question: {question}")
                        try:
                            search_response = requests.post(
                                "http://localhost:8080/search",
                                json={"query": question},
                                timeout=10
                            )
                            
                            if search_response.status_code == 200:
                                result = search_response.json()
                                answer = result.get('answer', 'No answer')
                                citations = result.get('citations', [])
                                
                                print(f"   💬 Answer: {answer[:200]}{'...' if len(answer) > 200 else ''}")
                                print(f"   📚 Citations: {len(citations)} found")
                            else:
                                print(f"   ❌ Query failed: {search_response.status_code}")
                                
                        except requests.exceptions.Timeout:
                            print("   ⏰ Query timeout")
                        except Exception as e:
                            print(f"   ❌ Query error: {e}")
                            
                else:
                    print(f"❌ RAG server not responding: {response.status_code}")
                    
            except requests.exceptions.ConnectionError:
                print("❌ RAG server not running at localhost:8080")
                print("💡 To start RAG server: python3 scripts/rag_local_web_app.py")
                
        except ImportError:
            print("❌ Missing 'requests' package for RAG testing")
            print("💡 Install with: pip install requests")
    
    def show_manual_test_instructions(self):
        """Show instructions for manual testing"""
        print("\n📋 Manual Testing Options")
        print("=" * 50)
        
        print("\n🔍 Graph Database Testing:")
        print("1. Visual Explorer:")
        print("   python3 graph_visualizer.py")
        print("   Then visit: http://localhost:8050")
        
        print("\n2. Direct Graph Queries:")
        print("   python3 scripts/check_graph_empty.py")
        print("   python3 scripts/test_graph_urls.py")
        
        print("\n🤖 RAG System Testing:")
        print("1. Start RAG Server:")
        print("   python3 scripts/rag_local_web_app.py")
        print("   Then visit: http://localhost:8080")
        
        print("\n2. Example RAG Questions:")
        questions = [
            "What is the city budget for this year?",
            "Who are the current city commissioners?", 
            "What ordinances were passed recently?",
            "What contracts need approval?",
            "When is the next city council meeting?"
        ]
        for i, q in enumerate(questions, 1):
            print(f"   {i}. {q}")
            
        print("\n🔧 System Status Commands:")
        print("   python3 -c 'from config import validate_config; validate_config()'")
        print("   python3 scripts/check_graph_empty.py")
    
    async def run_all_tests(self):
        """Run all available tests"""
        print("🧪 City Clerk Query System Tests")
        print("=" * 50)
        
        # Setup
        await self.setup_graph_client()
        self.setup_rag_client()
        
        # Run tests
        await self.test_graph_queries()
        self.test_rag_queries()
        
        # Show manual options
        self.show_manual_test_instructions()
        
        # Cleanup
        if self.graph_client:
            await self.graph_client.close()

async def main():
    """Main test runner"""
    tester = QueryTester()
    await tester.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main()) 