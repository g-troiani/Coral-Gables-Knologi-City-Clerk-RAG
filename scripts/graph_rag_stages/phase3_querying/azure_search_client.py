"""
Azure Cognitive Search client for vector and semantic search queries.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from azure.search.documents import SearchClient
from azure.search.documents.models import VectorizedQuery, QueryType
from azure.core.credentials import AzureKeyCredential
from openai import AzureOpenAI

log = logging.getLogger(__name__)


class AzureSearchClient:
    """Client for querying Azure Cognitive Search with vector/semantic capabilities."""
    
    def __init__(self):
        """Initialize Azure Search client with credentials from environment."""
        
        # Azure Cognitive Search configuration
        self.search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT", "").strip()
        self.search_key = os.getenv("VECTOR_DATABASE_KEY", "").strip()
        self.index_name = os.getenv("VECTOR_DATABASE_NAME", "city-clerk-rag").strip()
        
        # Validate configuration
        if not self.search_endpoint or not self.search_key:
            raise ValueError(
                "Azure Search credentials missing. Required environment variables:\n"
                "- AZURE_SEARCH_ENDPOINT\n"
                "- VECTOR_DATABASE_KEY"
            )
        
        # Initialize search client
        self.search_client = SearchClient(
            endpoint=self.search_endpoint,
            index_name=self.index_name,
            credential=AzureKeyCredential(self.search_key)
        )
        
        # Initialize OpenAI for query embeddings
        self.openai_client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        
        self.embeddings_model = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT", "text-embedding-ada-002")
        
        log.info(f"✅ Azure Search client initialized for index: {self.index_name}")
    
    async def generate_query_embedding(self, query: str) -> List[float]:
        """Generate embedding vector for search query."""
        try:
            response = self.openai_client.embeddings.create(
                model=self.embeddings_model,
                input=query
            )
            return response.data[0].embedding
        except Exception as e:
            log.error(f"Failed to generate query embedding: {e}")
            raise
    
    async def semantic_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute semantic search using embeddings.
        
        Args:
            query: Natural language query
            limit: Maximum results to return
            
        Returns:
            List of results with similarity scores
        """
        try:
            # Generate query embedding
            query_vector = await self.generate_query_embedding(query)
            
            # Create vector query
            vector_query = VectorizedQuery(
                vector=query_vector,
                k_nearest_neighbors=limit,
                fields="vector"
            )
            
            # Execute search
            results = self.search_client.search(
                search_text=None,  # Pure vector search
                vector_queries=[vector_query],
                select=["chunkKey", "sourceDocument", "content", "documentType", "Date"],
                top=limit
            )
            
            # Format results
            formatted_results = []
            for result in results:
                formatted_results.append({
                    'id': result.get('chunkKey', ''),
                    'text': result.get('content', ''),
                    'similarity': result.get('@search.score', 0.0) / 100.0,  # Normalize to 0-1
                    'metadata': {
                        'source_file': result.get('sourceDocument', ''),
                        'document_type': result.get('documentType', ''),
                        'meeting_date': result.get('Date', '')
                    }
                })
            
            log.info(f"Semantic search returned {len(formatted_results)} results")
            return formatted_results
            
        except Exception as e:
            log.error(f"Semantic search failed: {e}")
            return []
    
    async def hybrid_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute hybrid search combining vector and keyword search.
        
        Args:
            query: Natural language query
            limit: Maximum results to return
            
        Returns:
            List of results with combined scores
        """
        try:
            # Generate query embedding
            query_vector = await self.generate_query_embedding(query)
            
            # Create vector query
            vector_query = VectorizedQuery(
                vector=query_vector,
                k_nearest_neighbors=limit,
                fields="vector"
            )
            
            # Execute hybrid search (text + vector)
            results = self.search_client.search(
                search_text=query,  # Also use text search
                vector_queries=[vector_query],
                query_type=QueryType.SEMANTIC,  # Enable semantic reranking if available
                semantic_configuration_name="sem",  # Use the semantic config from schema
                select=["chunkKey", "sourceDocument", "content", "documentType", "Date"],
                top=limit
            )
            
            # Format results
            formatted_results = []
            for result in results:
                # Extract semantic caption if available
                captions = getattr(result, '@search.captions', [])
                caption_text = captions[0].text if captions else None
                
                formatted_results.append({
                    'id': result.get('chunkKey', ''),
                    'text': caption_text or result.get('content', ''),  # Use caption if available
                    'similarity': result.get('@search.score', 0.0) / 100.0,  # Normalize
                    'metadata': {
                        'source_file': result.get('sourceDocument', ''),
                        'document_type': result.get('documentType', ''),
                        'meeting_date': result.get('Date', ''),
                        'search_type': 'hybrid'
                    }
                })
            
            log.info(f"Hybrid search returned {len(formatted_results)} results")
            return formatted_results
            
        except Exception as e:
            log.error(f"Hybrid search failed: {e}")
            return []
    
    async def keyword_search(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Execute simple keyword search without embeddings.
        
        Args:
            query: Search terms
            limit: Maximum results to return
            
        Returns:
            List of results
        """
        try:
            # Execute keyword search
            results = self.search_client.search(
                search_text=query,
                select=["chunkKey", "sourceDocument", "content", "documentType", "Date"],
                top=limit
            )
            
            # Format results
            formatted_results = []
            for result in results:
                formatted_results.append({
                    'id': result.get('chunkKey', ''),
                    'text': result.get('content', ''),
                    'similarity': result.get('@search.score', 0.0) / 100.0,
                    'metadata': {
                        'source_file': result.get('sourceDocument', ''),
                        'document_type': result.get('documentType', ''),
                        'meeting_date': result.get('Date', ''),
                        'search_type': 'keyword'
                    }
                })
            
            log.info(f"Keyword search returned {len(formatted_results)} results")
            return formatted_results
            
        except Exception as e:
            log.error(f"Keyword search failed: {e}")
            return []
    
    def test_connection(self) -> bool:
        """Test if Azure Search connection is working."""
        try:
            # Try to get document count
            result = self.search_client.search(
                search_text="*",
                top=1,
                include_total_count=True
            )
            
            # Get total count
            count = result.get_count()
            log.info(f"✅ Azure Search connection successful. Index contains {count} documents.")
            return True
            
        except Exception as e:
            log.error(f"❌ Azure Search connection failed: {e}")
            return False