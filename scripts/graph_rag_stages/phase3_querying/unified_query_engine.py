"""
Unified Query Engine - Replacement for SimpleNERQueryEngine using AgentQueryPlanner.

This provides a drop-in replacement that preserves the same interface while using
the sophisticated graph_agent_query system underneath.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from scripts.graph_rag_stages.phase1_preprocessing.ner.markdown_chunker import MarkdownChunker
from .graph_agent_query import AgentQueryPlanner

log = logging.getLogger(__name__)


class UnifiedQueryEngine:
    """
    Unified query engine that replaces SimpleNERQueryEngine.
    Uses AgentQueryPlanner for sophisticated query processing while preserving
    the same interface and data infrastructure.
    """
    

    def __init__(self, graph_dir: Path = Path("simple_ner_graph")):
        """Initialize the unified query engine."""
        self.graph_dir = Path(graph_dir)
        self.graph_dir.mkdir(parents=True, exist_ok=True)
        self.chunks_dir = self.graph_dir / "document_chunks"
        
        # Use unified ontology categories
        self.entity_categories = UnifiedOntology.get_entity_categories()
        
        # Load indices (preserve existing data infrastructure)
        self._load_indices()
        
        # Initialize AgentQueryPlanner
        self._initialize_agent_query_planner()
        
    def _load_indices(self):
        """Load all data indices from existing NER pipeline data."""
        # Load entity index (check both indices subdir and root)
        entity_index_path = self.graph_dir / "indices" / "entity_index.json"
        if not entity_index_path.exists():
            entity_index_path = self.graph_dir / "entity_index.json"
        
        if entity_index_path.exists():
            self.entity_index = json.loads(entity_index_path.read_text())
            log.info(f"✅ Entity index loaded: {self.entity_index.get('total_count', 0)} entities")
        else:
            self.entity_index = {}
            log.warning("Entity index not found")
        
        # Load chunk index (check both indices subdir and root)
        chunk_index_path = self.graph_dir / "indices" / "chunk_index.json"
        if not chunk_index_path.exists():
            chunk_index_path = self.graph_dir / "chunk_index.json"
            
        if chunk_index_path.exists():
            self.chunk_index = json.loads(chunk_index_path.read_text())
            log.info(f"✅ Chunk index loaded: {len(self.chunk_index)} chunks")
        else:
            self.chunk_index = {}
            log.warning("Chunk index not found")
        
        # Load relationship index (check both indices subdir and root)
        relationship_index_path = self.graph_dir / "indices" / "relationship_index.json"
        if not relationship_index_path.exists():
            relationship_index_path = self.graph_dir / "relationship_index.json"
            
        if relationship_index_path.exists():
            self.relationship_index = json.loads(relationship_index_path.read_text())
            log.info(f"✅ Relationship index loaded: {len(self.relationship_index)} relationships")
        else:
            self.relationship_index = {}
            log.warning("Relationship index not found")
        
        # Load bidirectional relationship index
        bidirectional_path = self.graph_dir / "bidirectional_relationship_index.json"
        if bidirectional_path.exists():
            self.bidirectional_relationship_index = json.loads(bidirectional_path.read_text())
        else:
            self.bidirectional_relationship_index = {}
    
    def _initialize_agent_query_planner(self):
        """Initialize the AgentQueryPlanner with available data sources."""
        
        # Initialize Azure Search client first
        self._initialize_azure_search()
        
        try:
            # Initialize Cosmos client
            cosmos_client = None
            cosmos_endpoint = os.getenv("COSMOS_ENDPOINT", "").strip()
            cosmos_key = os.getenv("COSMOS_KEY", "").strip()
            
            if cosmos_endpoint and cosmos_key:
                from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
                cosmos_client = CosmosGraphClient()
                log.info("✅ Cosmos DB client initialized")
            else:
                log.warning("⚠️ Cosmos DB credentials not found, graph queries unavailable")
            
            # Create vector search function
            vector_search_fn = self._create_vector_search_function()
            
            # Initialize AgentQueryPlanner
            self.agent_planner = AgentQueryPlanner(
                cosmos_client=cosmos_client,
                vector_search_fn=vector_search_fn
            )
            
            log.info("✅ AgentQueryPlanner initialized successfully")
            
        except Exception as e:
            log.warning(f"⚠️ Failed to initialize AgentQueryPlanner: {e}")
            self.agent_planner = None

    def _initialize_azure_search(self):
        """Initialize Azure Search client for vector queries."""
        try:
            from .azure_search_client import AzureSearchClient
            
            self.azure_search_client = AzureSearchClient()
            
            # Test connection
            if self.azure_search_client.test_connection():
                log.info("✅ Azure Search client connected successfully")
            else:
                log.warning("⚠️ Azure Search connection test failed")
                self.azure_search_client = None
                
        except ValueError as e:
            log.warning(f"⚠️ Azure Search not configured: {e}")
            self.azure_search_client = None
        except Exception as e:
            log.error(f"❌ Failed to initialize Azure Search: {e}")
            self.azure_search_client = None
    
    def _create_vector_search_function(self):
        """Create a vector search function using Azure Cognitive Search."""
        
        # Check if Azure Search is available
        if not hasattr(self, 'azure_search_client') or not self.azure_search_client:
            log.warning("Azure Search not available, using fallback local search")
            return self._create_fallback_search_function()
        
        def vector_search(query: str, limit: int = 10) -> List[Dict[str, Any]]:
            """Execute vector search using Azure Cognitive Search."""
            try:
                # Use synchronous Azure Search client instead
                from azure.search.documents import SearchClient
                from azure.core.credentials import AzureKeyCredential
                
                # Create synchronous client
                sync_client = SearchClient(
                    endpoint=self.azure_search_client.search_endpoint,
                    index_name=self.azure_search_client.index_name,
                    credential=AzureKeyCredential(self.azure_search_client.search_key)
                )
                
                # Execute search synchronously
                results = sync_client.search(
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
                            'search_type': 'azure_search'
                        }
                    })
                
                log.info(f"Azure Search returned {len(formatted_results)} results")
                return formatted_results
                
            except Exception as e:
                log.error(f"Azure Search query failed: {e}")
                # Fall back to local search
                return self._execute_fallback_search(query, limit)
        
        return vector_search

    def _create_fallback_search_function(self):
        """Create fallback search using local chunk index."""
        
        def fallback_search(query: str, limit: int = 10) -> List[Dict[str, Any]]:
            """Fallback to local text matching when Azure Search unavailable."""
            if not self.chunk_index:
                return []
            
            results = []
            query_lower = query.lower()
            query_words = set(query_lower.split())
            
            # Score each chunk based on word matches
            scored_chunks = []
            for chunk_id, chunk_data in self.chunk_index.items():
                chunk_text = chunk_data.get('text', '').lower()
                chunk_words = set(chunk_text.split())
                
                # Calculate similarity based on word overlap
                common_words = query_words & chunk_words
                if common_words:
                    similarity = len(common_words) / len(query_words)
                    scored_chunks.append({
                        'id': chunk_id,
                        'text': chunk_data.get('text', ''),
                        'similarity': min(similarity, 1.0),
                        'metadata': {
                            'source_file': chunk_data.get('Source_File_Name', ''),
                            'document_type': chunk_data.get('document_type', ''),
                            'meeting_date': chunk_data.get('meeting_date', ''),
                            'search_type': 'fallback'
                        }
                    })
            
            # Sort by similarity and return top results
            scored_chunks.sort(key=lambda x: x['similarity'], reverse=True)
            return scored_chunks[:limit]
        
        return fallback_search

    def _execute_fallback_search(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """Execute fallback search when Azure Search fails."""
        log.info("Using fallback local search")
        fallback_fn = self._create_fallback_search_function()
        return fallback_fn(query, limit)
    
    async def initialize_pipeline(
        self, 
        markdown_source_dir: Path, 
        chunk_size: int = 1000, 
        chunk_overlap: int = 100, 
        use_integrated_pipeline: bool = True, 
        phase1_entities: Optional[List] = None,
        persist_to_disk: bool = True,
        skip_internal_graph_build: bool = True,
    ):
        """
        Runs chunking + NER extraction. Optionally persists results to disk.
        """
        try:
            # Step 1: Chunk documents
            log.info("📄 Chunking documents...")
            chunker = MarkdownChunker(self.graph_dir, chunk_size, chunk_overlap)
            chunk_count = await chunker.process_directory(markdown_source_dir)
            if chunk_count == 0:
                log.warning("No chunks created")
                return
        except Exception as e:
            log.error(f"Chunking failed: {e}")
            return
        
        # Store extraction results for persistence
        chunks = []
        extracted_entities = []
        relationships = []
        entity_count = 0
        
        # Optional: chunk-only mode to avoid double extraction when using external NER
        chunk_only = os.getenv("NER_CHUNK_ONLY", "true").lower() in ("1", "true", "yes")
        if not chunk_only:
            try:
                # Step 2: Extract entities using integrated pipeline
                log.info("🔍 Extracting entities...")
                if use_integrated_pipeline:
                    log.warning("Integrated pipeline was removed. Falling back to standard enhanced extractor...")
                    use_integrated_pipeline = False
                
                log.info("Using standard enhanced extractor...")
                from scripts.graph_rag_stages.phase2_building.ner.phase2_new_extractor import Phase2NEWExtractor
                extractor = Phase2NEWExtractor(self.graph_dir)
                entity_count = await extractor.run_all()
            except Exception as e:
                log.error(f"Entity extraction failed: {e}")
                return

        # Collect results for persistence
        if persist_to_disk:
            # Load chunks from the chunks directory
            chunks_dir = self.graph_dir / "document_chunks"
            if chunks_dir.exists():
                for chunk_file in chunks_dir.glob("*.txt"):
                    chunks.append({
                        "id": chunk_file.stem,
                        "content": chunk_file.read_text(encoding='utf-8'),
                        "source": str(chunk_file)
                    })
            
            if not chunk_only:
                # Load extracted entities from entity directories
                for entity_dir in self.graph_dir.iterdir():
                    if entity_dir.is_dir() and entity_dir.name not in {"document_chunks", "registry", "relationships", "merged"}:
                        for entity_file in entity_dir.glob("*.json"):
                            try:
                                entity_data = json.loads(entity_file.read_text(encoding='utf-8'))
                                extracted_entities.append(entity_data)
                            except Exception as e:
                                log.warning(f"Could not load entity file {entity_file}: {e}")
                
                # Load relationships if they exist
                relationships_file = self.graph_dir / "relationships" / "relationships.jsonl"
                if relationships_file.exists():
                    try:
                        for line in relationships_file.read_text(encoding='utf-8').strip().split('\n'):
                            if line.strip():
                                relationships.append(json.loads(line))
                    except Exception as e:
                        log.warning(f"Could not load relationships: {e}")
        
        # Persist results to disk if requested
        if persist_to_disk:
            from .ner.io_writer import SimpleNERWriter
            writer = SimpleNERWriter(self.graph_dir)
            writer.write_chunks(chunks)
            writer.write_entities(extracted_entities)
            writer.write_relationships(relationships)
            # helpful log for Stage 3.5
            by_type = {}
            for e in extracted_entities:
                t = (e.get("type") if isinstance(e, dict) else getattr(e, "type", "Unknown")) or "Unknown"
                by_type[t] = by_type.get(t, 0) + 1
            log.info("💾 NER persisted: chunks=%s, entities=%s (by type=%s), rels=%s",
                     len(chunks), len(extracted_entities), by_type, len(relationships or []))

        # IMPORTANT: do NOT build any graph here - graph building now happens in Stage 5
        if not skip_internal_graph_build:
            log.warning("Internal graph building is deprecated and skipped - graph building now happens in Stage 5 via CustomGraphBuilder")
        
        # Reload indices after processing
        self._load_indices()
        
        # Reinitialize AgentQueryPlanner with updated data
        self._initialize_agent_query_planner()
        
        log.info(f"✅ Pipeline initialization completed. Processed {entity_count} entities.")
    
    async def query(self, query_text: str, **kwargs) -> Dict[str, Any]:
        """
        Execute a query using the AgentQueryPlanner.
        Preserves the same interface as SimpleNERQueryEngine.query().
        """
        if not self.agent_planner:
            return {
                "answer": "Query system is not available. Please check configuration.",
                "retrieval_method": "error",
                "error": "AgentQueryPlanner not initialized"
            }
        
        try:
            log.info(f"🔍 Processing query: {query_text}")
            
            # Use AgentQueryPlanner for sophisticated query processing
            result = await self.agent_planner.plan_and_execute(query_text)
            
            # Transform result to match expected interface
            return {
                "answer": result.get("answer", "No answer generated"),
                "retrieval_method": result.get("execution_path", "unknown"),
                "query_type": result.get("query_type", "unknown"), 
                "confidence": result.get("confidence", 0.0),
                "metadata": result.get("metadata", {}),
                "sources_used": result.get("citations", [])
            }
            
        except Exception as e:
            log.error(f"Query execution failed: {e}")
            return {
                "answer": f"Error processing query: {str(e)}",
                "retrieval_method": "error",
                "error": str(e)
            }
    
    def get_system_stats(self) -> Dict[str, Any]:
        """
        Get system statistics.
        Preserves the same interface as SimpleNERQueryEngine.get_system_stats().
        """
        return {
            "entities_count": len(self.entity_index),
            "relationships_count": len(self.relationship_index),
            "chunks_count": len(self.chunk_index),
            "data_directory": str(self.graph_dir),
            "agent_planner_available": self.agent_planner is not None
        }
    
