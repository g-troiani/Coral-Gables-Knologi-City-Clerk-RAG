"""
Debug wrapper for UnifiedQueryEngine with comprehensive logging and data structure adaptation.
"""

import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

from .unified_query_engine import UnifiedQueryEngine

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

log = logging.getLogger(__name__)


class DebugQueryEngine(UnifiedQueryEngine):
    """Enhanced query engine with full debugging and logging capabilities."""
    
    def __init__(self, graph_dir: Path = Path("simple_ner_graph"), enable_debug: bool = True):
        """Initialize with debug mode."""
        super().__init__(graph_dir)
        self.enable_debug = enable_debug
        self.query_count = 0
        self.query_history = []
        
        if self.enable_debug:
            log.info("="*80)
            log.info("🔍 DEBUG MODE ENABLED - Full query tracing active")
            log.info("="*80)
            self._log_system_state()
    
    def _log_system_state(self):
        """Log current system state and configuration."""
        log.info("\n📊 SYSTEM STATE:")
        log.info(f"  - Graph directory: {self.graph_dir}")
        log.info(f"  - Entity index loaded: {len(self.entity_index)} categories")
        log.info(f"  - Chunk index loaded: {len(self.chunk_index)} chunks")
        log.info(f"  - Relationship index loaded: {len(self.relationship_index)} relationships")
        log.info(f"  - Agent planner available: {self.agent_planner is not None}")
        
        if hasattr(self, 'azure_search_client') and self.azure_search_client:
            log.info("  - Azure Search: ✅ Connected")
        else:
            log.info("  - Azure Search: ❌ Not available")
        
        if self.agent_planner and self.agent_planner.cosmos_client:
            log.info("  - Cosmos DB: ✅ Connected")
        else:
            log.info("  - Cosmos DB: ❌ Not available")
        
        log.info("-"*80)
    
    async def query(self, query_text: str, **kwargs) -> Dict[str, Any]:
        """Execute query with comprehensive debugging."""
        self.query_count += 1
        query_id = f"Q{self.query_count:04d}"
        start_time = time.time()
        
        # Log query start
        log.info("\n" + "="*80)
        log.info(f"🎯 QUERY {query_id} STARTED")
        log.info("="*80)
        log.info(f"📝 Query Text: {query_text}")
        log.info(f"⚙️ Options: {json.dumps(kwargs, indent=2)}")
        log.info("-"*80)
        
        try:
            # Call parent query method with debugging hooks
            if self.enable_debug:
                result = await self._debug_query_execution(query_text, kwargs)
            else:
                result = await super().query(query_text, **kwargs)
            
            # Process and adapt result for web app compatibility
            adapted_result = self._adapt_result_for_webapp(result)
            
            # Log query completion
            elapsed_time = time.time() - start_time
            log.info("\n" + "-"*80)
            log.info(f"✅ QUERY {query_id} COMPLETED in {elapsed_time:.2f}s")
            log.info(f"📊 Result Summary:")
            log.info(f"  - Answer length: {len(adapted_result.get('answer', ''))} chars")
            log.info(f"  - Retrieval method: {adapted_result.get('retrieval_method', 'unknown')}")
            log.info(f"  - Confidence: {adapted_result.get('confidence', 0):.2%}")
            log.info(f"  - Chunks returned: {len(adapted_result.get('chunks', []))}")
            log.info("="*80 + "\n")
            
            # Store in history
            self.query_history.append({
                'query_id': query_id,
                'query': query_text,
                'timestamp': datetime.now().isoformat(),
                'elapsed_time': elapsed_time,
                'result_summary': {
                    'method': adapted_result.get('retrieval_method'),
                    'confidence': adapted_result.get('confidence'),
                    'chunks_count': len(adapted_result.get('chunks', []))
                }
            })
            
            return adapted_result
            
        except Exception as e:
            elapsed_time = time.time() - start_time
            log.error("\n" + "-"*80)
            log.error(f"❌ QUERY {query_id} FAILED after {elapsed_time:.2f}s")
            log.error(f"Error: {str(e)}")
            log.error(f"Error Type: {type(e).__name__}")
            
            import traceback
            log.error("Traceback:")
            log.error(traceback.format_exc())
            log.error("="*80 + "\n")
            
            # Return error result in webapp-compatible format
            return {
                "answer": f"Error processing query: {str(e)}",
                "retrieval_method": "error",
                "error": str(e),
                "chunks": [],
                "confidence": 0.0
            }
    
    async def _debug_query_execution(self, query_text: str, kwargs: Dict) -> Dict[str, Any]:
        """Execute query with step-by-step debugging."""
        
        if not self.agent_planner:
            log.warning("⚠️ AgentQueryPlanner not initialized")
            return {
                "answer": "Query system is not available. Please check configuration.",
                "retrieval_method": "error",
                "error": "AgentQueryPlanner not initialized"
            }
        
        log.info("📍 Step 1: Query Classification")
        log.info("-"*40)
        
        # Classification with debugging
        query_type, confidence, entities = self.agent_planner.classifier.classify(query_text)
        
        log.info(f"  Query Type: {query_type.value}")
        log.info(f"  Confidence: {confidence:.2%}")
        log.info(f"  Entities Found: {len(entities)}")
        
        for i, entity in enumerate(entities, 1):
            log.info(f"    Entity {i}: {entity['type']} = '{entity['value']}'")
            if 'normalized' in entity:
                log.info(f"      Normalized: '{entity['normalized']}'")
            if 'subtype' in entity:
                log.info(f"      Subtype: {entity['subtype']}")
        
        log.info("\n📍 Step 2: Query Planning & Execution")
        log.info("-"*40)
        
        # Execute with agent planner
        result = await self.agent_planner.plan_and_execute(query_text)
        
        log.info(f"  Execution Path: {result.get('execution_path', 'unknown')}")
        log.info(f"  Query Type: {result.get('query_type', 'unknown')}")
        
        if 'metadata' in result:
            log.info("  Metadata:")
            for key, value in result['metadata'].items():
                if isinstance(value, (list, dict)):
                    log.info(f"    {key}: {len(value)} items")
                else:
                    log.info(f"    {key}: {value}")
        
        if 'hops' in result:
            log.info(f"\n📍 Step 3: Multi-hop Execution Details")
            log.info("-"*40)
            for hop in result['hops']:
                log.info(f"  Hop {hop['hop_number']}:")
                log.info(f"    Query: {hop['query']}")
                log.info(f"    Source: {hop['source_type']}")
                log.info(f"    Results: {len(hop.get('results', []))}")
                log.info(f"    Complete: {hop.get('is_complete', False)}")
        
        # Transform result
        return {
            "answer": result.get("answer", "No answer generated"),
            "retrieval_method": result.get("execution_path", "unknown"),
            "query_type": result.get("query_type", "unknown"),
            "confidence": result.get("confidence", 0.0),
            "metadata": result.get("metadata", {}),
            "sources_used": result.get("citations", [])
        }
    
    def _adapt_result_for_webapp(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Adapt query result to webapp-expected format with 'doc' key."""
        
        log.info("\n📍 Adapting Result for Web App")
        log.info("-"*40)
        
        # Create chunks array for webapp compatibility
        chunks = []
        
        # Extract chunks from metadata if available
        if 'metadata' in result:
            metadata = result['metadata']
            
            # Handle graph results
            if 'gremlin_query' in metadata:
                log.info(f"  Graph Query: {metadata['gremlin_query']}")
                log.info(f"  Graph Results: {metadata.get('result_count', 0)}")
            
            # Handle vector results
            if 'sources' in metadata:
                for source in metadata.get('sources', {}).get('vector_chunks', []):
                    chunk = {
                        'text': source.get('text', ''),
                        'similarity': source.get('similarity', 0),
                        # CRITICAL: Add 'doc' key for webapp compatibility
                        'doc': {
                            'title': 'Vector Search Result',
                            'source': 'Azure Cognitive Search'
                        }
                    }
                    chunks.append(chunk)
                    log.info(f"  Added vector chunk with similarity {chunk['similarity']:.2%}")
        
        # If no chunks but we have an answer, create a synthetic chunk
        if not chunks and result.get('answer'):
            chunks.append({
                'text': result['answer'][:500],  # Use part of answer as chunk
                'similarity': result.get('confidence', 0.5),
                'doc': {
                    'title': f"{result.get('query_type', 'Query')} Result",
                    'source': result.get('retrieval_method', 'GraphRAG')
                }
            })
            log.info("  Created synthetic chunk from answer")
        
        log.info(f"  Total chunks prepared: {len(chunks)}")
        
        # Build adapted result
        adapted_result = {
            **result,  # Keep all original fields
            'chunks': chunks  # Add webapp-compatible chunks
        }
        
        return adapted_result
    
    def get_debug_stats(self) -> Dict[str, Any]:
        """Get comprehensive debug statistics."""
        stats = super().get_system_stats()
        
        stats.update({
            'debug_mode': self.enable_debug,
            'queries_processed': self.query_count,
            'query_history': self.query_history[-10:],  # Last 10 queries
            'entity_categories': list(self.entity_index.keys()),
            'chunk_sample': list(self.chunk_index.keys())[:5] if self.chunk_index else []
        })
        
        return stats
    
    def export_debug_log(self, output_path: Path):
        """Export debug information to file."""
        debug_data = {
            'timestamp': datetime.now().isoformat(),
            'system_stats': self.get_debug_stats(),
            'query_history': self.query_history,
            'configuration': {
                'graph_dir': str(self.graph_dir),
                'enable_debug': self.enable_debug
            }
        }
        
        with open(output_path, 'w') as f:
            json.dump(debug_data, f, indent=2)
        
        log.info(f"📁 Debug log exported to: {output_path}")