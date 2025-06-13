"""
The core query engine that processes user requests and generates responses using GraphRAG.
"""

import logging
import subprocess
import sys
from pathlib import Path
from typing import Dict, Any, Optional, List
import json
import pandas as pd

from .query_router import QueryRouter
from .response_enhancer import ResponseEnhancer
from .source_tracker import SourceTracker

log = logging.getLogger(__name__)


class QueryEngine:
    """Handles the end-to-end process of answering user queries using GraphRAG."""

    def __init__(self, graphrag_root: Path):
        self.graphrag_root = graphrag_root
        self.output_dir = graphrag_root / "output"
        
        # Initialize components
        self.router = QueryRouter()
        self.enhancer = ResponseEnhancer()
        self.source_tracker = SourceTracker()
        
        # Determine if deduplicated data exists and should be used
        dedup_dir = self.output_dir / "deduplicated"
        self.data_root = dedup_dir if dedup_dir.exists() else self.output_dir

    def validate_graphrag_output(self) -> bool:
        """Validate that GraphRAG output exists and is usable."""
        required_files = [
            "create_final_entities.parquet",
            "create_final_relationships.parquet",
            "create_final_communities.parquet"
        ]
        
        for file_name in required_files:
            file_path = self.data_root / file_name
            if not file_path.exists():
                log.warning(f"Missing required GraphRAG output file: {file_name}")
                return False
        
        return True

    async def answer_query(self, query_text: str, method: str = "auto") -> Dict[str, Any]:
        """
        Takes a user query and returns a comprehensive answer.
        
        Args:
            query_text: The user's question
            method: Query method ('global', 'local', or 'auto')
            
        Returns:
            Dictionary containing the answer and metadata
        """
        log.info(f"🔍 Processing query: '{query_text[:100]}...'")
        
        # Reset source tracking
        self.source_tracker.reset()
        
        # 1. Route the query to determine the best method
        if method == "auto":
            routing_info = self.router.determine_query_method(query_text)
            method = routing_info['method']
            log.info(f"📍 Auto-routed to method: '{method}'")
        
        # 2. Execute the GraphRAG query
        try:
            raw_response = await self._execute_graphrag_query(query_text, method)
            if not raw_response:
                return self._create_error_response("GraphRAG query failed")
            
            # 3. Process and enhance the response
            enhanced_response = await self.enhancer.enhance_response(query_text, raw_response)
            
            # 4. Add source tracking information
            enhanced_response['sources'] = self.source_tracker.get_summary()
            enhanced_response['query_metadata'] = {
                'method_used': method,
                'graphrag_root': str(self.graphrag_root),
                'data_source': 'deduplicated' if self.data_root.name == 'deduplicated' else 'original'
            }
            
            log.info("✅ Query processed successfully")
            return enhanced_response
            
        except Exception as e:
            log.error(f"❌ Query processing failed: {e}")
            return self._create_error_response(str(e))

    async def _execute_graphrag_query(self, query_text: str, method: str) -> Optional[Dict[str, Any]]:
        """Execute GraphRAG query using subprocess."""
        log.info(f"🚀 Executing GraphRAG {method} query")
        
        # Prepare the command
        cmd = [
            sys.executable,
            "-m", "graphrag.query",
            "--root", str(self.graphrag_root),
            "--method", method,
            query_text
        ]
        
        try:
            # Execute the query
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode != 0:
                log.error(f"GraphRAG query failed: {result.stderr}")
                return None
            
            # Parse the output
            output_lines = result.stdout.strip().split('\n')
            
            # Find the response (usually the last substantial line)
            response_text = ""
            for line in reversed(output_lines):
                if line.strip() and not line.startswith('[') and not line.startswith('INFO'):
                    response_text = line.strip()
                    break
            
            if not response_text:
                response_text = result.stdout.strip()
            
            return {
                'answer': response_text,
                'method': method,
                'raw_output': result.stdout,
                'success': True
            }
            
        except subprocess.TimeoutExpired:
            log.error("GraphRAG query timed out")
            return None
        except Exception as e:
            log.error(f"Error executing GraphRAG query: {e}")
            return None

    def _create_error_response(self, error_message: str) -> Dict[str, Any]:
        """Create a standardized error response."""
        return {
            'answer': f"I apologize, but I encountered an error processing your query: {error_message}",
            'success': False,
            'error': error_message,
            'sources': {},
            'query_metadata': {
                'method_used': 'error',
                'graphrag_root': str(self.graphrag_root)
            }
        }

    def get_available_entities(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get a sample of available entities for query suggestions."""
        entities_file = self.data_root / "create_final_entities.parquet"
        
        if not entities_file.exists():
            return []
        
        try:
            df = pd.read_parquet(entities_file)
            
            # Sample entities and return relevant fields
            sample_df = df.head(limit)
            entities = []
            
            for _, row in sample_df.iterrows():
                entity = {
                    'title': row.get('title', 'Unknown'),
                    'type': row.get('type', 'Unknown'),
                    'description': str(row.get('description', ''))[:200]  # Truncate description
                }
                entities.append(entity)
            
            return entities
            
        except Exception as e:
            log.error(f"Error loading entities: {e}")
            return []

    def get_system_stats(self) -> Dict[str, Any]:
        """Get statistics about the GraphRAG system."""
        stats = {
            'graphrag_root': str(self.graphrag_root),
            'data_source': 'deduplicated' if self.data_root.name == 'deduplicated' else 'original',
            'entities_count': 0,
            'relationships_count': 0,
            'communities_count': 0,
            'system_ready': self.validate_graphrag_output()
        }
        
        # Count entities
        entities_file = self.data_root / "create_final_entities.parquet"
        if entities_file.exists():
            try:
                df = pd.read_parquet(entities_file)
                stats['entities_count'] = len(df)
            except Exception:
                pass
        
        # Count relationships
        relationships_file = self.data_root / "create_final_relationships.parquet"
        if relationships_file.exists():
            try:
                df = pd.read_parquet(relationships_file)
                stats['relationships_count'] = len(df)
            except Exception:
                pass
        
        # Count communities
        communities_file = self.data_root / "create_final_communities.parquet"
        if communities_file.exists():
            try:
                df = pd.read_parquet(communities_file)
                stats['communities_count'] = len(df)
            except Exception:
                pass
        
        return stats 