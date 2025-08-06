"""
Simple Data Loader - Extracted data infrastructure from SimpleNERQueryEngine.

This module contains only the data loading and indexing functionality,
removing the duplicated query processing logic that's now handled by UnifiedQueryEngine.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import networkx as nx

from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser, TemporalIndex
from scripts.graph_rag_stages.phase1_preprocessing.ner.markdown_chunker import MarkdownChunker

log = logging.getLogger(__name__)


class SimpleDataLoader:
    """
    Data loading and indexing functionality extracted from SimpleNERQueryEngine.
    Handles entity indices, chunks, relationships, and knowledge graph loading.
    """
    
    def __init__(self, graph_dir: Path = Path("simple_ner_graph")):
        """Initialize the data loader."""
        self.graph_dir = Path(graph_dir)
        self.graph_dir.mkdir(parents=True, exist_ok=True)
        self.chunks_dir = self.graph_dir / "document_chunks"
        
        # Use unified ontology categories
        self.entity_categories = UnifiedOntology.get_entity_categories()
        
        # Load indices
        self._load_all_indices()
        
        # Load knowledge graph
        self.graph = self._load_knowledge_graph()
        self.temporal_index = self._build_temporal_index()
    
    def _load_all_indices(self):
        """Load all data indices."""
        # Load entity index
        entity_index_path = self.graph_dir / "entity_index.json"
        if entity_index_path.exists():
            self.entity_index = json.loads(entity_index_path.read_text())
        else:
            self.entity_index = {}
            log.warning("Entity index not found")
        
        # Load chunk index
        chunk_index_path = self.graph_dir / "chunk_index.json"
        if chunk_index_path.exists():
            self.chunk_index = json.loads(chunk_index_path.read_text())
        else:
            self.chunk_index = {}
            log.warning("Chunk index not found")
        
        # Load relationship index
        relationship_index_path = self.graph_dir / "relationship_index.json"
        if relationship_index_path.exists():
            self.relationship_index = json.loads(relationship_index_path.read_text())
        else:
            self.relationship_index = {}
            log.warning("Relationship index not found")
        
        # Load bidirectional relationship index
        bidirectional_path = self.graph_dir / "bidirectional_relationship_index.json"
        if bidirectional_path.exists():
            self.bidirectional_relationship_index = json.loads(bidirectional_path.read_text())
        else:
            self.bidirectional_relationship_index = {}
        
        # Load status index
        self.status_index = self._load_status_index()
    
    def _load_status_index(self) -> Dict[str, List[str]]:
        """Load or create status index."""
        status_index_path = self.graph_dir / "status_index.json"
        if status_index_path.exists():
            return json.loads(status_index_path.read_text())
        return {}
    
    def _load_knowledge_graph(self) -> Optional[nx.Graph]:
        """Load knowledge graph from GraphML."""
        graph_path = self.graph_dir / "city_clerk_graph.graphml"
        if graph_path.exists():
            try:
                return nx.read_graphml(graph_path)
            except Exception as e:
                log.warning(f"Failed to load knowledge graph: {e}")
                return None
        return None
    
    def _build_temporal_index(self) -> TemporalIndex:
        """Build temporal index from chunk data."""
        temporal_index = TemporalIndex()
        
        for chunk_id, chunk_data in self.chunk_index.items():
            chunk_date = chunk_data.get('meeting_date')
            if chunk_date:
                try:
                    normalized_date = TemporalParser.normalize_date(chunk_date)
                    if normalized_date:
                        temporal_index.add_chunk(normalized_date, chunk_id, chunk_data)
                except Exception as e:
                    log.debug(f"Failed to parse date '{chunk_date}': {e}")
        
        return temporal_index
    
    async def initialize_pipeline(
        self, 
        markdown_source_dir: Path, 
        chunk_size: int = 1000, 
        chunk_overlap: int = 100, 
        use_integrated_pipeline: bool = True, 
        phase1_entities: Optional[List] = None
    ):
        """
        Initialize the data pipeline by processing documents.
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
        
        try:
            # Step 2: Extract entities
            log.info("🔍 Extracting entities...")
            if use_integrated_pipeline:
                log.info("Using integrated enhanced pipeline...")
                from scripts.graph_rag_stages.phase2_building.integrated_pipeline import IntegratedEntityPipeline
                integrated = IntegratedEntityPipeline(self.graph_dir)
                
                if phase1_entities:
                    await integrated.process_with_phase1_context(phase1_entities)
                    entity_count = len(phase1_entities)
                else:
                    entity_count = await integrated.process_chunks_standard()
            else:
                log.info("Using standard enhanced extractor...")
                from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor
                extractor = EnhancedNERExtractor(self.graph_dir)
                entity_count = await extractor.process_all_chunks()
        except Exception as e:
            log.error(f"Entity extraction failed: {e}")
            return
        
        try:
            # Step 3: Build graph
            log.info("🏗️ Building knowledge graph...")
            from scripts.graph_rag_stages.phase2_building.ner.simple_graph_builder import SimpleGraphBuilder
            builder = SimpleGraphBuilder(self.graph_dir)
            await builder.build_complete_graph()
        except Exception as e:
            log.error(f"Graph building failed: {e}")
            return
        
        # Reload indices after processing
        self._load_all_indices()
        
        # Rebuild temporal index
        self.temporal_index = self._build_temporal_index()
        
        log.info(f"✅ Data pipeline initialization completed. Processed {entity_count} entities.")
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics."""
        return {
            "entities_count": len(self.entity_index),
            "relationships_count": len(self.relationship_index),
            "chunks_count": len(self.chunk_index),
            "data_directory": str(self.graph_dir),
            "graph_loaded": self.graph is not None,
            "temporal_index_size": len(self.temporal_index.date_chunks) if self.temporal_index else 0
        }
    
    def get_entities_by_category(self, category: str) -> List[str]:
        """Get all entities of a specific category."""
        return list(self.entity_index.get(category, {}).keys())
    
    def get_chunk_data(self, chunk_id: str) -> Optional[Dict]:
        """Get chunk data by ID."""
        return self.chunk_index.get(chunk_id)
    
    def get_entity_relationships(self, entity: str) -> List[Dict]:
        """Get relationships for an entity."""
        relationships = []
        
        # Forward relationships
        for rel in self.bidirectional_relationship_index.get("forward", {}).get(entity, []):
            relationships.append(rel)
        
        # Reverse relationships
        for rel in self.bidirectional_relationship_index.get("reverse", {}).get(entity, []):
            relationships.append(rel)
        
        return relationships