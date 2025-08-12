#!/usr/bin/env python3
"""
Complete test that:
1. Uses ACTUAL main pipeline functions
2. Processes complete documents (all chunks)
3. Validates against ontology
4. Shows full Cosmos DB format
5. Provides fast feedback
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple
from collections import defaultdict
import time
import sys

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Import ACTUAL pipeline components
from scripts.graph_rag_stages.phase2_building.ner.enhanced_ner_extractor import EnhancedNERExtractor
from scripts.graph_rag_stages.phase2_building.ner.simple_graph_builder import SimpleGraphBuilder
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)


class CompleteExtractionTest:
    """Complete test using actual pipeline logic with ontology validation."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        
        # Log the paths being used
        log.info(f"Output directory: {self.output_dir}")
        log.info(f"Chunks directory: {self.chunks_dir}")
        log.info(f"Chunks directory exists: {self.chunks_dir.exists()}")
        
        if self.chunks_dir.exists():
            chunk_count = len(list(self.chunks_dir.glob("*.txt")))
            log.info(f"Found {chunk_count} chunk files in chunks directory")
        
        # Use ACTUAL pipeline components
        self.ner_extractor = EnhancedNERExtractor(output_dir)
        
        # Get ontology for validation
        self.valid_relationships = set(UnifiedOntology.RELATIONSHIP_TYPES)
        self.relationship_definitions = UnifiedOntology.RELATIONSHIP_DEFINITIONS
        
    async def run_complete_test(self, max_documents: int = 1) -> Dict:
        """Run complete extraction test on specific document: 2024-03_-_01_09_2024_enhanced_ordinance."""
        
        print("\n" + "="*100)
        print("COMPLETE EXTRACTION TEST - USING MAIN PIPELINE FUNCTIONS")
        print("="*100)
        
        start_time = time.time()
        
        # Check if chunks directory exists
        if not self.chunks_dir.exists():
            print(f"\n❌ ERROR: Chunks directory not found: {self.chunks_dir}")
            print(f"Please ensure the path is correct or use --output-dir to specify the correct directory")
            return {}
        
        # Group chunks by document
        chunks_by_document = self._group_chunks_by_document()
        
        if not chunks_by_document:
            print(f"\n❌ ERROR: No chunk files found in: {self.chunks_dir}")
            print(f"Looking for *.txt files")
            # List what's actually in the directory
            if self.chunks_dir.exists():
                files = list(self.chunks_dir.iterdir())[:10]
                if files:
                    print(f"\nFound these files instead:")
                    for f in files:
                        print(f"  - {f.name}")
            return {}
        
        # Select specific document: 2024-03_-_01_09_2024_enhanced_ordinance
        target_document = "2024-03_-_01_09_2024_enhanced_ordinance"
        
        if target_document in chunks_by_document:
            selected_documents = [(target_document, chunks_by_document[target_document])]
            print(f"\n✅ Found target document: {target_document}")
        else:
            print(f"\n❌ Target document '{target_document}' not found!")
            print(f"Available documents:")
            for doc_name, chunks in chunks_by_document.items():
                print(f"  - {doc_name}: {len(chunks)} chunks")
            return {}
        
        total_chunks = sum(len(chunks) for _, chunks in selected_documents)
        
        print(f"\n📄 Processing {len(selected_documents)} documents ({total_chunks} total chunks)")
        for doc_name, chunks in selected_documents:
            print(f"  - {doc_name}: {len(chunks)} chunks")
        
        # Process each document's chunks using ACTUAL pipeline method
        all_entities = defaultdict(list)
        all_relationships = []
        extraction_stats = {
            'documents_processed': [],
            'chunks_processed': 0,
            'entities_by_type': defaultdict(int),
            'relationships_by_type': defaultdict(int),
            'invalid_relationships': []
        }
        
        for doc_name, chunk_files in selected_documents:
            print(f"\n{'='*80}")
            print(f"Processing document: {doc_name}")
            print('='*80)
            
            extraction_stats['documents_processed'].append(doc_name)
            
            # Process all chunks for this document
            for chunk_file in chunk_files:
                try:
                    print(f"  Processing chunk: {chunk_file.name}")
                    
                    # Use ACTUAL pipeline extraction method
                    entity_count = await self.ner_extractor._process_chunk(chunk_file)
                    
                    # Read what was extracted
                    entities, relationships = self._read_extracted_data(chunk_file)
                    
                    # Collect entities
                    for entity_type, entity_list in entities.items():
                        all_entities[entity_type].extend(entity_list)
                        extraction_stats['entities_by_type'][entity_type] += len(entity_list)
                    
                    # Collect and validate relationships
                    for rel in relationships:
                        rel_type = rel.get('type')
                        
                        # Skip extractedFrom (metadata relationship)
                        if rel_type == 'extractedFrom':
                            continue
                        
                        # Validate against ontology
                        if rel_type in self.valid_relationships:
                            all_relationships.append(rel)
                            extraction_stats['relationships_by_type'][rel_type] += 1
                        else:
                            extraction_stats['invalid_relationships'].append({
                                'type': rel_type,
                                'source': rel.get('source'),
                                'target': rel.get('target'),
                                'chunk': chunk_file.name
                            })
                    
                    extraction_stats['chunks_processed'] += 1
                    
                except Exception as e:
                    log.error(f"Error processing {chunk_file.name}: {e}")
        
        # Format for Cosmos DB
        cosmos_data = self._format_for_cosmos_db(all_entities, all_relationships)
        
        # Calculate timing
        total_time = time.time() - start_time
        extraction_stats['total_time'] = total_time
        extraction_stats['avg_time_per_chunk'] = total_time / extraction_stats['chunks_processed'] if extraction_stats['chunks_processed'] > 0 else 0
        
        # Display complete results
        self._display_complete_results(cosmos_data, extraction_stats)
        
        # Save results
        self._save_test_results(cosmos_data, extraction_stats)
        
        return {
            'cosmos_data': cosmos_data,
            'extraction_stats': extraction_stats
        }
    
    def _group_chunks_by_document(self) -> Dict[str, List[Path]]:
        """Group chunk files by their source document."""
        chunks_by_doc = defaultdict(list)
        
        # Debug: show what we're looking for
        print(f"\nLooking for chunk files in: {self.chunks_dir}")
        
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        print(f"Found {len(chunk_files)} .txt files")
        
        if chunk_files:
            print(f"Sample chunk files:")
            for f in chunk_files[:3]:
                print(f"  - {f.name}")
        
        for chunk_file in chunk_files:
            # Extract document name from chunk filename
            parts = chunk_file.stem.split("_", 1)
            if len(parts) >= 2:
                doc_name = parts[1]
                chunks_by_doc[doc_name].append(chunk_file)
            else:
                # Handle chunks with different naming convention
                doc_name = chunk_file.stem
                chunks_by_doc[doc_name].append(chunk_file)
        
        # Sort chunks within each document
        for doc_name in chunks_by_doc:
            chunks_by_doc[doc_name].sort()
        
        return dict(chunks_by_doc)
    
    def _read_extracted_data(self, chunk_file: Path) -> Tuple[Dict, List]:
        """Read entities and relationships from extraction output."""
        entities = defaultdict(list)
        relationships = []
        
        chunk_id = chunk_file.stem.split("_")[0]
        doc_name = "_".join(chunk_file.stem.split("_")[1:]) if len(chunk_file.stem.split("_")) > 1 else chunk_file.stem
        
        # Read entities
        for entity_dir in self.output_dir.iterdir():
            if entity_dir.is_dir() and entity_dir.name not in ['document_chunks', 'relationships']:
                entity_file = entity_dir / f"{chunk_id}_{doc_name}.json"
                if entity_file.exists():
                    with open(entity_file, 'r') as f:
                        data = json.load(f)
                        entities[entity_dir.name] = data.get('entities', [])
        
        # Read relationships
        rel_file = self.output_dir / "relationships" / f"{chunk_id}_{doc_name}.json"
        if rel_file.exists():
            with open(rel_file, 'r') as f:
                data = json.load(f)
                relationships = data.get('relationships', [])
        
        return entities, relationships
    
    def _format_for_cosmos_db(self, entities: Dict, relationships: List) -> Dict:
        """Format entities and relationships for Cosmos DB."""
        cosmos_vertices = []
        cosmos_edges = []
        
        # Keep track of all entity IDs
        entity_ids = set()
        
        # Format vertices
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                id_field = EntityIDStandards.get_id_field(entity_type)
                entity_id = entity.get(id_field) or entity.get('id')
                
                if not entity_id or entity_id in entity_ids:
                    continue
                
                entity_ids.add(entity_id)
                
                vertex = {
                    "id": entity_id,
                    "label": entity_type.lower(),
                    "type": "vertex",
                    "partitionKey": entity_type.lower(),
                    "properties": {}
                }
                
                for key, value in entity.items():
                    if key not in [id_field, 'id'] and value is not None:
                        vertex["properties"][key] = [{
                            "id": f"{entity_id}_{key}",
                            "value": value
                        }]
                
                cosmos_vertices.append(vertex)
        
        # Format edges (deduplicate)
        edge_ids = set()
        for rel in relationships:
            edge_id = f"{rel['source']}_{rel['type']}_{rel['target']}"
            
            if edge_id not in edge_ids:
                edge_ids.add(edge_id)
                
                edge = {
                    "id": edge_id,
                    "label": rel['type'],
                    "type": "edge",
                    "inV": rel['target'],
                    "outV": rel['source'],
                    "properties": rel.get('attributes', {})
                }
                
                cosmos_edges.append(edge)
        
        return {
            "vertices": cosmos_vertices,
            "edges": cosmos_edges
        }
    
    def _display_complete_results(self, cosmos_data: Dict, stats: Dict) -> None:
        """Display complete test results with ontology validation."""
        vertices = cosmos_data['vertices']
        edges = cosmos_data['edges']
        
        print("\n" + "="*100)
        print("EXTRACTION RESULTS - COSMOS DB FORMAT")
        print("="*100)
        
        # Summary
        print(f"\n📊 EXTRACTION SUMMARY:")
        print(f"  - Documents processed: {len(stats['documents_processed'])}")
        print(f"  - Total chunks: {stats['chunks_processed']}")
        print(f"  - Total time: {stats['total_time']:.2f} seconds")
        print(f"  - Avg per chunk: {stats['avg_time_per_chunk']:.2f} seconds")
        print(f"  - Total vertices: {len(vertices)}")
        print(f"  - Total edges: {len(edges)}")
        
        # Entity breakdown
        print(f"\n📦 ENTITIES EXTRACTED:")
        for entity_type, count in sorted(stats['entities_by_type'].items()):
            print(f"  - {entity_type}: {count}")
        
        # Relationship breakdown
        print(f"\n🔗 RELATIONSHIPS EXTRACTED (Ontology-Validated):")
        for rel_type, count in sorted(stats['relationships_by_type'].items(), key=lambda x: -x[1]):
            print(f"  - {rel_type}: {count}")
        
        # Invalid relationships
        if stats['invalid_relationships']:
            print(f"\n❌ INVALID RELATIONSHIPS (not in ontology): {len(stats['invalid_relationships'])}")
            for inv in stats['invalid_relationships'][:5]:
                print(f"  - {inv['type']}: {inv['source']} → {inv['target']}")
        
        # Ontology coverage
        found_types = set(stats['relationships_by_type'].keys())
        coverage = len(found_types) / len(self.valid_relationships) * 100 if self.valid_relationships else 0
        print(f"\n✅ ONTOLOGY VALIDATION:")
        print(f"  - Valid types found: {len(found_types)}/{len(self.valid_relationships)}")
        print(f"  - Coverage: {coverage:.1f}%")
        
        # Show sample Cosmos DB data
        print(f"\n🔷 SAMPLE COSMOS DB VERTICES:")
        print("-"*80)
        
        vertices_by_type = defaultdict(list)
        for v in vertices:
            vertices_by_type[v['label']].append(v)
        
        for vtype, vlist in list(vertices_by_type.items())[:3]:
            print(f"\n{vtype.upper()} vertex example:")
            v = vlist[0]
            print(json.dumps(v, indent=2)[:500] + "...")
        
        print(f"\n➡️  SAMPLE COSMOS DB EDGES:")
        print("-"*80)
        
        edges_by_type = defaultdict(list)
        for e in edges:
            edges_by_type[e['label']].append(e)
        
        for etype, elist in list(edges_by_type.items())[:3]:
            print(f"\n{etype} edge example:")
            e = elist[0]
            print(json.dumps(e, indent=2))
        
        # Gremlin queries
        print(f"\n🚀 COSMOS DB GREMLIN COMMANDS:")
        print("-"*80)
        
        if vertices:
            v = vertices[0]
            print("\n// Add vertex:")
            print(f"g.addV('{v['label']}').property('id', '{v['id']}').property('partitionKey', '{v.get('partitionKey', v['label'])}')")
        
        if edges:
            e = edges[0]
            print("\n// Add edge:")
            print(f"g.V('{e['outV']}').addE('{e['label']}').to(g.V('{e['inV']}'))")
    
    def _save_test_results(self, cosmos_data: Dict, stats: Dict) -> None:
        """Save complete test results."""
        output_file = self.output_dir / "complete_extraction_test_results.json"
        
        with open(output_file, 'w') as f:
            json.dump({
                "test_timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "extraction_stats": stats,
                "cosmos_db_format": cosmos_data,
                "ontology_validation": {
                    "valid_relationship_types": list(self.valid_relationships),
                    "found_relationship_types": list(stats['relationships_by_type'].keys()),
                    "missing_relationship_types": list(self.valid_relationships - set(stats['relationships_by_type'].keys()))
                }
            }, f, indent=2)
        
        print(f"\n💾 Complete results saved to: {output_file}")


async def main():
    """Run the complete extraction test."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Complete extraction test using main pipeline')
    parser.add_argument('--output-dir', type=Path, 
                      default=Path('/Users/gianmariatroiani/Documents/knologi/graph_database/simple_ner_graph'),
                      help='NER extraction output directory')
    parser.add_argument('--max-documents', type=int, default=1,
                      help='Not used - script now processes specific document: 2024-03_-_01_09_2024_enhanced_ordinance')
    
    args = parser.parse_args()
    
    # Ensure directories exist
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run complete test
    tester = CompleteExtractionTest(args.output_dir)
    results = await tester.run_complete_test(args.max_documents)
    
    print("\n✅ Complete extraction test finished!")


if __name__ == "__main__":
    asyncio.run(main()) 