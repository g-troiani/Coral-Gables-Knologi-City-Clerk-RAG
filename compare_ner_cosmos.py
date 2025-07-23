#!/usr/bin/env python3
"""
Compare entity and relationship counts between local NER extraction and Cosmos DB.
Shows discrepancies to help debug missing or duplicate nodes.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Set, Tuple, Any
from collections import defaultdict
from azure.cosmos import CosmosClient
from gremlin_python.driver import client, serializer
import os
from dotenv import load_dotenv  # Add this import

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class NERCosmosComparator:
    """Compare local NER extraction results with Cosmos DB graph."""
    
    def __init__(self, ner_output_dir: Path):
        self.ner_output_dir = Path(ner_output_dir)
        
        # Load .env file
        load_dotenv()  # This loads the .env file from current directory
        
        # Initialize Cosmos/Gremlin connection
        self.cosmos_endpoint = os.getenv("COSMOS_ENDPOINT")
        self.cosmos_key = os.getenv("COSMOS_KEY")
        self.database_name = os.getenv("COSMOS_DATABASE", "cgGraph")
        self.container_name = os.getenv("COSMOS_CONTAINER", "cityClerk")
        
        # Initialize gremlin_client to None first
        self.gremlin_client = None
        
        # For Gremlin API
        self.gremlin_endpoint = os.getenv("GREMLIN_ENDPOINT") or os.getenv("GREMLIN_URI")
        
        # Try to construct Gremlin endpoint from Cosmos endpoint
        if not self.gremlin_endpoint and self.cosmos_endpoint:
            # Convert https://xxx.documents.azure.com to wss://xxx.gremlin.cosmos.azure.com:443/
            if 'documents.azure.com' in self.cosmos_endpoint:
                account_name = self.cosmos_endpoint.split('.')[0].split('/')[-1]
                self.gremlin_endpoint = f"wss://{account_name}.gremlin.cosmos.azure.com:443/"
                log.info(f"📍 Constructed Gremlin endpoint: {self.gremlin_endpoint}")
        
        # Debug: print what we found
        log.info(f"🔍 Environment variables loaded:")
        log.info(f"   GREMLIN_ENDPOINT: {'✅ Set' if os.getenv('GREMLIN_ENDPOINT') else '❌ Not found'}")
        log.info(f"   GREMLIN_URI: {'✅ Set' if os.getenv('GREMLIN_URI') else '❌ Not found'}")
        log.info(f"   COSMOS_KEY: {'✅ Set' if self.cosmos_key else '❌ Not found'}")
        log.info(f"   COSMOS_DATABASE: {self.database_name}")
        log.info(f"   COSMOS_CONTAINER: {self.container_name}")
        log.info(f"   Using Gremlin endpoint: {'✅ Available' if self.gremlin_endpoint else '❌ Missing'}")
        if self.gremlin_endpoint and self.cosmos_key:
            try:
                self.gremlin_client = client.Client(
                    self.gremlin_endpoint,
                    'g',
                    username=f"/dbs/{self.database_name}/colls/{self.container_name}",
                    password=self.cosmos_key,
                    message_serializer=serializer.GraphSONSerializersV2d0()
                )
                log.info(f"✅ Connected to Gremlin endpoint: {self.gremlin_endpoint}")
            except Exception as e:
                log.error(f"❌ Failed to connect to Gremlin: {e}")
                self.gremlin_client = None
        else:
            log.warning("⚠️  GREMLIN_ENDPOINT or COSMOS_KEY not set - will only analyze local files")
    
    def count_local_entities(self) -> Dict[str, Dict[str, Any]]:
        """Count entities in local NER extraction output."""
        log.info("📊 Counting local NER entities...")
        
        entity_counts = defaultdict(lambda: {"count": 0, "unique_ids": set(), "chunks": set()})
        
        # Process each entity type directory
        for entity_dir in self.ner_output_dir.iterdir():
            if entity_dir.is_dir() and entity_dir.name not in ['document_chunks', 'relationships']:
                entity_type = entity_dir.name
                
                # Process each JSON file in the directory
                for entity_file in entity_dir.glob("*.json"):
                    chunk_id = entity_file.stem.split("_")[0]
                    
                    try:
                        with open(entity_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        entities = data.get('entities', [])
                        
                        # Count entities and collect IDs
                        for entity in entities:
                            # Find the entity ID based on type
                            entity_id = self._get_entity_id(entity, entity_type)
                            
                            if entity_id:
                                entity_counts[entity_type]["count"] += 1
                                entity_counts[entity_type]["unique_ids"].add(entity_id)
                                entity_counts[entity_type]["chunks"].add(chunk_id)
                    
                    except Exception as e:
                        log.error(f"Error reading {entity_file}: {e}")
        
        # Convert sets to counts for JSON serialization
        result = {}
        for entity_type, data in entity_counts.items():
            result[entity_type] = {
                "total_occurrences": data["count"],
                "unique_entities": len(data["unique_ids"]),
                "chunks_with_type": len(data["chunks"]),
                "sample_ids": list(data["unique_ids"])[:5]  # First 5 for inspection
            }
        
        return result
    
    def count_local_relationships(self) -> Dict[str, Any]:
        """Count relationships in local NER extraction output."""
        log.info("📊 Counting local NER relationships...")
        
        rel_counts = defaultdict(lambda: {"count": 0, "unique_pairs": set(), "chunks": set()})
        all_relationships = []
        
        rel_dir = self.ner_output_dir / "relationships"
        if not rel_dir.exists():
            log.warning("No relationships directory found")
            return {}
        
        for rel_file in rel_dir.glob("*.json"):
            chunk_id = rel_file.stem.split("_")[0]
            
            try:
                with open(rel_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                relationships = data.get('relationships', [])
                
                for rel in relationships:
                    rel_type = rel.get('type', 'unknown')
                    source = rel.get('source', '')
                    target = rel.get('target', '')
                    
                    if source and target:
                        # Create unique pair identifier
                        pair_id = f"{source}--{rel_type}--{target}"
                        
                        rel_counts[rel_type]["count"] += 1
                        rel_counts[rel_type]["unique_pairs"].add(pair_id)
                        rel_counts[rel_type]["chunks"].add(chunk_id)
                        
                        all_relationships.append({
                            "type": rel_type,
                            "source": source,
                            "target": target,
                            "chunk": chunk_id
                        })
            
            except Exception as e:
                log.error(f"Error reading {rel_file}: {e}")
        
        # Convert to final format
        result = {
            "by_type": {},
            "total_relationships": len(all_relationships),
            "unique_relationships": sum(len(data["unique_pairs"]) for data in rel_counts.values())
        }
        
        for rel_type, data in rel_counts.items():
            result["by_type"][rel_type] = {
                "total_occurrences": data["count"],
                "unique_pairs": len(data["unique_pairs"]),
                "chunks_with_type": len(data["chunks"]),
                "sample_pairs": list(data["unique_pairs"])[:3]
            }
        
        return result
    
    def count_cosmos_entities(self) -> Dict[str, Any]:
        """Count entities in Cosmos DB using Gremlin."""
        log.info("🌐 Counting Cosmos DB entities...")
        
        if not self.gremlin_client:
            log.warning("⚠️  Gremlin client not available - skipping Cosmos entity count")
            return {"_error": "No Gremlin connection"}
        
        entity_counts = {}
        
        try:
            # Get all vertex labels
            labels_query = "g.V().label().dedup()"
            labels_result = self.gremlin_client.submit(labels_query).all().result()
            
            # Count vertices by label
            for label in labels_result:
                count_query = f"g.V().hasLabel('{label}').count()"
                count_result = self.gremlin_client.submit(count_query).all().result()
                count = count_result[0] if count_result else 0
                
                # Get sample IDs
                sample_query = f"g.V().hasLabel('{label}').limit(5).id()"
                sample_result = self.gremlin_client.submit(sample_query).all().result()
                
                entity_counts[label] = {
                    "count": count,
                    "sample_ids": sample_result[:5]
                }
            
            # Get total vertex count
            total_query = "g.V().count()"
            total_result = self.gremlin_client.submit(total_query).all().result()
            total_count = total_result[0] if total_result else 0
            
            entity_counts["_total"] = total_count
            
        except Exception as e:
            log.error(f"Error querying Cosmos DB: {e}")
            entity_counts["_error"] = str(e)
        
        return entity_counts
    
    def count_cosmos_relationships(self) -> Dict[str, Any]:
        """Count relationships/edges in Cosmos DB using Gremlin."""
        log.info("🌐 Counting Cosmos DB relationships...")
        
        if not self.gremlin_client:
            log.warning("⚠️  Gremlin client not available - skipping Cosmos relationship count")
            return {"_error": "No Gremlin connection"}
        
        rel_counts = {}
        
        try:
            # Get all edge labels
            labels_query = "g.E().label().dedup()"
            labels_result = self.gremlin_client.submit(labels_query).all().result()
            
            # Count edges by label
            for label in labels_result:
                count_query = f"g.E().hasLabel('{label}').count()"
                count_result = self.gremlin_client.submit(count_query).all().result()
                count = count_result[0] if count_result else 0
                
                # Get sample edge info
                sample_query = f"g.E().hasLabel('{label}').limit(3).project('source', 'target').by(outV().id()).by(inV().id())"
                sample_result = self.gremlin_client.submit(sample_query).all().result()
                
                rel_counts[label] = {
                    "count": count,
                    "sample_edges": sample_result[:3]
                }
            
            # Get total edge count
            total_query = "g.E().count()"
            total_result = self.gremlin_client.submit(total_query).all().result()
            total_count = total_result[0] if total_result else 0
            
            rel_counts["_total"] = total_count
            
        except Exception as e:
            log.error(f"Error querying Cosmos DB edges: {e}")
            rel_counts["_error"] = str(e)
        
        return rel_counts
    
    def _get_entity_id(self, entity: Dict, entity_type: str) -> str:
        """Extract entity ID based on type."""
        # ID field mapping
        id_field_map = {
            'Person': 'personID',
            'Organization': 'orgID', 
            'Location': 'locationID',
            'Event': 'eventID',
            'Document': 'documentID',
            'AgendaItem': 'agendaItemID',
            'Policy': 'policyID',
            'Asset': 'assetID',
            'Contract': 'contractID',
            'Project': 'projectID',
            'Role': 'roleID',
            'Action': 'actionID',
            'Topic': 'topicID',
            'Section': 'sectionID',
            'Technology': 'techID',
            'VoteOutcome': 'voteID'
        }
        
        id_field = id_field_map.get(entity_type, f"{entity_type.lower()}ID")
        return entity.get(id_field) or entity.get('id', '')
    
    def compare_counts(self, local_entities: Dict, local_rels: Dict, 
                      cosmos_entities: Dict, cosmos_rels: Dict) -> None:
        """Compare and display differences between local and Cosmos counts."""
        
        print("\n" + "="*80)
        print("📊 NER EXTRACTION vs COSMOS DB COMPARISON")
        print("="*80)
        
        # Check if we have Cosmos data
        if cosmos_entities.get("_error") or cosmos_rels.get("_error"):
            print("\n⚠️  WARNING: Could not connect to Cosmos DB")
            print("   Showing only local NER extraction statistics\n")
        
        # Entity comparison
        print("\n🔷 ENTITIES COMPARISON:")
        print(f"{'Type':<20} {'Local (Unique)':<15} {'Cosmos':<15} {'Difference':<15} {'Status'}")
        print("-" * 80)
        
        all_types = set(local_entities.keys()) | set(cosmos_entities.keys())
        all_types.discard('_total')
        all_types.discard('_error')
        
        total_local = 0
        total_cosmos = cosmos_entities.get('_total', 0)
        
        for entity_type in sorted(all_types):
            local_count = local_entities.get(entity_type, {}).get('unique_entities', 0)
            cosmos_count = cosmos_entities.get(entity_type, {}).get('count', 0)
            diff = cosmos_count - local_count
            
            total_local += local_count
            
            if cosmos_entities.get("_error"):
                status = "?"
                cosmos_display = "N/A"
                diff_display = "N/A"
            else:
                status = "✅" if diff == 0 else ("⚠️ Missing" if diff < 0 else "❌ Extra")
                cosmos_display = str(cosmos_count)
                diff_display = str(diff)
            
            print(f"{entity_type:<20} {local_count:<15} {cosmos_display:<15} {diff_display:<15} {status}")
        
        print("-" * 80)
        cosmos_total_display = str(total_cosmos) if not cosmos_entities.get("_error") else "N/A"
        diff_total_display = str(total_cosmos - total_local) if not cosmos_entities.get("_error") else "N/A"
        print(f"{'TOTAL':<20} {total_local:<15} {cosmos_total_display:<15} {diff_total_display:<15}")
        
        # Relationship comparison
        print("\n\n🔗 RELATIONSHIPS COMPARISON:")
        print(f"{'Type':<25} {'Local (Unique)':<15} {'Cosmos':<15} {'Difference':<15} {'Status'}")
        print("-" * 85)
        
        all_rel_types = set(local_rels.get('by_type', {}).keys()) | set(cosmos_rels.keys())
        all_rel_types.discard('_total')
        all_rel_types.discard('_error')
        
        for rel_type in sorted(all_rel_types):
            local_count = local_rels.get('by_type', {}).get(rel_type, {}).get('unique_pairs', 0)
            cosmos_count = cosmos_rels.get(rel_type, {}).get('count', 0)
            diff = cosmos_count - local_count
            
            if cosmos_rels.get("_error"):
                status = "?"
                cosmos_display = "N/A"
                diff_display = "N/A"
            else:
                status = "✅" if diff == 0 else ("⚠️ Missing" if diff < 0 else "❌ Extra")
                cosmos_display = str(cosmos_count)
                diff_display = str(diff)
            
            print(f"{rel_type:<25} {local_count:<15} {cosmos_display:<15} {diff_display:<15} {status}")
        
        print("-" * 85)
        total_local_rels = local_rels.get('unique_relationships', 0)
        total_cosmos_rels = cosmos_rels.get('_total', 0)
        
        cosmos_rels_display = str(total_cosmos_rels) if not cosmos_rels.get("_error") else "N/A"
        diff_rels_display = str(total_cosmos_rels - total_local_rels) if not cosmos_rels.get("_error") else "N/A"
        
        print(f"{'TOTAL':<25} {total_local_rels:<15} {cosmos_rels_display:<15} {diff_rels_display:<15}")
        
        # Detailed local analysis
        print("\n\n📋 LOCAL NER EXTRACTION SUMMARY:")
        print(f"\nEntity types found: {len([k for k in local_entities.keys() if k != '_total'])}")
        print(f"Relationship types found: {len(local_rels.get('by_type', {}))}")
        
        # Sample entities
        print("\n🔍 Sample Entity IDs from local extraction:")
        for entity_type in sorted(local_entities.keys())[:5]:  # Show first 5 types
            local_data = local_entities.get(entity_type, {})
            if local_data.get('sample_ids'):
                print(f"\n{entity_type}:")
                for id in local_data['sample_ids'][:3]:
                    print(f"  - {id}")
    
    def run_comparison(self):
        """Run the full comparison."""
        # Count local
        local_entities = self.count_local_entities()
        local_relationships = self.count_local_relationships()
        
        # Count Cosmos
        cosmos_entities = self.count_cosmos_entities()
        cosmos_relationships = self.count_cosmos_relationships()
        
        # Compare
        self.compare_counts(local_entities, local_relationships, 
                           cosmos_entities, cosmos_relationships)
        
        # Save detailed report
        report = {
            "local": {
                "entities": local_entities,
                "relationships": local_relationships
            },
            "cosmos": {
                "entities": cosmos_entities,
                "relationships": cosmos_relationships
            },
            "timestamp": str(Path.cwd())
        }
        
        report_path = self.ner_output_dir / "comparison_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n\n💾 Detailed report saved to: {report_path}")


def main():
    """Main entry point."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python compare_ner_cosmos.py <ner_output_dir>")
        print("\nOptional environment variables for Cosmos DB comparison:")
        print("  - GREMLIN_ENDPOINT (e.g., wss://your-cosmos.gremlin.cosmos.azure.com:443/)")
        print("  - COSMOS_KEY")
        print("  - COSMOS_DATABASE (default: cgGraph)")
        print("  - COSMOS_CONTAINER (default: cityClerk)")
        print("\nNote: Script will analyze local files even without Cosmos connection.")
        sys.exit(1)
    
    ner_output_dir = Path(sys.argv[1])
    if not ner_output_dir.exists():
        print(f"Error: Directory {ner_output_dir} does not exist")
        sys.exit(1)
    
    comparator = NERCosmosComparator(ner_output_dir)
    comparator.run_comparison()


if __name__ == "__main__":
    main() 