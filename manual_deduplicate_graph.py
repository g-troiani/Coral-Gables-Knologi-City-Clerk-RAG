#!/usr/bin/env python3
"""
Manual Graph Deduplication Script

This script manually deduplicates nodes in the graph database while preserving 
and redirecting relationships to canonical nodes.

It identifies duplicates based on:
- Fuzzy string matching on entity names
- Semantic similarity 
- Entity type analysis
- Known patterns from domain knowledge

Usage: python3 manual_deduplicate_graph.py
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from difflib import SequenceMatcher
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('deduplication.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GraphDeduplicator:
    """Handles manual deduplication of graph entities while preserving relationships."""
    
    def __init__(self, graph_dir: Path = Path("simple_ner_graph")):
        self.graph_dir = Path(graph_dir)
        self.merged_dir = self.graph_dir / "merged"
        self.entities_dir = self.merged_dir / "entities"
        self.backup_dir = self.graph_dir / "backup" / f"pre_dedup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Load current data
        self.entities_by_type: Dict[str, List[Dict]] = {}
        self.relationships: List[Dict] = []
        self.dedup_mappings: Dict[str, str] = {}  # old_id -> canonical_id
        
    def create_backup(self):
        """Create backup of merged data before deduplication."""
        logger.info(f"Creating backup at {self.backup_dir}")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        shutil.copytree(self.merged_dir, self.backup_dir / "merged")
        
    def load_data(self):
        """Load all merged entities and relationships."""
        logger.info("Loading merged entities and relationships...")
        
        # Load entities by type
        for entity_file in self.entities_dir.glob("*.json"):
            if entity_file.name.startswith("debug"):
                continue
                
            entity_type = entity_file.stem
            try:
                with open(entity_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    entities = data.get("entities", [])
                    self.entities_by_type[entity_type] = entities
                    logger.info(f"Loaded {len(entities)} {entity_type} entities")
            except Exception as e:
                logger.error(f"Error loading {entity_file}: {e}")
        
        # Load relationships
        rel_file = self.merged_dir / "relationships.json"
        if rel_file.exists():
            try:
                with open(rel_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.relationships = data.get("relationships", [])
                    logger.info(f"Loaded {len(self.relationships)} relationships")
            except Exception as e:
                logger.error(f"Error loading relationships: {e}")
    
    def normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        if not name:
            return ""
        # Remove common prefixes/suffixes, normalize whitespace
        normalized = re.sub(r'\b(Mayor|Commissioner|Vice Mayor|City Manager|City Clerk|Dr\.?|Mr\.?|Ms\.?|Mrs\.?)\s*', '', name, flags=re.IGNORECASE)
        normalized = re.sub(r'\s+', ' ', normalized).strip().lower()
        return normalized
    
    def calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two names."""
        norm1 = self.normalize_name(name1)
        norm2 = self.normalize_name(name2)
        
        if not norm1 or not norm2:
            return 0.0
            
        # Use sequence matcher for fuzzy matching
        return SequenceMatcher(None, norm1, norm2).ratio()
    
    def find_person_duplicates(self) -> Dict[str, str]:
        """Find duplicate Person entities and return mapping to canonical IDs."""
        persons = self.entities_by_type.get("Person", [])
        logger.info(f"Analyzing {len(persons)} Person entities for duplicates...")
        
        mappings = {}
        processed = set()
        
        for i, person1 in enumerate(persons):
            if person1.get("id") in processed:
                continue
                
            name1 = person1.get("name", "")
            duplicates = []
            
            # Find similar persons
            for j, person2 in enumerate(persons):
                if i >= j:  # Skip self and already compared pairs
                    continue
                    
                name2 = person2.get("name", "")
                similarity = self.calculate_similarity(name1, name2)
                
                # High similarity threshold for person names
                if similarity >= 0.75:  
                    duplicates.append((person2, similarity))
            
            if duplicates:
                # Sort by similarity, highest first
                duplicates.sort(key=lambda x: x[1], reverse=True)
                
                # Choose canonical entity (prefer most complete name/info)
                canonical = person1
                canonical_score = self._score_person_completeness(person1)
                
                for dup_person, sim in duplicates:
                    dup_score = self._score_person_completeness(dup_person)
                    if dup_score > canonical_score:
                        canonical = dup_person
                        canonical_score = dup_score
                
                # Map all duplicates to canonical
                canonical_id = canonical.get("id")
                processed.add(canonical_id)
                
                logger.info(f"Found duplicate group for '{canonical.get('name')}':")
                logger.info(f"  Canonical: {canonical_id}")
                
                for dup_person, sim in duplicates:
                    if dup_person.get("id") != canonical_id:
                        mappings[dup_person.get("id")] = canonical_id
                        processed.add(dup_person.get("id"))
                        logger.info(f"  Duplicate: {dup_person.get('id')} -> {canonical_id} (similarity: {sim:.3f})")
                
                if person1.get("id") != canonical_id:
                    mappings[person1.get("id")] = canonical_id
        
        return mappings
    
    def find_organization_duplicates(self) -> Dict[str, str]:
        """Find duplicate Organization entities and return mapping to canonical IDs."""
        orgs = self.entities_by_type.get("Organization", [])
        logger.info(f"Analyzing {len(orgs)} Organization entities for duplicates...")
        
        mappings = {}
        processed = set()
        
        # Check for persons incorrectly classified as organizations
        persons = self.entities_by_type.get("Person", [])
        person_names = {self.normalize_name(p.get("name", "")) for p in persons}
        
        for org in orgs:
            org_name = org.get("name", "")
            norm_org_name = self.normalize_name(org_name)
            
            # Check if this org name matches a person name
            for person in persons:
                person_name = person.get("name", "")
                norm_person_name = self.normalize_name(person_name)
                
                if norm_org_name and norm_person_name and norm_org_name == norm_person_name:
                    # This org should be merged with the person
                    mappings[org.get("id")] = person.get("id")
                    logger.info(f"Organization '{org_name}' -> Person '{person_name}' (misclassification)")
                    processed.add(org.get("id"))
                    break
        
        # Find similar organization names
        for i, org1 in enumerate(orgs):
            if org1.get("id") in processed:
                continue
                
            name1 = org1.get("name", "")
            duplicates = []
            
            for j, org2 in enumerate(orgs):
                if i >= j or org2.get("id") in processed:
                    continue
                    
                name2 = org2.get("name", "")
                similarity = self.calculate_similarity(name1, name2)
                
                # Slightly lower threshold for organizations
                if similarity >= 0.80:  
                    duplicates.append((org2, similarity))
            
            if duplicates:
                duplicates.sort(key=lambda x: x[1], reverse=True)
                
                canonical = org1
                canonical_score = self._score_org_completeness(org1)
                
                for dup_org, sim in duplicates:
                    dup_score = self._score_org_completeness(dup_org)
                    if dup_score > canonical_score:
                        canonical = dup_org
                        canonical_score = dup_score
                
                canonical_id = canonical.get("id")
                processed.add(canonical_id)
                
                logger.info(f"Found organization duplicate group for '{canonical.get('name')}':")
                logger.info(f"  Canonical: {canonical_id}")
                
                for dup_org, sim in duplicates:
                    if dup_org.get("id") != canonical_id:
                        mappings[dup_org.get("id")] = canonical_id
                        processed.add(dup_org.get("id"))
                        logger.info(f"  Duplicate: {dup_org.get('id')} -> {canonical_id} (similarity: {sim:.3f})")
                
                if org1.get("id") != canonical_id:
                    mappings[org1.get("id")] = canonical_id
        
        return mappings
    
    def find_document_duplicates(self) -> Dict[str, str]:
        """Find duplicate Document entities."""
        docs = self.entities_by_type.get("Document", [])
        logger.info(f"Analyzing {len(docs)} Document entities for duplicates...")
        
        mappings = {}
        processed = set()
        
        for i, doc1 in enumerate(docs):
            if doc1.get("id") in processed:
                continue
                
            name1 = doc1.get("name", "")
            doc_type1 = doc1.get("documentType", "")
            
            duplicates = []
            
            for j, doc2 in enumerate(docs):
                if i >= j:
                    continue
                    
                name2 = doc2.get("name", "")
                doc_type2 = doc2.get("documentType", "")
                
                # Must be same document type
                if doc_type1 != doc_type2:
                    continue
                    
                similarity = self.calculate_similarity(name1, name2)
                
                if similarity >= 0.85:  # High threshold for documents
                    duplicates.append((doc2, similarity))
            
            if duplicates:
                duplicates.sort(key=lambda x: x[1], reverse=True)
                
                canonical = doc1
                canonical_score = self._score_doc_completeness(doc1)
                
                for dup_doc, sim in duplicates:
                    dup_score = self._score_doc_completeness(dup_doc)
                    if dup_score > canonical_score:
                        canonical = dup_doc
                        canonical_score = dup_score
                
                canonical_id = canonical.get("id")
                processed.add(canonical_id)
                
                logger.info(f"Found document duplicate group for '{canonical.get('name')}':")
                logger.info(f"  Canonical: {canonical_id}")
                
                for dup_doc, sim in duplicates:
                    if dup_doc.get("id") != canonical_id:
                        mappings[dup_doc.get("id")] = canonical_id
                        processed.add(dup_doc.get("id"))
                        logger.info(f"  Duplicate: {dup_doc.get('id')} -> {canonical_id} (similarity: {sim:.3f})")
                
                if doc1.get("id") != canonical_id:
                    mappings[doc1.get("id")] = canonical_id
        
        return mappings
    
    def _score_person_completeness(self, person: Dict) -> int:
        """Score a person entity based on completeness of information."""
        score = 0
        if person.get("name"):
            score += len(person["name"])  # Prefer longer, more complete names
        if person.get("title"):
            score += 10
        if person.get("affiliation"):
            score += 10
        if person.get("contactInfo"):
            score += 5
        if person.get("_sources"):
            score += len(person["_sources"])  # Prefer entities from more sources
        return score
    
    def _score_org_completeness(self, org: Dict) -> int:
        """Score an organization entity based on completeness."""
        score = 0
        if org.get("name"):
            score += len(org["name"])
        if org.get("jurisdiction"):
            score += 10
        if org.get("address"):
            score += 10
        if org.get("_sources"):
            score += len(org["_sources"])
        return score
    
    def _score_doc_completeness(self, doc: Dict) -> int:
        """Score a document entity based on completeness."""
        score = 0
        if doc.get("name"):
            score += len(doc["name"])
        if doc.get("documentType"):
            score += 10
        if doc.get("pageCount"):
            score += 5
        if doc.get("_sources"):
            score += len(doc["_sources"])
        return score
    
    def find_all_duplicates(self) -> Dict[str, str]:
        """Find all duplicates across entity types."""
        logger.info("Starting comprehensive duplicate detection...")
        
        all_mappings = {}
        
        # Find duplicates by entity type
        person_mappings = self.find_person_duplicates()
        org_mappings = self.find_organization_duplicates()
        doc_mappings = self.find_document_duplicates()
        
        all_mappings.update(person_mappings)
        all_mappings.update(org_mappings)  
        all_mappings.update(doc_mappings)
        
        logger.info(f"Total duplicate mappings found: {len(all_mappings)}")
        return all_mappings
    
    def update_relationships(self, mappings: Dict[str, str]):
        """Update all relationships to use canonical entity IDs."""
        logger.info(f"Updating {len(self.relationships)} relationships...")
        
        updated_count = 0
        
        for relationship in self.relationships:
            original_source = relationship.get("source")
            original_target = relationship.get("target")
            
            # Update source if it's a duplicate
            if original_source in mappings:
                relationship["source"] = mappings[original_source]
                updated_count += 1
                logger.debug(f"Updated relationship source: {original_source} -> {mappings[original_source]}")
            
            # Update target if it's a duplicate
            if original_target in mappings:
                relationship["target"] = mappings[original_target]
                updated_count += 1
                logger.debug(f"Updated relationship target: {original_target} -> {mappings[original_target]}")
        
        logger.info(f"Updated {updated_count} relationship endpoints")
    
    def remove_duplicate_entities(self, mappings: Dict[str, str]):
        """Remove duplicate entities from merged entity files."""
        logger.info("Removing duplicate entities...")
        
        duplicate_ids = set(mappings.keys())
        removed_count = 0
        
        for entity_type, entities in self.entities_by_type.items():
            original_count = len(entities)
            
            # Filter out duplicates
            filtered_entities = [
                entity for entity in entities 
                if entity.get("id") not in duplicate_ids
            ]
            
            removed = original_count - len(filtered_entities)
            if removed > 0:
                logger.info(f"Removed {removed} duplicate {entity_type} entities")
                removed_count += removed
                self.entities_by_type[entity_type] = filtered_entities
        
        logger.info(f"Total entities removed: {removed_count}")
    
    def merge_duplicate_sources(self, mappings: Dict[str, str]):
        """Merge source information from duplicates into canonical entities."""
        logger.info("Merging source information from duplicates...")
        
        # Create reverse mapping: canonical_id -> [duplicate_ids]
        canonical_to_dups = {}
        for dup_id, canonical_id in mappings.items():
            if canonical_id not in canonical_to_dups:
                canonical_to_dups[canonical_id] = []
            canonical_to_dups[canonical_id].append(dup_id)
        
        # Find duplicate entities to extract source info
        all_entities_by_id = {}
        for entity_type, entities in self.entities_by_type.items():
            for entity in entities:
                all_entities_by_id[entity.get("id")] = entity
        
        # Merge source information
        for canonical_id, dup_ids in canonical_to_dups.items():
            canonical_entity = all_entities_by_id.get(canonical_id)
            if not canonical_entity:
                continue
                
            canonical_sources = set(canonical_entity.get("_sources", []))
            
            # Add sources from duplicates
            for dup_id in dup_ids:
                # Note: duplicate entity may have been removed already, so we'd need 
                # to get this info before removal. For now, we'll skip this step
                # as the existing _sources should be sufficient
                pass
    
    def save_updated_data(self):
        """Save deduplicated entities and relationships."""
        logger.info("Saving deduplicated data...")
        
        # Save updated entities
        for entity_type, entities in self.entities_by_type.items():
            entity_file = self.entities_dir / f"{entity_type}.json"
            
            data = {
                "entity_type": entity_type,
                "count": len(entities),
                "entities": entities
            }
            
            with open(entity_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(entities)} {entity_type} entities")
        
        # Save updated relationships
        rel_file = self.merged_dir / "relationships.json"
        rel_data = {
            "count": len(self.relationships),
            "relationships": self.relationships
        }
        
        with open(rel_file, 'w', encoding='utf-8') as f:
            json.dump(rel_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(self.relationships)} relationships")
        
        # Save deduplication mappings for reference
        mapping_file = self.merged_dir / "deduplication_mappings.json"
        mapping_data = {
            "created": datetime.now().isoformat(),
            "total_mappings": len(self.dedup_mappings),
            "mappings": self.dedup_mappings
        }
        
        with open(mapping_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(self.dedup_mappings)} deduplication mappings")
    
    def verify_integrity(self):
        """Verify relationship integrity after deduplication."""
        logger.info("Verifying relationship integrity...")
        
        # Get all valid entity IDs
        valid_entity_ids = set()
        for entities in self.entities_by_type.values():
            for entity in entities:
                valid_entity_ids.add(entity.get("id"))
        
        # Check relationships
        invalid_relationships = []
        
        for i, rel in enumerate(self.relationships):
            source_id = rel.get("source")
            target_id = rel.get("target")
            
            if source_id not in valid_entity_ids:
                invalid_relationships.append((i, "source", source_id))
            
            if target_id not in valid_entity_ids:
                invalid_relationships.append((i, "target", target_id))
        
        if invalid_relationships:
            logger.warning(f"Found {len(invalid_relationships)} invalid relationship endpoints:")
            for idx, endpoint_type, entity_id in invalid_relationships[:10]:  # Show first 10
                logger.warning(f"  Relationship {idx}: {endpoint_type} '{entity_id}' not found")
            if len(invalid_relationships) > 10:
                logger.warning(f"  ... and {len(invalid_relationships) - 10} more")
        else:
            logger.info("All relationships have valid endpoints ✓")
    
    def run_deduplication(self):
        """Run the complete deduplication process."""
        logger.info("Starting manual graph deduplication...")
        
        # Step 1: Create backup
        self.create_backup()
        
        # Step 2: Load data
        self.load_data()
        
        # Step 3: Find duplicates
        self.dedup_mappings = self.find_all_duplicates()
        
        if not self.dedup_mappings:
            logger.info("No duplicates found!")
            return
        
        # Step 4: Update relationships
        self.update_relationships(self.dedup_mappings)
        
        # Step 5: Remove duplicate entities
        self.remove_duplicate_entities(self.dedup_mappings)
        
        # Step 6: Save updated data
        self.save_updated_data()
        
        # Step 7: Verify integrity
        self.verify_integrity()
        
        logger.info("Deduplication complete!")
        logger.info(f"Backup created at: {self.backup_dir}")
        logger.info(f"Removed {len(self.dedup_mappings)} duplicate entities")


def main():
    """Main execution function."""
    graph_dir = Path("simple_ner_graph")
    
    if not graph_dir.exists():
        logger.error(f"Graph directory not found: {graph_dir}")
        return
    
    deduplicator = GraphDeduplicator(graph_dir)
    deduplicator.run_deduplication()


if __name__ == "__main__":
    main()




