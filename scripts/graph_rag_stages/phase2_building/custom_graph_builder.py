"""
Custom graph builder for creating knowledge graphs in Cosmos DB.
"""

import hashlib
import re
import asyncio
import json
import logging as log
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from collections import defaultdict
from datetime import datetime
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from scripts.graph_rag_stages.common.config import get_config
from scripts.graph_rag_stages.common.temporal_utils import natural_item_sort_key
from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from tqdm import tqdm


class CosmosGraphOptimizer:
    """Optimizations for Cosmos DB Gremlin API performance."""
    
    @staticmethod
    def get_vertex_label_mapping():
        """Cosmos DB optimized vertex labels (lowercase, no special chars)."""
        return {
            'Person': 'person',
            'Organization': 'organization',
            'Document': 'document',
            'Policy': 'policy',
            'Event': 'event',
            'Action': 'action',
            'Asset': 'asset',
            'Project': 'project',
            'Location': 'location',
            'Role': 'role',
            'Topic': 'topic',
            'AgendaItem': 'agendaitem',
            'Contract': 'contract',
            'Technology': 'technology',
            'VoteOutcome': 'voteoutcome'
        }
    
    @staticmethod
    def get_indexed_properties():
        """Properties that should be indexed for performance."""
        return {
            'person': ['name', 'title'],
            'organization': ['name', 'type'],
            'document': ['title', 'issueDate', 'type'],
            'policy': ['policyID', 'title', 'status', 'effectiveDate'],
            'event': ['name', 'dateTime', 'type'],
            'agendaitem': ['itemID', 'title'],
            'location': ['name', 'address'],
            'voteoutcome': ['status', 'agendaItemID']
        }
    
    @staticmethod
    def get_composite_indices():
        """Composite index recommendations for common queries."""
        return [
            ('policy', ['status', 'effectiveDate']),
            ('event', ['type', 'dateTime']),
            ('document', ['type', 'issueDate']),
            ('person', ['affiliation', 'title'])
        ]


class CustomGraphBuilder:
    """Builds custom knowledge graphs in Cosmos DB from processed documents."""
    
    def __init__(self, cosmos_config: Optional[Dict] = None, output_dir: Optional[Path] = None, 
                 ner_output_dir: Optional[Path] = None):
        """Initialize with Cosmos optimizations."""
        self.config = get_config()
        self.output_dir = output_dir
        
        # Override with custom config if provided
        if cosmos_config:
            for key, value in cosmos_config.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
        
        # Initialize Cosmos client
        self.cosmos_client = CosmosGraphClient(
            endpoint=self.config.cosmos_endpoint,
            key=self.config.cosmos_key,
            database=self.config.cosmos_database,
            container=self.config.cosmos_container
        )
        
        # Track processed ordinances to avoid duplicates
        self.processed_ordinances = set()
        
        # Dynamic mapping for ordinance reference numbers to final ordinance numbers
        # Structure: {meeting_date: {agenda_item_code: {reference_number: final_ordinance_number}}}
        self.ordinance_mapping = {}
        
        self._PK  = cosmos_config.get("partitionKey",  "partitionKey") if cosmos_config else "partitionKey"
        
        # Safer partition value (PV) with warning when defaulting
        self._PV = (cosmos_config.get("partitionValue") if cosmos_config else None
                    or os.getenv("COSMOS_PARTITION_VALUE")
                    or "demo")
        if self._PV == "demo":
            log.getLogger(__name__).warning(
                "Using default partition value 'demo'. Set COSMOS_PARTITION_VALUE or pass cosmos_config.partitionValue."
            )
        
        self.edge_locks = defaultdict(asyncio.Lock)  # For edge race prevention
        
        self.ner_output_dir = ner_output_dir
        
        # Initialize optimizer
        self.optimizer = CosmosGraphOptimizer()
        
        # Cache for frequently accessed vertices
        self._vertex_cache = {}
        self._cache_ttl = 300  # 5 minutes
    
    def _agenda_item_vertex_id(self, code: str, meeting_date: str) -> str:
        normalized_date = (meeting_date or "").replace("-", "_").replace(".", "_")
        code_norm = (code or "").lower().replace("-", "_")
        return self._sanitize_id(f"agenda_item_{code_norm}_{normalized_date}")

    async def _execute_with_retry(self, query: str, max_retries: int = 3) -> List[Any]:
        """Execute query with retry logic for PreconditionFailed errors."""
        for attempt in range(max_retries):
            try:
                return await self.cosmos_client._execute_query(query)
            except Exception as e:
                if "PreconditionFailed" in str(e) and attempt < max_retries - 1:
                    wait_time = 0.1 * (2 ** attempt)  # Exponential backoff
                    log.warning(f"PreconditionFailed on attempt {attempt + 1}, retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                raise

    def sanitize_label(self, s: str, is_label: bool = False) -> str:
        """Sanitize: alphanum + _, ≤63 chars for labels/edges, ≤255 for vertices, hash if needed."""
        s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
        max_len = 63 if is_label else 255
        if len(s) > max_len or not s or not s[0].isalnum():
            s = 'id_' + hashlib.sha256(s.encode()).hexdigest()[:max_len - 3]
        return s

    def _escape_str(self, s: str) -> str:
        """Escape for Gremlin."""
        return s.replace('\\', '\\\\').replace("'", "\\'").replace('"', '\\"')

    async def _upsert_vertex(self, id: str, label: str, props: Dict[str, Any]) -> str:
        id = self.sanitize_label(id)
        label = self.sanitize_label(label, is_label=True)
        
        # ADD THIS LINE:
        props = self._reorder_properties(props)
        
        # Prop chain for update (exclude partitionKey AND id, start with '.' if non-empty)
        prop_chain = ""
        props_copy = {k: v for k, v in props.items() 
                      if k not in {'partitionKey', 'id'} and v is not None}
        for key, value in props_copy.items():
            escaped_key = self._escape_str(key)
            
            # CRITICAL FIX: Handle string booleans
            if isinstance(value, str) and value in ['True', 'False']:
                # Convert string boolean to lowercase
                val_str = 'true' if value == 'True' else 'false'
                prop_chain += f".property('{escaped_key}', {val_str})"
            elif isinstance(value, bool):
                val_str = 'true' if value else 'false'
                prop_chain += f".property('{escaped_key}', {val_str})"
            elif isinstance(value, (int, float)):
                val_str = str(value)
                prop_chain += f".property('{escaped_key}', {val_str})"
            elif isinstance(value, list):
                # Fix list handling too - clean recursively
                cleaned_list = []
                for item in value:
                    if isinstance(item, str) and item in ['True', 'False']:
                        cleaned_list.append(item.lower())
                    elif isinstance(item, bool):
                        cleaned_list.append('true' if item else 'false')
                    else:
                        cleaned_list.append(item)
                json_val = json.dumps(cleaned_list).replace("'", "\\'")
                prop_chain += f".property('{escaped_key}', '{json_val}')"
            else:
                val_str = f"'{self._escape_str(str(value))}'"
                prop_chain += f".property('{escaped_key}', {val_str})"
        
        # Create chain with partitionKey
        create_prop_chain = prop_chain + f".property('partitionKey', '{self._PV}')"

        # Use proper upsert pattern instead of fold().coalesce(unfold()...)
        if prop_chain:
            query = f"g.V('{id}').fold().coalesce(unfold(), addV('{label}').property('id', '{id}').property('partitionKey', '{self._PV}')){prop_chain}"
        else:
            query = f"g.V('{id}').fold().coalesce(unfold(), addV('{label}').property('id', '{id}').property('partitionKey', '{self._PV}'))"
        
        try:
            await self._execute_with_retry(query)
            log.debug(f"Upserted vertex {id}")
            return 'upserted'
        except Exception as e:
            if "conflict" in str(e).lower() or "already exists" in str(e).lower():
                log.debug(f"Ignored duplicate vertex {id}")
                return 'skipped'
            raise

    async def _upsert_edge(self, outV: str, label: str, inV: str, props: Dict[str, Any] = None) -> str:
        outV = self.sanitize_label(outV)
        inV = self.sanitize_label(inV)
        label = self.sanitize_label(label, is_label=True)
        
        # Check if nodes exist using count() which is supported in Azure Cosmos DB
        try:
            out_count = await self.cosmos_client._execute_query(f"g.V('{outV}').count()")
            in_count = await self.cosmos_client._execute_query(f"g.V('{inV}').count()")
            if not out_count or out_count[0] == 0 or not in_count or in_count[0] == 0:
                log.warning(f"Cannot create edge: nodes {outV} or {inV} don't exist")
                return 'skipped'
        except Exception as e:
            log.warning(f"Error checking vertex existence: {e}")
            # Continue with edge creation anyway, let it fail if vertices don't exist
        
        lock_key = (outV, label, inV)
        async with self.edge_locks[lock_key]:
            # Prop chain (similar quoting)
            prop_chain = ""
            if props:
                for key, value in props.items():
                    if value is not None:
                        escaped_key = self._escape_str(key)
                        
                        # CRITICAL FIX: Handle string booleans
                        if isinstance(value, str) and value in ['True', 'False']:
                            # Convert string boolean to lowercase
                            val_str = 'true' if value == 'True' else 'false'
                            prop_chain += f".property('{escaped_key}', {val_str})"
                        elif isinstance(value, bool):
                            val_str = 'true' if value else 'false'
                            prop_chain += f".property('{escaped_key}', {val_str})"
                        elif isinstance(value, (int, float)):
                            val_str = str(value)
                            prop_chain += f".property('{escaped_key}', {val_str})"
                        elif isinstance(value, list):
                            # Fix list handling too - clean recursively
                            cleaned_list = []
                            for item in value:
                                if isinstance(item, str) and item in ['True', 'False']:
                                    cleaned_list.append(item.lower())
                                elif isinstance(item, bool):
                                    cleaned_list.append('true' if item else 'false')
                                else:
                                    cleaned_list.append(item)
                            json_val = json.dumps(cleaned_list).replace("'", "\\'")
                            prop_chain += f".property('{escaped_key}', '{json_val}')"
                        else:
                            val_str = f"'{self._escape_str(str(value))}'"
                            prop_chain += f".property('{escaped_key}', {val_str})"
            
            # Use proper upsert pattern for edges
            if prop_chain:
                query = f"g.V('{outV}').outE('{label}').where(inV().hasId('{inV}')).fold().coalesce(unfold(), g.V('{outV}').addE('{label}').to(g.V('{inV}'))){prop_chain}"
            else:
                query = f"g.V('{outV}').outE('{label}').where(inV().hasId('{inV}')).fold().coalesce(unfold(), g.V('{outV}').addE('{label}').to(g.V('{inV}')))"
            
            try:
                await self._execute_with_retry(query)
                log.debug(f"Upserted edge {outV} -[{label}]-> {inV}")
                return 'upserted'
            except Exception as e:
                if "conflict" in str(e).lower() or "already exists" in str(e).lower():
                    log.debug(f"Ignored duplicate edge {outV} -[{label}]-> {inV}")
                    return 'skipped'
                raise
            finally:
                del self.edge_locks[lock_key]  # Prune after use

    async def _execute_batch(self, vertex_batch: List[Dict], edge_batch: List[Dict]) -> None:
        # Use the corrected method with delays
        await self._execute_batches(vertex_batch, edge_batch)

    async def _execute_batches(self, vertex_batch: List[Dict], edge_batch: List[Dict]) -> None:
        await self._execute_vertex_batch(vertex_batch)
        await self._execute_edge_batch(edge_batch)

    async def _execute_vertex_batch(self, batch: List[Dict]) -> None:
        sem = asyncio.Semaphore(5)
        async def _upsert_one(v):
            async with sem:
                await self._upsert_vertex(v["id"], v["label"], v["properties"])
        await asyncio.gather(*[_upsert_one(v) for v in batch])

    async def _execute_edge_batch(self, batch: List[Dict]) -> None:
        sem = asyncio.Semaphore(5)
        async def _upsert_one(e):
            async with sem:
                await self._upsert_edge(e["from"], e["label"], e["to"], e.get("properties", {}))
        await asyncio.gather(*[_upsert_one(e) for e in batch])

    async def _optimized_upsert_vertex(self, entity_id: str, entity_type: str, properties: Dict) -> str:
        """Optimized vertex creation with caching and proper labeling."""
        
        # ADD THIS LINE:
        properties = self._reorder_properties(properties)
        
        # Get optimized label
        label_mapping = self.optimizer.get_vertex_label_mapping()
        optimized_label = label_mapping.get(entity_type, entity_type.lower())
        
        # ADD THIS DEBUG LOG
        log.info(f"Creating vertex: entity_type='{entity_type}', optimized_label='{optimized_label}', entity_id='{entity_id}'")
        
        # Add indexed properties marker for Cosmos
        indexed_props = self.optimizer.get_indexed_properties().get(optimized_label, [])
        for prop in indexed_props:
            if prop in properties:
                # Add index hint (Cosmos uses this for optimization)
                properties[f'_indexed_{prop}'] = properties[prop]
        
        # Check cache first
        cache_key = f"{optimized_label}:{entity_id}"
        if cache_key in self._vertex_cache:
            log.debug(f"Using cached vertex: {cache_key}")
            return 'cached'
        
        # Create vertex
        result = await self._upsert_vertex(entity_id, optimized_label, properties)
        
        # Cache successful creation
        if result == 'upserted':
            self._vertex_cache[cache_key] = True
        
        return result

    async def _upsert_edge_with_entity_creation(self, outV: str, label: str, inV: str, 
                                              props: Dict[str, Any] = None,
                                              entity_map: Dict = None) -> str:
        """Create edge and create missing vertices if needed."""
        outV = self.sanitize_label(outV)
        inV = self.sanitize_label(inV)
        label = self.sanitize_label(label, is_label=True)
        
        # Check and create missing vertices
        try:
            out_exists = await self.cosmos_client.vertex_exists(outV)
            if not out_exists and entity_map and outV in entity_map:
                # Create minimal vertex
                entity_type = entity_map.get(outV, 'entity')
                await self._upsert_vertex(outV, entity_type.lower(), {
                    self._PK: self._PV,
                    'name': outV,
                    'auto_created': True
                })
                
            in_exists = await self.cosmos_client.vertex_exists(inV)
            if not in_exists and entity_map and inV in entity_map:
                # Create minimal vertex
                entity_type = entity_map.get(inV, 'entity')
                await self._upsert_vertex(inV, entity_type.lower(), {
                    self._PK: self._PV,
                    'name': inV,
                    'auto_created': True
                })
        except:
            pass  # Continue with edge creation
        
        # Now create edge
        return await self._upsert_edge(outV, label, inV, props)
    
    async def _create_search_indices(self) -> None:
        """Create search optimization structures in Cosmos."""
        log.info("Creating Cosmos DB search optimization structures...")
        
        # Create index vertices for each entity type
        for entity_type, label in self.optimizer.get_vertex_label_mapping().items():
            index_id = self.sanitize_label(f"idx_{label}_search")
            await self._upsert_vertex(
                index_id,
                'searchindex',
                {
                    self._PK: self._PV,
                    'entity_type': label,
                    'indexed_properties': self.optimizer.get_indexed_properties().get(label, []),
                    'created_at': datetime.now().isoformat()
                }
            )
        
        # Create composite index markers
        for label, props in self.optimizer.get_composite_indices():
            index_id = self.sanitize_label(f"idx_{label}_{'_'.join(props)}")
            await self._upsert_vertex(
                index_id,
                'compositeindex',
                {
                    self._PK: self._PV,
                    'entity_type': label,
                    'properties': props,
                    'created_at': datetime.now().isoformat()
                }
            )
    
    # Optimized query methods
    async def query_by_name(self, entity_type: str, name: str) -> List[Dict]:
        """Optimized name-based query."""
        label = self.optimizer.get_vertex_label_mapping().get(entity_type, entity_type.lower())
        
        # Use indexed property if available
        if 'name' in self.optimizer.get_indexed_properties().get(label, []):
            query = f"g.V().hasLabel('{label}').has('_indexed_name', '{self._escape_str(name)}').valueMap(true)"
        else:
            query = f"g.V().hasLabel('{label}').has('name', '{self._escape_str(name)}').valueMap(true)"
        
        try:
            return await self.cosmos_client._execute_query(query)
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []
    
    async def query_by_date_range(self, entity_type: str, start_date: str, end_date: str, 
                                  date_field: str = 'dateTime') -> List[Dict]:
        """Optimized date range query."""
        label = self.optimizer.get_vertex_label_mapping().get(entity_type, entity_type.lower())
        
        query = (f"g.V().hasLabel('{label}')"
                f".has('{date_field}', gte('{start_date}'))"
                f".has('{date_field}', lte('{end_date}'))"
                f".order().by('{date_field}', desc)"
                f".valueMap(true)")
        
        try:
            return await self.cosmos_client._execute_query(query)
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []
    
    async def query_relationships_typed(self, source_id: str, rel_type: str, 
                                      target_type: Optional[str] = None) -> List[Dict]:
        """Query specific relationship type with optional target filtering."""
        base_query = f"g.V('{source_id}').outE('{rel_type}')"
        
        if target_type:
            target_label = self.optimizer.get_vertex_label_mapping().get(target_type, target_type.lower())
            query = f"{base_query}.inV().hasLabel('{target_label}').path().by(valueMap(true))"
        else:
            query = f"{base_query}.inV().path().by(valueMap(true))"
        
        try:
            return await self.cosmos_client._execute_query(query)
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []
    
    async def query_agenda_items_by_meeting(self, meeting_date: str) -> List[Dict]:
        """Optimized query for agenda items by meeting date."""
        query = (f"g.V().hasLabel('agendaitem')"
                f".has('meeting_date', '{meeting_date}')"
                f".order().by('itemID', asc)"
                f".project('item', 'votes', 'documents')"
                f".by(valueMap(true))"
                f".by(outE('resultsIn').inV().valueMap(true).fold())"
                f".by(inE('discusses').outV().valueMap(true).fold())")
        
        try:
            return await self.cosmos_client._execute_query(query)
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []

    async def build_graph_from_ner_extraction(self, ner_output_dir: Path) -> None:
        """
        Build graph directly from NER extraction output without intermediate indices.
        
        Args:
            ner_output_dir: Directory containing NER extraction results
        """
        log.info("🚀 Building Cosmos DB graph directly from NER extraction")
        
        async with self.cosmos_client:
            # Process entities by type
            entity_map = {}  # Track entity IDs for relationship creation
            
            # Process each entity type
            for entity_type in ['Person', 'Organization', 'Document', 'Policy', 'Event', 
                              'Action', 'Asset', 'Project', 'Location', 'Role', 'Topic',
                              'AgendaItem', 'Contract', 'Technology', 'VoteOutcome']:
                
                entity_dir = ner_output_dir / entity_type
                if entity_dir.exists():
                    log.info(f"Processing {entity_type} entities...")
                    count = await self._process_entity_type(entity_dir, entity_type, entity_map)
                    log.info(f"✅ Created {count} {entity_type} vertices")
            
            # Process relationships
            rel_dir = ner_output_dir / "relationships"
            if rel_dir.exists():
                log.info("Processing relationships...")
                rel_count = await self._process_relationships(rel_dir, entity_map)
                log.info(f"✅ Created {rel_count} edges")
            
            log.info("✅ NER to Cosmos DB integration completed")
    
    async def _process_entity_type(self, entity_dir: Path, entity_type: str, entity_map: Dict) -> int:
        """Process entities with comprehensive error handling and no data loss."""
        count = 0
        seen_ids = set()
        failed_entities = []
        
        for json_file in entity_dir.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                entities = data.get('entities', [])
                chunk_id = data.get('chunk_id', '')
                source_file = data.get('source_file', '')
                
                # Process each entity individually to avoid batch failures
                for entity in entities:
                    try:
                        # Get entity ID with improved extraction
                        entity_id = self._get_entity_id(entity, entity_type)
                        if not entity_id:
                            # Try to generate ID from entity data
                            entity_id = self._generate_fallback_entity_id(entity, entity_type)
                            if not entity_id:
                                log.warning(f"Skipping entity without ID: {entity}")
                                continue
                        
                        # Check for duplicates by ID
                        if entity_id in seen_ids:
                            continue
                        
                        seen_ids.add(entity_id)
                        
                        # ALWAYS add to entity map, even before creation attempt
                        entity_map[entity_id] = entity_type
                        
                        # Build properties with all available data
                        properties = self._build_vertex_properties(entity, entity_type, chunk_id, source_file)
                        
                        # Create vertex with retry logic
                        try:
                            result = await self._optimized_upsert_vertex(
                                entity_id,
                                entity_type,
                                properties
                            )
                            
                            if result in ['upserted', 'cached']:
                                count += 1
                            else:
                                # Even if skipped, keep in entity map
                                log.debug(f"Entity {entity_id} already exists, kept in map")
                                
                        except Exception as e:
                            log.warning(f"Failed to create vertex {entity_id}: {e}")
                            failed_entities.append({
                                'entity': entity,
                                'error': str(e),
                                'entity_id': entity_id
                            })
                            # Keep in entity map anyway for relationship creation
                            
                    except Exception as e:
                        log.error(f"Error processing individual entity: {e}")
                        failed_entities.append({
                            'entity': entity,
                            'error': str(e),
                            'file': json_file.name
                        })
                        
            except Exception as e:
                log.error(f"Error reading {json_file}: {e}")
                continue
        
        # Log summary of failures
        if failed_entities:
            log.warning(f"Failed to process {len(failed_entities)} {entity_type} entities")
            # Optionally save failed entities for later analysis
            failed_file = entity_dir / f"_failed_{entity_type}.json"
            with open(failed_file, 'w', encoding='utf-8') as f:
                json.dump(failed_entities, f, indent=2)
        
        log.info(f"Processed {count} new {entity_type} entities, {len(seen_ids)} total unique IDs")
        return count

    def _generate_fallback_entity_id(self, entity: Dict, entity_type: str) -> Optional[str]:
        """Generate fallback entity ID from available entity data."""
        # Try various fields that could identify an entity
        id_candidates = []
        
        # Common identifying fields
        for field in ['name', 'title', 'code', 'number', 'reference', 'description']:
            if field in entity and entity[field]:
                id_candidates.append(str(entity[field]))
        
        # Combine available identifiers
        if id_candidates:
            # Use first non-empty identifier
            base_id = id_candidates[0][:50]  # Limit length
            return self._generate_entity_id(entity_type, base_id)
        
        # Generate from hash of entity content as last resort
        entity_str = json.dumps(entity, sort_keys=True)
        hash_id = hashlib.sha256(entity_str.encode()).hexdigest()[:12]
        return self.sanitize_label(f"{entity_type.lower()}_auto_{hash_id}")
    
    def _get_entity_id(self, entity: Dict, entity_type: str) -> Optional[str]:
        """Extract entity ID with comprehensive fallback handling."""
        # Comprehensive ID field mapping including all variations
        id_field_map = {
            'Person': ['personID', 'person_id', 'id'],
            'Organization': ['orgID', 'org_id', 'organizationID', 'organization_id', 'id'],
            'Location': ['locationID', 'location_id', 'id'],
            'Event': ['eventID', 'event_id', 'id'],
            'Document': ['documentID', 'document_id', 'docID', 'doc_id', 'id'],
            'AgendaItem': ['agendaItemID', 'agenda_item_id', 'agendaID', 'itemID', 'item_id', 'id'],
            'Policy': ['policyID', 'policy_id', 'id'],
            'Asset': ['assetID', 'asset_id', 'id'],
            'Contract': ['contractID', 'contract_id', 'id'],
            'Project': ['projectID', 'project_id', 'id'],
            'Role': ['roleID', 'role_id', 'id'],
            'Action': ['actionID', 'action_id', 'id'],
            'Topic': ['topicID', 'topic_id', 'id'],
            'Technology': ['techID', 'technology_id', 'technologyID', 'id'],
            'VoteOutcome': ['outcomeID', 'outcome_id', 'voteOutcomeID', 'vote_outcome_id', 'id'],
            'Section': ['sectionID', 'section_id', 'id'],
            'AgendaDocument': ['agendaDocID', 'agenda_doc_id', 'id'],
            'Board': ['boardID', 'board_id', 'id'],
            'Appointment': ['appointmentID', 'appointment_id', 'id'],
            'LegalReference': ['referenceID', 'reference_id', 'id'],
            'outcomes': ['outcomeID', 'outcome_id', 'id']  # Handle lowercase entity type
        }
        
        # Get possible fields for this entity type
        possible_fields = id_field_map.get(entity_type, ['id'])
        
        # Try each possible field name
        for field in possible_fields:
            if field in entity and entity[field]:
                entity_id = entity[field]
                # Ensure it's a string and not empty
                if entity_id and str(entity_id).strip():
                    return self.sanitize_label(str(entity_id))
        
        # If entity has generic 'id' field not in the list
        if 'id' in entity and entity['id']:
            return self.sanitize_label(str(entity['id']))
        
        # Fallback: generate ID from name if available
        if 'name' in entity and entity['name']:
            return self._generate_entity_id(entity_type, entity['name'])
        
        # Last resort: generate from any identifying field
        for field in ['title', 'code', 'number', 'reference']:
            if field in entity and entity[field]:
                return self._generate_entity_id(entity_type, str(entity[field]))
        
        log.warning(f"No ID found for {entity_type} entity: {entity}")
        return None
    
    async def _process_entity_batch(self, batch: List[Dict]) -> int:
        """Process a batch of entities with optimizations."""
        tasks = []
        
        for entity_data in batch:
            task = self._optimized_upsert_vertex(
                entity_data['id'],
                entity_data['type'],
                entity_data['properties']
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Create chunk edges in parallel
        chunk_tasks = []
        for i, entity_data in enumerate(batch):
            if results[i] not in ['cached', 'skipped'] and entity_data.get('chunk_id'):
                chunk_id = entity_data['chunk_id']
                chunk_vertex_id = self.sanitize_label(f"chunk-{chunk_id}")
                
                chunk_task = self._upsert_edge(
                    entity_data['id'],
                    "mentionedIn",
                    chunk_vertex_id,
                    {"extraction_date": datetime.now().isoformat()}
                )
                chunk_tasks.append(chunk_task)
        
        if chunk_tasks:
            await asyncio.gather(*chunk_tasks, return_exceptions=True)
        
        return len([r for r in results if r not in ['skipped', Exception]])
    
    async def _process_relationships(self, rel_dir: Path, entity_map: Dict) -> int:
        """Process relationships with auto-creation of missing entities."""
        count = 0
        missing_entities = defaultdict(set)  # Track missing entities by type
        created_auto_entities = set()
        
        for json_file in rel_dir.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                relationships = data.get('relationships', [])
                
                for rel in relationships:
                    try:
                        rel_type = rel.get('type')
                        source_id = self.sanitize_label(str(rel.get('source', '')))
                        target_id = self.sanitize_label(str(rel.get('target', '')))
                        attributes = rel.get('attributes', {})
                        
                        if not all([rel_type, source_id, target_id]):
                            log.debug(f"Skipping incomplete relationship: {rel}")
                            continue
                        
                        # Auto-create missing entities if needed
                        for entity_id in [source_id, target_id]:
                            if entity_id not in entity_map and entity_id not in created_auto_entities:
                                # Try to determine entity type from ID pattern
                                entity_type = self._infer_entity_type_from_id(entity_id)
                                if entity_type:
                                    # Create minimal entity
                                    log.info(f"Auto-creating missing entity: {entity_id} as {entity_type}")
                                    try:
                                        await self._upsert_vertex(
                                            entity_id,
                                            entity_type.lower(),
                                            {
                                                self._PK: self._PV,
                                                'name': entity_id,
                                                'auto_created': True,
                                                'created_for_relationship': rel_type
                                            }
                                        )
                                        entity_map[entity_id] = entity_type
                                        created_auto_entities.add(entity_id)
                                    except Exception as e:
                                        log.warning(f"Failed to auto-create entity {entity_id}: {e}")
                                        missing_entities[entity_type].add(entity_id)
                                else:
                                    missing_entities['unknown'].add(entity_id)
                        
                        # Create edge with cleaned attributes
                        try:
                            edge_label = self.sanitize_label(rel_type, is_label=True)
                            cleaned_attrs = self._clean_boolean_fields(attributes)
                            
                            result = await self._upsert_edge(source_id, edge_label, target_id, cleaned_attrs)
                            if result == 'upserted':
                                count += 1
                                
                        except Exception as e:
                            if "don't exist" in str(e):
                                log.debug(f"Edge creation failed - missing nodes: {source_id} -> {target_id}")
                                missing_entities['edge_failure'].add(f"{source_id}->{target_id}")
                            else:
                                log.warning(f"Error creating edge {rel_type}: {e}")
                        
                    except Exception as e:
                        log.error(f"Error processing relationship: {e}")
                        continue
                            
            except Exception as e:
                log.error(f"Error processing relationships file {json_file}: {e}")
                continue
        
        # Log summary of missing entities
        if missing_entities:
            log.warning(f"Missing entities summary:")
            total_missing = sum(len(entities) for entities in missing_entities.values())
            for entity_type, entities in missing_entities.items():
                if entities:
                    log.warning(f"  - {entity_type}: {len(entities)} missing")
                    # Log first few examples
                    examples = list(entities)[:5]
                    for example in examples:
                        log.debug(f"    • {example}")
            
            # Save missing entities report
            missing_report = rel_dir.parent / "_missing_entities_report.json"
            with open(missing_report, 'w', encoding='utf-8') as f:
                json.dump({k: list(v) for k, v in missing_entities.items()}, f, indent=2)
                
        log.info(f"Created {len(created_auto_entities)} auto-generated entities")
        return count

    def _infer_entity_type_from_id(self, entity_id: str) -> Optional[str]:
        """Infer entity type from ID pattern."""
        id_lower = entity_id.lower()
        
        # Pattern-based inference
        patterns = {
            'Person': ['person_', 'commissioner_', 'mayor_', 'staff_'],
            'Organization': ['org_', 'organization_', 'dept_', 'department_', 'company_'],
            'Document': ['document_', 'doc_', 'ordinance_', 'resolution_'],
            'Policy': ['policy_', 'ordinance_', 'resolution_'],
            'Event': ['event_', 'meeting_', 'hearing_'],
            'Action': ['action_', 'vote_', 'motion_'],
            'Asset': ['asset_', 'fund_', 'budget_'],
            'Project': ['project_', 'initiative_'],
            'Location': ['location_', 'address_', 'building_'],
            'AgendaItem': ['agenda_', 'item_', 'agendaitem_'],
            'VoteOutcome': ['outcome_', 'vote_outcome_'],
            'Topic': ['topic_', 'subject_', 'issue_']
        }
        
        for entity_type, prefixes in patterns.items():
            for prefix in prefixes:
                if id_lower.startswith(prefix):
                    return entity_type
        
        # Check for agenda item pattern (e.g., "E-1")
        if re.match(r'^[A-Z]-\d+', entity_id):
            return 'AgendaItem'
        
        return None
    
    
    def _generate_meeting_id(self, meeting_date: str) -> str:
        """Generate meeting ID as an Event ID."""
        if not meeting_date or meeting_date == "unknown":
            return self._sanitize_id("event_meeting_unknown")
        normalized_date = meeting_date.replace('.', "_").replace('-', "_")
        return self._sanitize_id(f"event_meeting_{normalized_date}")

    def _build_vertex_properties(self, entity: Dict, entity_type: str, chunk_id: str, source_file: str) -> Dict:
        """Build comprehensive vertex properties preserving all entity data."""
        # Start with partition key
        props = {self._PK: self._PV}
        
        # Add extraction metadata
        props['extraction_chunk_id'] = chunk_id
        props['extraction_source_file'] = source_file
        props['entity_type'] = entity_type
        props['extracted_at'] = datetime.now().isoformat()
        
        # Define read-only fields to exclude
        READ_ONLY_FIELDS = {'id', 'partitionKey', '_id', '_pk'}
        
        # Get expected attributes from ontology
        expected_attrs = UnifiedOntology.ENTITY_TYPES.get(entity_type, {}).get('attributes', [])
        
        # Add ALL entity fields (not just expected ones)
        for key, value in entity.items():
            if key not in READ_ONLY_FIELDS and value is not None:
                # Clean the key name
                clean_key = key.replace('-', '_').replace(' ', '_')
                
                # Handle different value types
                if isinstance(value, str):
                    props[clean_key] = value[:1000]  # Limit string length
                elif isinstance(value, bool):
                    props[clean_key] = value
                elif isinstance(value, (int, float)):
                    props[clean_key] = value
                elif isinstance(value, (list, dict)):
                    # Serialize complex types
                    props[clean_key] = json.dumps(value)[:1000]
                elif isinstance(value, datetime):
                    props[clean_key] = value.isoformat()
                else:
                    props[clean_key] = str(value)[:1000]
        
        # Ensure expected attributes exist (with None if not present)
        for attr in expected_attrs:
            if attr not in props:
                props[attr] = None
        
        # Add name field if missing but can be derived
        if 'name' not in props:
            # Try to derive name from other fields
            name_candidates = ['title', 'label', 'description', 'text']
            for candidate in name_candidates:
                if candidate in entity and entity[candidate]:
                    props['name'] = str(entity[candidate])[:255]
                    break
        
        return props
    
    async def _create_chunk_vertex_if_needed(self, chunk_id: str, original_chunk_id: str, source_file: str) -> None:
        """Create a chunk vertex if it doesn't exist."""
        props = {
            self._PK: self._PV,
            'chunk_id': original_chunk_id,
            'source_file': source_file,
            'type': 'extraction_chunk'
        }
        await self._upsert_vertex(chunk_id, 'chunk', props)
    
    # Add optimized Cosmos DB queries for common patterns
    async def query_entities_by_type(self, entity_type: str, limit: int = 100) -> List[Dict]:
        """Query entities by type with Cosmos optimization."""
        query = f"g.V().hasLabel('{entity_type.lower()}').limit({limit}).valueMap(true)"
        try:
            results = await self.cosmos_client._execute_query(query)
            return results
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []
    
    async def query_entity_relationships(self, entity_id: str, rel_type: Optional[str] = None) -> List[Dict]:
        """Query all relationships for an entity."""
        if rel_type:
            query = f"g.V('{entity_id}').bothE('{rel_type}').valueMap(true)"
        else:
            query = f"g.V('{entity_id}').bothE().valueMap(true)"
        
        try:
            results = await self.cosmos_client._execute_query(query)
            return results
        except Exception as e:
            log.error(f"Query failed: {e}")
            return []
    

    


    # ----------------------------------------------------------------------  
    # NEW public entry‑point – mirrors the NetworkX builder
    # ----------------------------------------------------------------------  
    async def build_graph_from_json(self, json_source_dir: Path) -> None:
        """Build graph with optimizations enabled."""
        log.info("🔗 Starting Optimized Cosmos DB Graph Building Pipeline")
        
        # MODIFIED: Look for agenda files in new location
        agenda_dir = json_source_dir / "agenda"
        if agenda_dir.exists():
            ontology_files = list(agenda_dir.glob("agenda_*.json"))
        else:
            # Fallback for backward compatibility
            stage3_dir = json_source_dir / "stage3"
            if stage3_dir.exists():
                ontology_files = list(stage3_dir.glob("*_stage3_ontology.json"))
            else:
                ontology_files = list(json_source_dir.rglob("*_stage3_ontology.json"))
        
        # Find all enhanced ordinance and resolution files in organized structure
        enhanced_files = []
        legal_dir = json_source_dir / "legal"
        if legal_dir.exists():
            enhanced_files.extend(list(legal_dir.glob("*_enhanced_ordinance.json")))
            enhanced_files.extend(list(legal_dir.glob("*_enhanced_resolution.json")))
        else:
            # Fallback to flat structure for backward compatibility
            enhanced_files.extend(list(json_source_dir.rglob("*_enhanced_ordinance.json")))
            enhanced_files.extend(list(json_source_dir.rglob("*_enhanced_resolution.json")))
        
        # Find ALL ordinances stuck at stage1 (both special and standard)
        stage1_ordinances = []
        
        # Get list of documents that already have enhanced files
        enhanced_doc_numbers = set()
        for enhanced_file in enhanced_files:
            doc_number = self._extract_document_number_from_filename(enhanced_file.name)
            if doc_number:
                enhanced_doc_numbers.add(doc_number)
        
        # Look for stage1 files in organized structure
        stage1_dir = json_source_dir / "stage1"
        if stage1_dir.exists():
            stage1_files = list(stage1_dir.glob("*_stage1_ocr.json"))
        else:
            # Fallback to flat structure for backward compatibility
            stage1_files = list(json_source_dir.rglob("*_stage1_ocr.json"))
        
        for stage1_file in stage1_files:
            filename = stage1_file.name
            
            # Extract document number from stage1 file
            doc_number = self._extract_document_number_from_filename(filename)
            if not doc_number:
                # For files without standard numbering, use full filename
                doc_number = filename.replace('_stage1_ocr.json', '')
            
            # Check if this document doesn't already have an enhanced version
            if doc_number not in enhanced_doc_numbers:
                # Check if this looks like an ordinance (has year pattern or special type)
                is_ordinance = (
                    any(prefix in filename for prefix in ['SOE', 'CG', 'EO', 'CAO']) or
                    filename.startswith(('201', '202')) or  # Standard ordinances 2010-2029
                    'Amendment' in filename or
                    'City Hall' in filename
                )
                
                if is_ordinance:
                    stage1_ordinances.append(stage1_file)
                    log.debug(f"📄 Found stage1-only ordinance: {filename}")
        
        enhanced_files.extend(stage1_ordinances)
        
        
        # Find all verbatim transcript files
        transcript_files = []
        verbatim_dir = json_source_dir / "verbatim"
        if verbatim_dir.exists():
            transcript_files.extend(list(verbatim_dir.glob("*_verbatim_transcript.json")))
            # Exclude collection files to avoid duplicates
            transcript_files = [f for f in transcript_files 
                              if not ('_collection' in f.name or 'comprehensive_' in f.name)]
        else:
            # Fallback to flat structure for backward compatibility
            transcript_files.extend(list(json_source_dir.rglob("*_verbatim_transcript.json")))
            transcript_files = [f for f in transcript_files 
                              if not ('_collection' in f.name or 'comprehensive_' in f.name)]
        
        log.info(f"Found {len(stage1_ordinances)} additional stage1-only ordinances to process")
        
        log.info(f"Found {len(ontology_files)} ontology files, {len(enhanced_files)} enhanced document files, and {len(transcript_files)} transcript files")
        
        if not ontology_files and not enhanced_files:
            log.warning("⚠️ No ontology or enhanced document files found!")
            return
        
        log.info(f"🚀 HIGH-PERFORMANCE MODE: Processing {len(ontology_files + enhanced_files)} files with batching...")
        
        # Use async context manager for proper client lifecycle
        async with self.cosmos_client:
            # Clear cache for fresh build
            self._vertex_cache.clear()
            
            # Create search indices first
            await self._create_search_indices()
            
            # Reset processed ordinances tracking
            self.processed_ordinances.clear()
            
            # Clear ordinance mapping for fresh build
            self.ordinance_mapping.clear()
            
            # Process enhanced document files FIRST (higher priority)
            if enhanced_files:
                log.info(f"📄 Processing {len(enhanced_files)} enhanced document files (priority processing)...")
                for json_file in tqdm(enhanced_files, desc="Processing enhanced documents"):
                    try:
                        await self._process_enhanced_document_file(json_file)
                        await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
                    except Exception as e:
                        log.error(f"❌ Error processing enhanced document file {json_file.name}: {e}")
                        continue
            
            # BUILD REFERENCE MAPPING: Process ontology files to extract reference numbers
            if ontology_files:
                log.info(f"📋 Building reference number mapping from {len(ontology_files)} ontology files...")
                for json_file in tqdm(ontology_files, desc="Building reference mappings"):
                    try:
                        await self._build_reference_mapping(json_file)
                        await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
                    except Exception as e:
                        log.error(f"❌ Error building reference mapping from {json_file.name}: {e}")
                        continue
            
            # Process ontology files SECOND (skip duplicates)
            if ontology_files:
                log.info(f"📄 Processing {len(ontology_files)} ontology files (skipping duplicates)...")
                for json_file in tqdm(ontology_files, desc="Processing ontology files"):
                    try:
                        await self._process_ontology_file(json_file)
                        await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
                    except Exception as e:
                        log.error(f"❌ Error processing ontology file {json_file.name}: {e}")
                        continue
            
            
            # Process verbatim transcript files
            if transcript_files:
                log.info(f"🎤 Processing {len(transcript_files)} verbatim transcript files...")
                for json_file in tqdm(transcript_files, desc="Processing transcript documents"):
                    try:
                        await self._process_transcript_file(json_file)
                        await asyncio.sleep(0.01)  # Small delay to prevent overwhelming
                    except Exception as e:
                        log.error(f"❌ Error processing transcript file {json_file.name}: {e}")
                        continue
            # Direct NER integration with optimizations
            ner_dir = self.ner_output_dir or json_source_dir.parent / "ner_output"
            if ner_dir.exists():
                log.info("📊 Found NER extraction output, integrating with optimizations...")
                await self.build_graph_from_ner_extraction(ner_dir)
            
            # Log cache statistics
            log.info(f"📈 Cache hits saved {len(self._vertex_cache)} vertex operations")
            
            log.info("✅ Optimized graph building completed!")


    


    async def _build_reference_mapping(self, ontology_file: Path) -> None:
        """
        Build reference number mapping from ontology files.
        This extracts reference numbers from agenda items and maps them to final ordinance numbers.
        """
        try:
            data = json.loads(ontology_file.read_text(encoding="utf-8"))
            meeting_date = (data.get("meeting_date") or "UNKNOWN").replace(".", "-")
            
            # Initialize meeting date mapping if not exists
            if meeting_date not in self.ordinance_mapping:
                self.ordinance_mapping[meeting_date] = {}
            
            # Extract reference numbers from entities
            for entity in data.get("entities", []):
                if entity.get("type") in ("ORDINANCE", "RESOLUTION"):
                    reference_number = entity.get("name")
                    agenda_item_code = entity.get("related_item") or entity.get("agenda_item_code")
                    
                    if reference_number and agenda_item_code:
                        # Initialize agenda item mapping if not exists
                        if agenda_item_code not in self.ordinance_mapping[meeting_date]:
                            self.ordinance_mapping[meeting_date][agenda_item_code] = {}
                        
                        # Add reference number to the mapping
                        self.ordinance_mapping[meeting_date][agenda_item_code]['reference_number'] = reference_number
                        
                        log.debug(f"📋 Added reference mapping: {meeting_date} -> {agenda_item_code} -> {reference_number}")
            
            # Also check sections/items for additional reference mappings
            for section in data.get("sections", []):
                for item in section.get("items", []):
                    agenda_item_code = item.get("item_code")
                    reference_number = item.get("document_reference")
                    
                    if agenda_item_code and reference_number:
                        # Initialize agenda item mapping if not exists
                        if agenda_item_code not in self.ordinance_mapping[meeting_date]:
                            self.ordinance_mapping[meeting_date][agenda_item_code] = {}
                        
                        # Add reference number to the mapping
                        self.ordinance_mapping[meeting_date][agenda_item_code]['reference_number'] = reference_number
                        
                        log.debug(f"📋 Added agenda reference mapping: {meeting_date} -> {agenda_item_code} -> {reference_number}")
                        
        except Exception as e:
            log.error(f"❌ Error building reference mapping from {ontology_file.name}: {e}")

    # ----------------------------------------------------------------------  
    # Internal helpers
    # ----------------------------------------------------------------------  
    def _clean_boolean_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert string booleans to actual booleans recursively."""
        if isinstance(data, dict):
            cleaned = {}
            for k, v in data.items():
                if isinstance(v, str) and v in ['True', 'False']:
                    cleaned[k] = v == 'True'
                elif isinstance(v, dict):
                    cleaned[k] = self._clean_boolean_fields(v)
                elif isinstance(v, list):
                    cleaned[k] = [self._clean_boolean_fields(item) for item in v]
                else:
                    cleaned[k] = v
            return cleaned
        elif isinstance(data, list):
            return [self._clean_boolean_fields(item) for item in data]
        else:
            return data

    def _sanitize_id(self, id_str: str) -> str:
        """Sanitize ID string to remove invalid characters for Cosmos DB Gremlin."""
        if not id_str:
            return "unknown"
        # Replace invalid characters with safe alternatives
        sanitized = (id_str
                    .replace('/', '-')       # Forward slash -> dash
                    .replace('\\', '-')      # Backslash -> dash  
                    .replace(' ', '-')       # Space -> dash
                    .replace(':', '-')       # Colon -> dash
                    .replace('"', '')        # Remove quotes
                    .replace("'", '')        # Remove quotes
                    .replace('(', '')        # Remove parentheses
                    .replace(')', '')        # Remove parentheses
                    .replace('[', '')        # Remove brackets
                    .replace(']', '')        # Remove brackets
                    .replace('{', '')        # Remove braces
                    .replace('}', '')        # Remove braces
                    .replace('&', 'and')     # Ampersand -> and
                    .replace('%', 'pct')     # Percent -> pct
                    .replace('#', 'num')     # Hash -> num
                    .replace('@', 'at')      # At symbol -> at
                    .replace('?', '')        # Remove question mark
                    .replace('!', '')        # Remove exclamation
                    .replace('*', '')        # Remove asterisk
                    .replace('+', 'plus')    # Plus -> plus
                    .replace('=', 'eq')      # Equals -> eq
                    .replace('<', 'lt')      # Less than -> lt
                    .replace('>', 'gt')      # Greater than -> gt
                    .replace('|', '-')       # Pipe -> dash
                    .replace(',', '')        # Remove comma
                    .replace(';', '')        # Remove semicolon
                    )
        # Remove any remaining non-alphanumeric characters except dash and underscore
        sanitized = re.sub(r'[^a-zA-Z0-9\-_]', '', sanitized)
        
        # Ensure it doesn't start or end with dash/underscore
        sanitized = sanitized.strip('-_')
        
        # Ensure it's not empty after sanitization
        return sanitized if sanitized else "unknown"









    async def _process_ontology_file(self, p: Path) -> None:
        data = json.loads(p.read_text(encoding="utf‑8"))
        doc_id = data.get("doc_id") or self.sanitize_label(f"doc-{p.stem}")
        meeting_date = (data.get("meeting_date") or "UNKNOWN").replace(".", "-")
        meeting_id = self._generate_meeting_id(meeting_date)
        
        # Extract hyperlinks for URL attributes
        hyperlinks = data.get("hyperlinks", [])

        # 1️⃣ EVENT vertex - add sourceURL
        await self._upsert_vertex(
            meeting_id,
            "event",
            {self._PK: self._PV,
             "eventID": meeting_id,
             "name": f"City Commission Meeting {meeting_date}",
             "type": "Regular Meeting",
             "dateTime": meeting_date,
             "status": "Completed",
             "doc_id": doc_id,
             "Source_File_Name": data.get("Source_File_Name", data.get("source_file", "")),
             # ADD: sourceURL from first hyperlink if available
             "sourceURL": hyperlinks[0].get("url", "") if hyperlinks else ""}
        )

        # Add AGENDA_DOCUMENT vertex with sourceURL
        agenda_doc_id = self._sanitize_id(f"agenda_{meeting_date.replace('-', '_')}")
        await self._upsert_vertex(
            agenda_doc_id,
            "agenda_document",
            {self._PK: self._PV,
             "agendaDocID": agenda_doc_id,
             "title": f"Agenda for Meeting {meeting_date}",
             "type": "agenda",
             "status": "Final",
             "issueDate": meeting_date,
             "meeting_date": meeting_date,
             "parent_meeting_id": meeting_id,
             "Source_File_Name": data.get("Source_File_Name", data.get("source_file", "")),
             "Source_File_Path": str(data.get("Source_File_Path", data.get("file_path", ""))),
             # ADD: sourceURL and hyperlinks as attributes
             "sourceURL": hyperlinks[0].get("url", "") if hyperlinks else "",
             "hyperlinks": hyperlinks
            }
        )
        await self._upsert_edge(meeting_id, "hasAgenda", agenda_doc_id, {})

        # 2️⃣  SECTION + AGENDA‑ITEM vertices --------------------------------
        sections: List[Dict[str, Any]] = data.get("sections", [])
        for s in sections:
            sec_id = self._sanitize_id(f"section_{meeting_date.replace('-', '_')}_{s.get('section_id')}")
            
            # Determine section_type based on section name
            section_name = s.get("section_name", "").upper()
            if "CONSENT" in section_name:
                section_type = "CONSENT"
            elif "PUBLIC COMMENT" in section_name:
                section_type = "PUBLIC_COMMENT"
            elif "PRESENTATION" in section_name:
                section_type = "PRESENTATIONS"
            else:
                section_type = "REGULAR_BUSINESS"
            
            await self._upsert_vertex(
                sec_id,
                "section",
                {self._PK: self._PV,
                 "sectionID": sec_id,
                 "name": s.get("section_name"),
                 "code": s.get("section_name"),
                 "section_type": section_type,
                 "order": s.get("section_order"),
                 "meeting_date": meeting_date,
                 "parent_agenda_doc_id": agenda_doc_id  # NEW: Add parent agenda ID
                }
            )
            await self._upsert_edge(agenda_doc_id, "hasSection", sec_id,
                    {"order": s.get("section_order")})

            for it in s.get("items", []):
                code = it.get("item_code") or "--"
                item_id = self._agenda_item_vertex_id(code, meeting_date)
                
                # Find hyperlinks for this item from hyperlinks
                item_hyperlinks = []
                doc_ref = it.get("document_reference", "")
                for link in hyperlinks:
                    link_text = link.get("text", "")
                    if doc_ref and doc_ref in link_text:
                        item_hyperlinks.append({
                            "url": link.get("url", ""),
                            "text": link.get("text", ""),
                            "page": link.get("page", 0)
                        })
                    elif code and code in link_text:
                        item_hyperlinks.append({
                            "url": link.get("url", ""),
                            "text": link.get("text", ""),
                            "page": link.get("page", 0)
                        })
                
                await self._upsert_vertex(
                    item_id,
                    "agendaitem",
                    {self._PK: self._PV,
                     # CHANGE: Use official ontology field name
                     "itemID": code,  # Changed from "code" to "itemID"
                     "title": it.get("title", ""),
                     "type": it.get("type", ""),
                     "presenter": it.get("presenter", ""),
                     "estimatedDuration": it.get("estimatedDuration"),
                     "document_reference": it.get("document_reference"),  # Keep this
                     "order": it.get("item_order"),
                     "meeting_date": meeting_date,
                     "document_type": MetadataStandards.classify_document(it.get("document_reference", ""), it.get("title", "")),
                     "document_classification": MetadataStandards.classify_document(it.get("document_reference", ""), it.get("title", "")),
                     "is_proclamation": self._is_proclamation(it),
                     "parent_section_id": sec_id,
                     # ADD: URLs and hyperlinks as attributes instead of separate vertices
                     "sourceURLs": [link["url"] for link in item_hyperlinks],  # Extract URLs for backwards compatibility
                     "hyperlinks": item_hyperlinks,  # Store full hyperlink metadata
                     "urls": json.dumps([link.get("url") for link in it.get("urls", [])]) if it.get("urls") else None
                    }
                )
                await self._upsert_edge(sec_id, "hasAgendaItem", item_id,
                        {"order": it.get("item_order")})

        # 3️⃣  TEMPORAL PRECEDES edges ---------------------------------------
        items = [it["item_code"] for s in sections for it in s.get("items", []) if it.get("item_code")]
        items_sorted = sorted(items, key=natural_item_sort_key)
        for a, b in zip(items_sorted, items_sorted[1:]):
            await self._upsert_edge(self._agenda_item_vertex_id(a, meeting_date), "precedes",
                    self._agenda_item_vertex_id(b, meeting_date), {})

        # 4️⃣  LEGAL DOCS, MOTIONS & VOTES -----------------------------------
        for e in data.get("entities", []):
            if e.get("type") in ("ORDINANCE", "RESOLUTION"):
                doc_num = e.get("name")
                
                # DYNAMIC MAPPING: Check if this reference number maps to a final ordinance number
                ref_code = e.get("related_item") or e.get("agenda_item_code")
                should_skip = False
                mapped_final_number = None
                
                if ref_code and meeting_date in self.ordinance_mapping:
                    # Check if this agenda item has a mapping
                    if ref_code in self.ordinance_mapping[meeting_date]:
                        mapping_data = self.ordinance_mapping[meeting_date][ref_code]
                        mapped_final_number = mapping_data.get('final_ordinance_number')
                        mapped_reference_number = mapping_data.get('reference_number')
                        
                        # Check if the reference number matches and final ordinance is already processed
                        if (mapped_reference_number == doc_num and 
                            mapped_final_number and 
                            mapped_final_number in self.processed_ordinances):
                            should_skip = True
                            log.debug(f"⏭️ Skipping duplicate {e['type']} {doc_num} (maps to {mapped_final_number} already processed from enhanced file)")
                
                # Also check direct reference number match (fallback)
                if not should_skip and doc_num in self.processed_ordinances:
                    should_skip = True
                    log.debug(f"⏭️ Skipping duplicate {e['type']} {doc_num} (already processed from enhanced file)")
                
                # Skip if this ordinance was already processed from enhanced files
                if should_skip:
                    continue
                
                doc_id  = self._sanitize_id(f"{e['type'].lower()}-{doc_num}")
                await self._upsert_vertex(doc_id, e["type"].lower(),
                        {self._PK: self._PV,
                         "doc_number": doc_num,
                         "title": e.get("description", "")[:512],
                         "meeting_date": meeting_date,
                         "document_type": MetadataStandards.classify_document("", e.get("description", "")),
                         "document_classification": MetadataStandards.classify_document("", e.get("description", "")),
                         # If we have a mapped final number, note it for potential future updates
                         "reference_number": doc_num if mapped_final_number else None,
                         "final_ordinance_number": mapped_final_number,
                         "parent_agenda_item_id": self._sanitize_id(f"item-{meeting_date.replace('.', '-')}-{ref_code}") if ref_code else None  # NEW: Add parent agenda item ID + date fix
                        })

            ref_code = e.get("related_item") or e.get("agenda_item_code")
            if ref_code:
                await self._upsert_edge(self._agenda_item_vertex_id(ref_code, meeting_date), "implements", doc_id, {})

            if e.get("vote_details"):
                await self._upsert_edge(doc_id, "votedOn", meeting_id,
                        {"yeas": e["vote_details"].get("yeas"),
                         "nays": e["vote_details"].get("nays"),
                         "unanimous": e["vote_details"].get("unanimous", False)})

            motion = e.get("motion", {})
            for label, person in [("sponsors", motion.get("moved_by")),
                                  ("sponsors", motion.get("seconded_by"))]:
                if person:
                    pid = self._sanitize_id(f"person_{person.lower().replace(' ', '_').replace('-', '_')}")
                    await self._upsert_vertex(pid, "person", {self._PK: self._PV, "name": person})
                    await self._upsert_edge(pid, label, doc_id, {})

        # 5️⃣  HYPERLINKS now stored as attributes (no separate vertices needed)

        # 6️⃣  GRAPH STATISTICS storage --------------------------------------
        stats_id = self.sanitize_label(f"stats-{meeting_date}")
        await self._upsert_vertex(
            stats_id,
            "statistics",
            {
                self._PK: self._PV,
                "meeting_date": meeting_date,
                "total_sections": len(sections),
                "total_items": sum(len(s.get("items", [])) for s in sections),
                "total_entities": len(data.get("entities", [])),
                "extraction_timestamp": datetime.now().isoformat()
            }
        )

    async def _process_transcript_file(self, json_file: Path) -> None:
        """Process a verbatim transcript JSON file and create document nodes."""
        log.debug(f"🎤 Processing transcript: {json_file.name}")
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                transcript_data = json.load(f)
            
            # Clean boolean fields
            transcript_data = self._clean_boolean_fields(transcript_data)
            
            # Generate document ID
            doc_id = transcript_data.get('id') or self._sanitize_id(f"transcript-{json_file.stem}")
            
            # Create document vertex
            properties = {
                self._PK: self._PV,
                'title': f"Verbatim Transcript - {', '.join(transcript_data.get('item_codes', ['Unknown']))}",
                'document_type': 'verbatim_transcript',
                'document_classification': 'verbatim_transcript',
                'Source_File_Name': transcript_data.get('Source_File_Name', json_file.name),
                'Source_File_Path': transcript_data.get('Source_File_Path', str(json_file)),
                'meeting_date': transcript_data.get('meeting_date', ''),
                'item_codes': json.dumps(transcript_data.get('item_codes', [])),  # Store as JSON string
                'transcript_type': transcript_data.get('transcript_type', 'unknown'),
                'page_count': len(transcript_data.get('pages', [])),
                'created_at': transcript_data.get('metadata', {}).get('extracted_at', ''),
                'text_content': transcript_data.get('full_text', '')[:1000] if transcript_data.get('full_text') else ''
            }
            
            await self._upsert_vertex(doc_id, 'document', properties)
            
            # Create relationships to agenda items
            meeting_date = transcript_data.get('meeting_date', '')
            if meeting_date:
                # Link to meeting
                meeting_id = self._generate_meeting_id(meeting_date)
                await self._upsert_edge(doc_id, 'discussedIn', meeting_id, {})
                
                # Link to each agenda item
                for item_code in transcript_data.get('item_codes', []):
                    item_id = self._agenda_item_vertex_id(item_code, meeting_date)
                    await self._upsert_edge(item_id, 'hasTranscript', doc_id, {
                        'transcript_type': transcript_data.get('transcript_type', 'unknown')
                    })
            
            log.debug(f"✅ Created transcript document node: {doc_id}")
            
        except Exception as e:
            log.error(f"❌ Error processing transcript {json_file.name}: {e}")
    async def _process_json_document_for_graph(self, json_file: Path) -> None:
        """
        Process a single JSON document and add its entities/relationships to the graph.
        
        Args:
            json_file: Path to JSON file
        """
        log.info(f"📄 Processing {json_file.name} for graph building")
        
        # Read the JSON content
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # Extract document metadata
        metadata = json_data.get('metadata', {})
        entities = json_data.get('entities', {})
        
        # Create document vertex
        doc_id = self._generate_document_id_from_json(json_file, metadata)
        await self._create_document_vertex_from_json(doc_id, metadata, json_file)
        
        # Process entities from JSON
        await self._process_entities_from_json(doc_id, entities, metadata)

    async def _create_document_vertex_from_json(self, doc_id: str, metadata: Dict, json_file: Path) -> None:
        """Create a vertex for the document from JSON data."""
        doc_id = doc_id or self.sanitize_label("unknown_doc")
        properties = {
            'title': metadata.get('title', json_file.stem),
            'document_type': metadata.get('document_type', 'document'),
            'Source_File_Name': json_file.name,
            'meeting_date': metadata.get('meeting_date', ''),
            'created_at': metadata.get('extraction_timestamp', ''),
            'word_count': metadata.get('word_count', 0),
            'page_count': metadata.get('page_count', 0),
        }
        
        await self.cosmos_client.create_vertex('document', doc_id, properties)
        log.debug(f"Created document vertex: {doc_id}")

    async def _process_entities_from_json(self, doc_id: str, entities: Dict, metadata: Dict) -> None:
        """Process entities from JSON data and create vertices/relationships."""
        doc_id = doc_id or self.sanitize_label("unknown_doc")
        log.debug(f"Processing entities for document: {doc_id}")
        
        # Process each entity type
        for entity_type, entity_list in entities.items():
            if not entity_list:
                continue
                
            log.debug(f"Processing {len(entity_list)} {entity_type} entities")
            
            for entity in entity_list:
                try:
                    await self._create_entity_vertex_from_json(entity_type, entity, doc_id, metadata)
                except Exception as e:
                    log.error(f"Error creating entity vertex for {entity_type}: {e}")
                    continue

    async def _create_entity_vertex_from_json(self, entity_type: str, entity: Dict, doc_id: str, metadata: Dict) -> None:
        """Create a vertex for an entity from JSON data."""
        doc_id = doc_id or self.sanitize_label("unknown_doc")
        entity_text = entity.get('text', '').strip()
        if not entity_text:
            return
        
        # Generate entity ID
        entity_id = self._generate_entity_id(entity_type, entity_text)
        
        # Create entity properties
        properties = {
            'text': entity_text,
            'type': entity_type,
            'confidence': entity.get('confidence', 0.0),
            'start_pos': entity.get('start_pos', 0),
            'end_pos': entity.get('end_pos', 0),
            'source_document': doc_id,
            'meeting_date': metadata.get('meeting_date', ''),
        }
        
        # Add any additional properties from the entity
        for key, value in entity.items():
            if key not in ['text', 'confidence', 'start_pos', 'end_pos']:
                properties[key] = value
        
        # Create vertex with appropriate label
        label = self._get_entity_label(entity_type)
        await self.cosmos_client.create_vertex(label, entity_id, properties)
        
        # Create relationship from document to entity
        await self.cosmos_client.create_edge_if_not_exists(
            doc_id, entity_id, 'CONTAINS'
        )
        
        log.debug(f"Created {entity_type} entity: {entity_text[:50]}...")

    def _get_entity_label(self, entity_type: str) -> str:
        """Get appropriate vertex label for entity type."""
        label_mapping = {
            'people': 'Person',
            'organizations': 'Organization',
            'named_locations': 'Location',
            'addresses': 'Address',
            'dates': 'Date',
            'events': 'Event',
            'actions': 'Action',
            'agenda_items': 'AgendaItem',
            'dollar_amounts': 'MonetaryAmount',
            'document_titles': 'DocumentTitle',
            'document_references': 'DocumentReference',
            'official_records': 'OfficialRecord',
            'meeting_metadata': 'MeetingMetadata',
            'products_technologies': 'Technology',
            'contracts': 'Contract',
        }
        return label_mapping.get(entity_type, 'Entity')































    async def _process_enhanced_document_file(self, json_file: Path) -> None:
        """
        Process enhanced ordinance/resolution JSON files and stage1 special ordinances.
        
        Args:
            json_file: Path to enhanced document JSON file or stage1 OCR file
        """
        log.debug(f"📄 Processing document: {json_file.name}")
        
        try:
            # Read the JSON content
            with open(json_file, 'r', encoding='utf-8') as f:
                doc_data = json.load(f)
            
            # Clean boolean fields at source
            doc_data = self._clean_boolean_fields(doc_data)
            
            # Check if this is a stage1 OCR file or enhanced file
            is_stage1_file = 'stage1_ocr' in json_file.name
            
            # TODO: Process document file - implementation removed during cleanup
            log.warning(f"Document processing not implemented for {json_file.name}")
                
        except Exception as e:
            log.error(f"❌ Error processing document {json_file.name}: {e}")
            raise





    def _extract_document_number_from_filename(self, filename: str) -> str:
        """Extract document number from filename with support for various patterns."""
        # Pattern 1: Standard ordinances like "2017-09" at beginning
        match = re.match(r'^(\d{4}-\d+)', filename)
        if match:
            return match.group(1)
        
        # Pattern 2: Special ordinances like "SOE-", "CG-", "EO-", "CAO-"
        special_match = re.match(r'^(SOE|CG|EO|CAO)[-\s]', filename)
        if special_match:
            # Return the full special identifier
            return filename.split('_stage1_ocr.json')[0].split('_enhanced_')[0]
        
        # Pattern 3: Amendment documents
        if filename.startswith('Amendment'):
            return filename.split('_stage1_ocr.json')[0].split('_enhanced_')[0]
        
        # Pattern 4: City Hall documents
        if filename.startswith('City Hall'):
            return filename.split('_stage1_ocr.json')[0].split('_enhanced_')[0]
        
        # Pattern 5: 190xxx series
        if filename.startswith('190'):
            match = re.match(r'^(190\d+)', filename)
            if match:
                return match.group(1)
        
        return ''











    async def push_from_merged_manifests(self, merged_dir: Path) -> Dict[str, int]:
        """Push deduplicated entities and relationships from merged manifests."""
        stats = {'vertices': 0, 'edges': 0, 'errors': 0}
        
        # Push entities
        entities_dir = merged_dir / "entities"
        if entities_dir.exists():
            for entity_file in entities_dir.glob("*.json"):
                with open(entity_file, 'r') as f:
                    data = json.load(f)
                
                entity_type = data['entity_type']
                label = self.optimizer.get_vertex_label_mapping().get(
                    entity_type, entity_type.lower()
                )
                
                for entity in data.get('entities', []):
                    try:
                        id_field = EntityIDStandards.get_id_field(entity_type)
                        entity_id = entity.get(id_field) or entity.get('id')
                        
                        if entity_id:
                            # Clean properties but keep it simple
                            props = {self._PK: self._PV}
                            for k, v in entity.items():
                                if not k.startswith('_') and v is not None:
                                    props[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
                            
                            await self._upsert_vertex(entity_id, label, props)
                            stats['vertices'] += 1
                    except Exception as e:
                        log.error(f"Failed to push {entity_type} {entity.get('id')}: {e}")
                        stats['errors'] += 1
                        continue  # Don't let one bad entity stop everything
        
        # Push relationships
        rel_file = merged_dir / "relationships.json"
        if rel_file.exists():
            with open(rel_file, 'r') as f:
                data = json.load(f)
            
            for rel in data.get('relationships', []):
                try:
                    await self._upsert_edge(
                        rel['source'],
                        self.sanitize_label(rel['type'], is_label=True),
                        rel['target'],
                        rel.get('attributes', {})
                    )
                    stats['edges'] += 1
                except Exception as e:
                    log.debug(f"Failed edge {rel}: {e}")
                    stats['errors'] += 1
                    continue
        
        log.info(f"✅ Pushed {stats['vertices']} vertices, {stats['edges']} edges ({stats['errors']} errors)")
        return stats

    def _reorder_properties(self, props: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reorder properties so document data appears first, system properties last.
        """
        # Properties that should appear LAST
        system_properties = {
            'group', 'degree', 'partitionKey', 'index',
            'x', 'y', 'vx', 'vy', 'fx', 'fy'
        }
        
        # Separate properties
        document_props = {}
        system_props = {}
        
        for key, value in props.items():
            if key in system_properties:
                system_props[key] = value
            else:
                document_props[key] = value
        
        # Return with document properties first
        reordered = {}
        reordered.update(document_props)
        reordered.update(system_props)
        
        return reordered 