"""
Custom graph builder for creating knowledge graphs in Cosmos DB.
"""

import hashlib
import re
import asyncio
import json
import logging as log
import os
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from collections import defaultdict
from datetime import datetime
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from scripts.graph_rag_stages.common.relationship_labels import normalize_rel_label
from scripts.graph_rag_stages.common.config import get_config
from scripts.graph_rag_stages.common.temporal_utils import natural_item_sort_key
from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards
from scripts.graph_rag_stages.common.unified_ontology import UnifiedOntology
from scripts.graph_rag_stages.common.entity_id_standards import EntityIDStandards
from scripts.graph_rag_stages.common.graph_entity_toolkit import GraphEntityToolkit
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
            'VoteOutcome': 'voteoutcome',
            'Section': 'section'
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
        
        # Get partition value from config or environment
        partition_value = (cosmos_config.get("partitionValue") if cosmos_config else None
                          or os.getenv("COSMOS_PARTITION_VALUE")
                          or "demo")
        
        # Initialize Cosmos client with partition value
        self.cosmos_client = CosmosGraphClient(
            endpoint=self.config.cosmos_endpoint,
            key=self.config.cosmos_key,
            database=self.config.cosmos_database,
            container=self.config.cosmos_container,
            partition_value=partition_value
        )
        
        log.info(f"🔧 CustomGraphBuilder initialized with partition value: '{partition_value}'")
        
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
        
        # Track processed vertices and edges to avoid duplicate operations
        self._processed_vertices = set()
        self._processed_edges = set()
        
        # Cache for frequently accessed vertices
        self._vertex_cache = {}
        self._cache_ttl = 300  # 5 minutes
        # Keep sanitization consistent with deduplicator/tooling
        try:
            self.toolkit = GraphEntityToolkit()
        except Exception:
            self.toolkit = None
    
    def _agenda_item_vertex_id(self, code: str, meeting_date: str) -> str:
        """Centralized AgendaItem ID formatting helper."""
        normalized_date = (meeting_date or "").replace("-", "_").replace(".", "_")
        code_norm = (code or "").lower().replace("-", "_")
        return self._sanitize_id(f"agenda_item_{code_norm}_{normalized_date}")
    
    def _agenda_item_parent_id(self, code: str, meeting_date: str) -> str:
        """Use the same canonical AgendaItem vertex ID everywhere."""
        return self._agenda_item_vertex_id(code, meeting_date)

    async def _execute_with_retry(self, query: str, max_retries: int = 1) -> List[Any]:
        """Execute query with minimal retry logic for PreconditionFailed errors."""
        for attempt in range(max_retries):
            try:
                return await self.cosmos_client._execute_query(query)
            except Exception as e:
                # Only retry PreconditionFailed errors once
                if "PreconditionFailed" in str(e) and attempt < max_retries - 1:
                    # Very short wait - just enough to let concurrent operation complete
                    await asyncio.sleep(0.01)
                    continue
                # Don't retry other errors
                raise

    def sanitize_label(self, s: str, is_label: bool = False) -> str:
        """Sanitize: alphanum + _, ≤63 chars for labels/edges, ≤255 for vertices, hash if needed."""
        if s is None:
            s = ""
        
        # First apply strict character rules (similar to _sanitize_id concept but without circular call)
        s = str(s).strip()
        if not s:
            s = "unknown"
            
        # Replace non-alphanumeric with underscores
        s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
        
        # Length and validity checks
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
        
        # Skip if already processed in this session
        if id in self._processed_vertices:
            return id
        
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
        import time
        start_time = time.time()
        
        outV = self.sanitize_label(outV)
        inV = self.sanitize_label(inV)
        label = self.sanitize_label(label, is_label=True)
        
        # Check if nodes exist using count() which is supported in Azure Cosmos DB
        node_check_start = time.time()
        try:
            out_count = await self.cosmos_client._execute_query(f"g.V('{outV}').count()")
            in_count = await self.cosmos_client._execute_query(f"g.V('{inV}').count()")
            # Handle nested count result structure: [[count]] -> count
            out_exists = out_count and out_count[0] and (out_count[0][0] if isinstance(out_count[0], list) else out_count[0]) > 0
            in_exists = in_count and in_count[0] and (in_count[0][0] if isinstance(in_count[0], list) else in_count[0]) > 0
            if not out_exists or not in_exists:
                log.warning(f"Cannot create edge: nodes {outV} or {inV} don't exist")
                return 'skipped'
        except Exception as e:
            log.warning(f"Error checking vertex existence: {e}")
            # Continue with edge creation anyway, let it fail if vertices don't exist
        node_check_time = time.time() - node_check_start
        
        lock_key = (outV, label, inV)
        lock_wait_start = time.time()
        async with self.edge_locks[lock_key]:
            lock_wait_time = time.time() - lock_wait_start
            
            # Prop chain (similar quoting)
            prop_build_start = time.time()
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
            prop_build_time = time.time() - prop_build_start
            
            # Use proper upsert pattern for edges
            query_build_start = time.time()
            if prop_chain:
                query = f"g.V('{outV}').outE('{label}').where(inV().hasId('{inV}')).fold().coalesce(unfold(), g.V('{outV}').addE('{label}').to(g.V('{inV}'))){prop_chain}"
            else:
                query = f"g.V('{outV}').outE('{label}').where(inV().hasId('{inV}')).fold().coalesce(unfold(), g.V('{outV}').addE('{label}').to(g.V('{inV}')))"
            query_build_time = time.time() - query_build_start
            
            try:
                execute_start = time.time()
                await self._execute_with_retry(query)
                execute_time = time.time() - execute_start
                
                total_time = time.time() - start_time
                
                # Log timing for slow operations
                if total_time > 1.0:
                    log.warning(f"⚠️  [COSMOS_SLOW] Edge {outV}--[{label}]-->{inV} took {total_time:.3f}s")
                    log.warning(f"   ⏱️  Node check: {node_check_time:.3f}s, Lock wait: {lock_wait_time:.3f}s, Prop build: {prop_build_time:.3f}s, Query build: {query_build_time:.3f}s, Execute: {execute_time:.3f}s")
                elif total_time > 0.5:
                    log.debug(f"🐌 [COSMOS_DEBUG] Edge {outV}--[{label}]-->{inV} took {total_time:.3f}s (execute: {execute_time:.3f}s)")
                
                log.debug(f"Upserted edge {outV} -[{label}]-> {inV}")
                return 'upserted'
            except Exception as e:
                total_time = time.time() - start_time
                if "conflict" in str(e).lower() or "already exists" in str(e).lower():
                    log.debug(f"Ignored duplicate edge {outV} -[{label}]-> {inV}")
                    return 'skipped'
                log.error(f"❌ [COSMOS_ERROR] Edge creation failed after {total_time:.3f}s: {e}")
                raise
            finally:
                del self.edge_locks[lock_key]  # Prune after use

    async def _optimized_upsert_vertex(self, entity_id: str, entity_type: str, properties: Dict) -> str:
        """Optimized vertex creation with caching and proper labeling."""
        
        # ADD THIS LINE:
        properties = self._reorder_properties(properties)
        
        # Get optimized label
        label_mapping = self.optimizer.get_vertex_label_mapping()
        optimized_label = label_mapping.get(entity_type, entity_type.lower())
        
        # Ensure partitionKey is set with safe default
        properties.setdefault('partitionKey', self.config.get('partition_key_value', 'cgGraph'))
        
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

    # (Removed the older _upsert_edge_with_entity_creation variant that took 'entity_map'.)
    
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
                    'created_at': datetime.now().isoformat(),
                    'Source_File_Name': 'system_generated',
                    'Source_File_Path': 'cosmos_db_search_index'
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
                    'created_at': datetime.now().isoformat(),
                    'Source_File_Name': 'system_generated',
                    'Source_File_Path': 'cosmos_db_composite_index'
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


    
    async def _process_entity_type(self, entity_dir: Path, entity_type: str, entity_map: Dict) -> int:
        """Process entities with parallel processing for better performance."""
        count = 0
        seen_ids = set()
        failed_entities = []
        
        # Collect all entities first
        all_entities_to_process = []
        
        for json_file in entity_dir.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                entities = data.get('entities', [])
                chunk_id = data.get('chunk_id', '')
                source_file = data.get('source_file', '')
                source_path = data.get('source_path', '')
                
                # Get metadata from parent structure
                chunk_metadata = data.get('_chunk_metadata', {})
                if not source_file and chunk_metadata:
                    source_file = chunk_metadata.get('source_file_name', '')
                if not source_path:
                    source_path = data.get('source_path', '')
                
                for entity in entities:
                    all_entities_to_process.append({
                        'entity': entity,
                        'chunk_id': chunk_id,
                        'source_file': source_file,
                        'source_path': source_path,
                        'json_file': json_file.name
                    })
                        
            except Exception as e:
                log.error(f"Error reading {json_file}: {e}")
                continue
        
        # Process entities in parallel batches
        batch_size = 50  # Process 50 entities concurrently
        for i in range(0, len(all_entities_to_process), batch_size):
            batch = all_entities_to_process[i:i + batch_size]
            
            async def process_single_entity(entity_data):
                nonlocal count
                entity = entity_data['entity']
                chunk_id = entity_data['chunk_id']
                source_file = entity_data['source_file']
                source_path = entity_data['source_path']
                
                try:
                    # Get entity ID with improved extraction
                    entity_id = self._get_entity_id(entity, entity_type)
                    if not entity_id:
                        # Try to generate ID from entity data
                        entity_id = self._generate_fallback_entity_id(entity, entity_type)
                        if not entity_id:
                            log.warning(f"Skipping entity without ID: {entity}")
                            return None
                    
                    # Normalize document IDs to match taxonomy format
                    if entity_type == 'Document':
                        entity_id = self._normalize_document_id(entity_id)
                    
                    # Check for duplicates by ID
                    if entity_id in seen_ids:
                        return None
                    
                    seen_ids.add(entity_id)
                    
                    # ALWAYS add to entity map, even before creation attempt
                    entity_map[entity_id] = entity_type
                    
                    # Build properties with all available data
                    properties = self._build_vertex_properties(entity, entity_type, chunk_id, source_file, source_path)
                    
                    # Create vertex with retry logic
                    try:
                        result = await self._optimized_upsert_vertex(
                            entity_id,
                            entity_type,
                            properties
                        )
                        
                        if result in ['upserted', 'cached']:
                            return 1  # Success
                        else:
                            # Even if skipped, keep in entity map
                            log.debug(f"Entity {entity_id} already exists, kept in map")
                            return 0
                            
                    except Exception as e:
                        log.warning(f"Failed to create vertex {entity_id}: {e}")
                        failed_entities.append({
                            'entity': entity,
                            'error': str(e),
                            'entity_id': entity_id
                        })
                        # Keep in entity map anyway for relationship creation
                        return 0
                        
                except Exception as e:
                    log.error(f"Error processing individual entity: {e}")
                    failed_entities.append({
                        'entity': entity,
                        'error': str(e),
                        'file': entity_data.get('json_file', 'unknown')
                    })
                    return 0
            
            # Process batch concurrently
            results = await asyncio.gather(*[process_single_entity(entity_data) for entity_data in batch])
            count += sum(r for r in results if r is not None)
        
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
            # Use GraphEntityToolkit for consistent ID generation
            key_attrs = {'name': base_id}
            return GraphEntityToolkit.generate_entity_id(entity_type, key_attrs)
        
        # Generate from hash of entity content as last resort
        entity_str = json.dumps(entity, sort_keys=True)
        hash_id = hashlib.sha256(entity_str.encode()).hexdigest()[:12]
        return self.sanitize_label(f"{entity_type.lower()}_auto_{hash_id}")
    
    def _get_entity_id(self, entity: Dict, entity_type: str) -> Optional[str]:
        """Resolve an entity's ID using shared standards, with safe fallbacks."""
        try:
            # Normalize field names first (aligns keys like agendaItemID vs itemID, etc.)
            normalized = EntityIDStandards.normalize_entity_id_fields(dict(entity), entity_type)
        except Exception:
            normalized = entity

        # Prefer the canonical field for this entity type
        id_field = None
        try:
            id_field = EntityIDStandards.get_id_field(entity_type)
        except Exception:
            id_field = None

        for key in filter(None, [id_field, 'id']):
            v = normalized.get(key)
            if v and str(v).strip():
                return self.sanitize_label(str(v))

        # Fallbacks: derive deterministic ID from meaningful fields
        for field in ('name', 'title', 'code', 'number', 'reference', 'description'):
            v = normalized.get(field)
            if v and str(v).strip():
                # reuse your existing fallback generator
                return self._generate_fallback_entity_id({field: v}, entity_type)

        log.warning(f"No ID found for {entity_type} entity: {entity}")
        return None
    

    
    async def _process_relationships(self, rel_dir: Path, entity_map: Dict) -> int:
        """Process relationships with parallel batching for better performance."""
        count = 0
        missing_entities = defaultdict(set)  # Track missing entities by type
        created_auto_entities = set()
        
        # Collect all relationships first
        all_relationships_to_process = []
        
        for json_file in rel_dir.glob("*.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                relationships = data.get('relationships', [])
                for rel in relationships:
                    all_relationships_to_process.append({
                        'rel': rel,
                        'json_file': json_file.name
                    })
                            
            except Exception as e:
                log.error(f"Error processing relationships file {json_file}: {e}")
                continue
        
        # Process relationships in parallel batches
        batch_size = 100  # Process 100 relationships concurrently
        for i in range(0, len(all_relationships_to_process), batch_size):
            batch = all_relationships_to_process[i:i + batch_size]
            
            async def process_single_relationship(rel_data):
                nonlocal count
                rel = rel_data['rel']
                
                try:
                    rel_type = rel.get('type')
                    source_id = self.sanitize_label(str(rel.get('source', '')))
                    target_id = self.sanitize_label(str(rel.get('target', '')))
                    attributes = rel.get('attributes', {})
                    
                    # Normalize document IDs in relationships
                    source_type = entity_map.get(source_id) or self._infer_entity_type_from_id(source_id)
                    target_type = entity_map.get(target_id) or self._infer_entity_type_from_id(target_id)
                    
                    if source_type == 'Document':
                        source_id = self._normalize_document_id(source_id)
                    if target_type == 'Document':
                        target_id = self._normalize_document_id(target_id)
                    
                    if not all([rel_type, source_id, target_id]):
                        log.debug(f"Skipping incomplete relationship: {rel}")
                        return 0
                    
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
                                            'created_for_relationship': rel_type,
                                            'Source_File_Name': 'auto_created',
                                            'Source_File_Path': f'auto_created_for_{rel_type}_relationship'
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
                            return 1
                        return 0
                            
                    except Exception as e:
                        if "don't exist" in str(e):
                            log.debug(f"Edge creation failed - missing nodes: {source_id} -> {target_id}")
                            missing_entities['edge_failure'].add(f"{source_id}->{target_id}")
                        else:
                            log.warning(f"Error creating edge {rel_type}: {e}")
                        return 0
                    
                except Exception as e:
                    log.error(f"Error processing relationship: {e}")
                    return 0
            
            # Process batch concurrently
            results = await asyncio.gather(*[process_single_relationship(rel_data) for rel_data in batch])
            count += sum(results)
        
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

    def _build_vertex_properties(self, entity: Dict, entity_type: str, chunk_id: str, source_file: str, source_path: str = '') -> Dict:
        """Build comprehensive vertex properties preserving all entity data."""
        # Start with partition key
        props = {self._PK: self._PV}
        
        # Add extraction metadata
        props['extraction_chunk_id'] = chunk_id
        props['extraction_source_file'] = source_file
        props['entity_type'] = entity_type
        props['extracted_at'] = datetime.now().isoformat()
        
        # Add source file attributes
        if source_file:
            props['Source_File_Name'] = source_file
        if source_path:
            props['Source_File_Path'] = source_path
        
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
    def _normalize_rel_label(self, label: str) -> str:
        """Normalize relationship labels using shared normalizer to keep Stage 4 and Stage 5 aligned."""
        return normalize_rel_label(label)

    async def _upsert_edge_with_entity_creation(
        self,
        source_id: str,
        edge_label: str,
        target_id: str,
        attributes: dict,
        entity_type_map: dict  # {entity_id: "Person" | "Organization" | ...}
    ):
        """Safe edge upsert that can auto-create missing endpoints (rare, but happens)."""
        import time
        start_time = time.time()
        
        # Normalize document IDs to match taxonomy format
        source_type = entity_type_map.get(source_id) or self._infer_entity_type_from_id(source_id)
        target_type = entity_type_map.get(target_id) or self._infer_entity_type_from_id(target_id)
        
        if source_type == 'Document':
            source_id = self._normalize_document_id(source_id)
        if target_type == 'Document':
            target_id = self._normalize_document_id(target_id)
        
        # Track timing for vertex existence checks
        vertex_check_start = time.time()
        
        # ensure source exists
        source_exists_start = time.time()
        source_exists = await self._vertex_exists(source_id)
        source_exists_time = time.time() - source_exists_start
        
        if not source_exists:
            et = source_type
            source_create_start = time.time()
            await self._upsert_vertex(
                source_id,
                (et or "Unknown").lower(),
                {
                    getattr(self, "_PK", "pk"): getattr(self, "_PV", "cgGraph"),
                    'Source_File_Name': 'auto_created',
                    'Source_File_Path': f'auto_created_for_{edge_label}_edge'
                }
            )
            source_create_time = time.time() - source_create_start
            log.debug(f"🔧 [COSMOS_DEBUG] Created missing source vertex '{source_id}' in {source_create_time:.3f}s")
        
        # ensure target exists
        target_exists_start = time.time()
        target_exists = await self._vertex_exists(target_id)
        target_exists_time = time.time() - target_exists_start
        
        if not target_exists:
            et = target_type
            target_create_start = time.time()
            await self._upsert_vertex(
                target_id,
                (et or "Unknown").lower(),
                {
                    getattr(self, "_PK", "pk"): getattr(self, "_PV", "cgGraph"),
                    'Source_File_Name': 'auto_created', 
                    'Source_File_Path': f'auto_created_for_{edge_label}_edge'
                }
            )
            target_create_time = time.time() - target_create_start
            log.debug(f"🔧 [COSMOS_DEBUG] Created missing target vertex '{target_id}' in {target_create_time:.3f}s")
        
        vertex_check_total = time.time() - vertex_check_start
        
        # finally, upsert the edge
        edge_start = time.time()
        await self._upsert_edge(source_id, edge_label, target_id, attributes or {})
        edge_time = time.time() - edge_start
        
        total_time = time.time() - start_time
        
        # Log detailed timing info every 50 edges or if operation takes >1 second
        if total_time > 1.0 or (hasattr(self, '_edge_debug_counter') and self._edge_debug_counter % 50 == 0):
            log.info(f"⏱️  [COSMOS_TIMING] Edge {edge_label}: {total_time:.3f}s total")
            log.info(f"   📊 Vertex checks: {vertex_check_total:.3f}s (src: {source_exists_time:.3f}s, tgt: {target_exists_time:.3f}s)")
            log.info(f"   🔗 Edge upsert: {edge_time:.3f}s")
            log.info(f"   🎯 Vertices existed: src={source_exists}, tgt={target_exists}")
        
        # Initialize and increment debug counter
        if not hasattr(self, '_edge_debug_counter'):
            self._edge_debug_counter = 0
        self._edge_debug_counter += 1

    async def _create_missing_vertex(self, vertex_id: str, entity_type: str):
        """Create a missing vertex with minimal properties."""
        try:
            label = self.optimizer.get_vertex_label_mapping().get(entity_type, entity_type.lower())
            props = {self._PK: self._PV, 'id': vertex_id}
            await self._upsert_vertex(vertex_id, label, props)
            log.debug(f"🔧 [COSMOS_FIX] Created missing {entity_type} vertex: {vertex_id}")
        except Exception as e:
            log.warning(f"Failed to create missing vertex {vertex_id}: {e}")
    
    async def _upsert_edge_direct(self, source_id: str, edge_label: str, target_id: str, attributes: dict):
        """Direct edge upsert without existence checks (assumes vertices exist)."""
        # Build edge attributes
        attr_clauses = []
        for key, value in attributes.items():
            if value is not None:
                if isinstance(value, str):
                    attr_clauses.append(f".property('{key}', '{value}')")
                else:
                    attr_clauses.append(f".property('{key}', {json.dumps(value)})")
        
        attr_string = ''.join(attr_clauses)
        
        query = (f"g.V('{source_id}').coalesce("
                f"outE('{edge_label}').where(inV().hasId('{target_id}')), "
                f"addE('{edge_label}').to(g.V('{target_id}'))){attr_string}")
        
        await self.cosmos_client._execute_query(query)

    def _normalize_document_id(self, entity_id: str) -> str:
        """Normalize document IDs to match taxonomy format."""
        id_lower = entity_id.lower()
        
        # Handle AgendaDocument IDs -> Document IDs
        if id_lower.startswith('agendadocument_') or id_lower.startswith('agendadoc_'):
            # Convert agendadocument_agenda_2024_01_09_abc123 -> document_agenda_2024_01_09
            id_lower = id_lower.replace('agendadocument_', 'document_').replace('agendadoc_', 'document_')
        
        # Special handling for ordinances and resolutions - they should already have correct format
        # Pattern: document_ordinance_YYYY_NN_HASH or document_resolution_YYYY_NN_HASH
        if id_lower.startswith('document_ordinance_') or id_lower.startswith('document_resolution_'):
            # These IDs are already in the correct format from EntityIDStandards.make_policy_id
            return id_lower
        
        # Remove _enhanced_ordinance suffix if present
        if '_enhanced_ordinance' in id_lower:
            id_lower = id_lower.replace('_enhanced_ordinance', '')
        
        # Remove random suffixes like _abc123, _xyz789
        # Pattern: underscore followed by 3+ alphanumeric chars at the end
        if '_' in id_lower:
            parts = id_lower.split('_')
            if len(parts) > 1 and len(parts[-1]) >= 3 and parts[-1].isalnum():
                # Check if last part looks like a random suffix (mix of letters and numbers)
                last_part = parts[-1]
                has_letters = any(c.isalpha() for c in last_part)
                has_numbers = any(c.isdigit() for c in last_part)
                if has_letters and has_numbers:
                    # Remove the suffix
                    id_lower = '_'.join(parts[:-1])
        
        # Fix date ordering: document_agenda_01_09_2024 -> document_agenda_2024_01_09
        if 'document_agenda_' in id_lower:
            # Pattern: document_agenda_DD_MM_YYYY
            import re
            match = re.search(r'document_agenda_(\d{2})_(\d{2})_(\d{4})', id_lower)
            if match:
                day, month, year = match.groups()
                # Convert to taxonomy format: document_agenda_YYYY_DD_MM (taxonomy uses DD_MM, not MM_DD)
                id_lower = f'document_agenda_{year}_{day}_{month}'
        
        return id_lower

    def _infer_entity_type_from_id(self, entity_id: str) -> Optional[str]:
        """Infer entity type from ID patterns."""
        id_lower = entity_id.lower()
        
        # Common ID patterns
        if id_lower.startswith('person_') or '_person_' in id_lower:
            return 'Person'
        elif id_lower.startswith('org_') or id_lower.startswith('organization_'):
            return 'Organization'
        elif id_lower.startswith('document_ordinance_') or id_lower.startswith('document_resolution_'):
            # Ordinances and resolutions are Policy entities in taxonomy
            return 'Policy'
        elif id_lower.startswith('document_') or id_lower.startswith('doc_'):
            return 'Document'
        elif id_lower.startswith('agendadocument_') or id_lower.startswith('agendadoc_'):
            return 'Document'  # Map AgendaDocument to Document
        elif id_lower.startswith('policy_'):
            return 'Policy'
        elif id_lower.startswith('event_') or id_lower.startswith('meeting_'):
            return 'Event'
        elif id_lower.startswith('agenda_item_') or id_lower.startswith('agendaitem_'):
            return 'AgendaItem'
        elif id_lower.startswith('location_'):
            return 'Location'
        elif id_lower.startswith('topic_'):
            return 'Topic'
        elif id_lower.startswith('section_'):
            return 'Section'
        elif id_lower.startswith('role_'):
            return 'Role'
        elif id_lower.startswith('contract_'):
            return 'Contract'
        elif id_lower.startswith('asset_'):
            return 'Asset'
        elif id_lower.startswith('project_'):
            return 'Project'
        elif id_lower.startswith('technology_'):
            return 'Technology'
        elif id_lower.startswith('voteoutcome_'):
            return 'VoteOutcome'
        
        return None

    async def _bulk_vertex_exists(self, vertex_ids: List[str]) -> Dict[str, bool]:
        """Bulk check if vertices exist - much more efficient than individual checks."""
        if not vertex_ids:
            return {}
        
        import time
        start_time = time.time()
        
        try:
            # Build query to check all vertices at once
            vertex_list = "', '".join(vertex_ids)
            query = f"g.V('{vertex_list}').project('id', 'exists').by('id').by(constant(true))"
            
            result = await self.cosmos_client._execute_query(query)
            query_time = time.time() - start_time
            
            # Create existence map - default all to False, then set True for existing ones
            existence_map = {vid: False for vid in vertex_ids}
            if result:
                for item in result:
                    if isinstance(item, dict) and 'id' in item:
                        existence_map[item['id']] = True
            
            log.debug(f"🔍 [BULK_CHECK] Checked {len(vertex_ids)} vertices in {query_time:.3f}s")
            return existence_map
            
        except Exception as e:
            log.warning(f"Bulk vertex check failed, falling back to individual checks: {e}")
            # Fallback to individual checks
            results = {}
            for vid in vertex_ids:
                results[vid] = await self._vertex_exists_individual(vid)
            return results
    
    async def _vertex_exists(self, vertex_id: str) -> bool:
        """Check if a vertex exists by doing a cheap point lookup by id."""
        return await self._vertex_exists_individual(vertex_id)
    
    async def _vertex_exists_individual(self, vertex_id: str) -> bool:
        """Individual vertex existence check - fallback method."""
        import time
        start_time = time.time()
        try:
            result = await self.cosmos_client._execute_query(f"g.V('{vertex_id}').count()")
            query_time = time.time() - start_time
            # Handle nested count result structure: [[count]] -> count
            exists = result and result[0] and (result[0][0] if isinstance(result[0], list) else result[0]) > 0
            
            # Log slow vertex existence checks
            if query_time > 0.5:
                log.warning(f"⚠️  [COSMOS_SLOW] Vertex existence check for '{vertex_id}' took {query_time:.3f}s")
            elif query_time > 0.2:
                log.debug(f"🐌 [COSMOS_DEBUG] Vertex existence check for '{vertex_id}' took {query_time:.3f}s")
            
            return exists
        except Exception as e:
            query_time = time.time() - start_time
            log.error(f"❌ [COSMOS_ERROR] Vertex existence check failed for '{vertex_id}' after {query_time:.3f}s: {e}")
            return False

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
        """Single source of truth: reuse sanitize_label()."""
        cleaned = self.sanitize_label(id_str, is_label=False)
        return cleaned or "unknown"

    async def _process_ontology_file(self, p: Path) -> None:
        data = json.loads(p.read_text(encoding="utf‑8"))
        doc_id = data.get("doc_id") or self.sanitize_label(f"doc-{p.stem}")
        meeting_date = (data.get("meeting_date") or "UNKNOWN").replace(".", "-")
        meeting_id = self._generate_meeting_id(meeting_date)
        
        # Extract hyperlinks for URL attributes
        hyperlinks = data.get("hyperlinks", [])

        # 1️⃣ EVENT vertex - add sourceURL
        source_file_name = data.get("Source_File_Name", data.get("source_file", p.name))
        source_file_path = data.get("Source_File_Path", str(p))
        
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
             "Source_File_Name": source_file_name,
             "Source_File_Path": source_file_path,
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
                 "parent_agenda_doc_id": agenda_doc_id,  # NEW: Add parent agenda ID
                 "Source_File_Name": source_file_name,
                 "Source_File_Path": source_file_path
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
                     # Keep existing 'itemID' for backward compatibility
                     "itemID": code,
                     # NEW: also write the canonical field so every consumer can rely on it
                     "agendaItemID": code,
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
                     "urls": json.dumps([link.get("url") for link in it.get("urls", [])]) if it.get("urls") else None,
                     "Source_File_Name": source_file_name,
                     "Source_File_Path": source_file_path
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
                         "parent_agenda_item_id": self._agenda_item_parent_id(ref_code, meeting_date) if ref_code else None,  # NEW: Add parent agenda item ID + date fix
                         "Source_File_Name": source_file_name,
                         "Source_File_Path": source_file_path
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
                    await self._upsert_vertex(pid, "person", {
                        self._PK: self._PV, 
                        "name": person,
                        "Source_File_Name": source_file_name,
                        "Source_File_Path": source_file_path
                    })
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
                "extraction_timestamp": datetime.now().isoformat(),
                "Source_File_Name": source_file_name,
                "Source_File_Path": source_file_path
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
        entity_type_map = {}  # id -> entity_type
        
        # Clear tracking sets for new push
        self._processed_vertices.clear()
        self._processed_edges.clear()
        
        log.info(f"🚀 Starting Cosmos DB push with partition configuration:")
        log.info(f"   🔑 Partition Key: '{self._PK}'")
        log.info(f"   🏷️ Partition Value: '{self._PV}'")
        log.info(f"   🌐 Endpoint: {self.config.cosmos_endpoint}")
        log.info(f"   🗄️ Database: {self.config.cosmos_database}")
        log.info(f"   📦 Container: {self.config.cosmos_container}")
        
        # Push entities with connection refresh checkpoints
        entities_dir = merged_dir / "entities"
        total_entities_to_push = 0
        
        if entities_dir.exists():
            entity_files = list(entities_dir.glob("*.json"))
            
            # Count total entities across all types for summary
            for entity_file in entity_files:
                with open(entity_file, 'r') as f:
                    data = json.load(f)
                    entities_count = len(data.get('entities', []))
                    total_entities_to_push += entities_count
            
            log.info(f"📦 COSMOS PUSH - ENTITIES:")
            log.info(f"   📊 Total entities to push: {total_entities_to_push}")
            log.info(f"   📂 Entity type files: {len(entity_files)}")
            log.info(f"   📋 Entity types: {[f.stem for f in entity_files]}")
            log.info(f"")
            
            for file_idx, entity_file in enumerate(entity_files, 1):
                with open(entity_file, 'r') as f:
                    data = json.load(f)
                
                entity_type = data['entity_type']
                label = self.optimizer.get_vertex_label_mapping().get(
                    entity_type, entity_type.lower()
                )
                
                entities_list = data.get('entities', [])
                log.info(f"📤 Processing {entity_type} ({file_idx}/{len(entity_files)}): {len(entities_list)} entities")
                
                # Connection refresh checkpoint between entity types
                if file_idx > 1:  # Skip for first entity type
                    log.info(f"🔄 Connection refresh checkpoint before {entity_type} processing")
                    await self._refresh_connection()
                
                # Sort entities by ID to ensure consistent processing order
                entities_list = sorted(entities_list, key=lambda e: e.get(EntityIDStandards.get_id_field(entity_type)) or e.get('id', ''))
                
                # Process entities in parallel batches with checkpoints
                batch_size = 50  # Increased for parallel processing (50 concurrent upserts)
                entity_batches = [entities_list[i:i + batch_size] for i in range(0, len(entities_list), batch_size)]
                
                for batch_idx, entity_batch in enumerate(entity_batches):
                    # Checkpoint every few batches to refresh connection
                    if batch_idx > 0 and batch_idx % 2 == 0:  # Every 100 entities (2 * 50)
                        log.debug(f"🔄 Mid-processing connection refresh for {entity_type} (batch {batch_idx + 1})")
                        await self._refresh_connection()
                    
                    # Process entities in current batch with parallel processing
                    async def process_single_entity(entity):
                        """Process a single entity with error handling."""
                        try:
                            id_field = EntityIDStandards.get_id_field(entity_type)
                            entity_id = entity.get(id_field) or entity.get('id')
                            
                            if entity_id:
                                # Skip if already processed
                                if entity_id in self._processed_vertices:
                                    return 0
                                
                                # Clean properties but keep it simple
                                props = {self._PK: self._PV}
                                for k, v in entity.items():
                                    if not k.startswith('_') and v is not None:
                                        props[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
                                
                                await self._upsert_vertex(entity_id, label, props)
                                entity_type_map[entity_id] = entity_type
                                self._processed_vertices.add(entity_id)
                                return 1
                        except Exception as e:
                            log.error(f"Failed to push {entity_type} {entity.get('id')}: {e}")
                            return -1  # Error marker
                        return 0
                    
                    # Process batch with parallel execution
                    log.debug(f"🔥 [PARALLEL_ENTITIES] Processing batch {batch_idx + 1}: {len(entity_batch)} entities concurrently")
                    results = await asyncio.gather(*[process_single_entity(entity) for entity in entity_batch])
                    
                    # Update stats from parallel results
                    batch_successes = sum(r for r in results if r == 1)
                    batch_errors = sum(1 for r in results if r == -1)
                    log.debug(f"✅ [PARALLEL_ENTITIES] Batch {batch_idx + 1} complete: {batch_successes} success, {batch_errors} errors")
                    stats['vertices'] += batch_successes
                    stats['errors'] += batch_errors
                    
                    # Log progress every 50 vertices
                    if stats['vertices'] % 50 == 0:
                        log.info(f"   Progress: {stats['vertices']} vertices pushed...")
        
        # Push relationships
        rel_file = merged_dir / "relationships.json"
        total_relationships_to_push = 0
        
        if rel_file.exists():
            with open(rel_file, 'r') as f:
                data = json.load(f)
            
            relationships = data.get('relationships', [])
            total_relationships_to_push = len(relationships)
            
            log.info(f"🔗 COSMOS PUSH - RELATIONSHIPS:")
            log.info(f"   📊 Total relationships to push: {total_relationships_to_push}")
            log.info(f"")
            
            # Sort relationships for consistent processing
            relationships = sorted(relationships, key=lambda r: (r.get('source', ''), r.get('type', ''), r.get('target', '')))
            
            # Process relationships with bulk existence checks and parallel processing
            import time
            batch_start_time = time.time()
            batch_size = 25  # Process relationships in parallel batches
            relationship_batches = [relationships[i:i + batch_size] for i in range(0, len(relationships), batch_size)]
            total_batches = len(relationship_batches)
            
            log.info(f"🚀 [PARALLEL_RELATIONSHIPS] Processing {total_batches} batches of {batch_size} relationships")
            
            for batch_idx, rel_batch in enumerate(relationship_batches):
                batch_individual_start = time.time()
                
                # Connection refresh checkpoint every few batches
                if batch_idx > 0 and batch_idx % 4 == 0:  # Every 100 relationships (4 * 25)
                    log.info(f"🔄 Connection refresh checkpoint at batch {batch_idx + 1}/{total_batches}")
                    await self._refresh_connection()
                
                # Collect all unique vertex IDs from this batch for bulk existence check
                vertex_ids = set()
                valid_relationships = []
                
                for rel in rel_batch:
                    src = rel.get("source")
                    tgt = rel.get("target")
                    if src and tgt:
                        # Normalize IDs like the original code
                        source_type = entity_type_map.get(src) or self._infer_entity_type_from_id(src)
                        target_type = entity_type_map.get(tgt) or self._infer_entity_type_from_id(tgt)
                        
                        if source_type == 'Document':
                            src = self._normalize_document_id(src)
                        if target_type == 'Document':
                            tgt = self._normalize_document_id(tgt)
                        
                        vertex_ids.add(src)
                        vertex_ids.add(tgt)
                        valid_relationships.append({
                            'original': rel,
                            'source': src,
                            'target': tgt,
                            'source_type': source_type,
                            'target_type': target_type
                        })
                
                # Bulk existence check for all vertices in this batch
                existence_map = await self._bulk_vertex_exists(list(vertex_ids))
                
                # Process relationships in parallel with cached existence results
                async def process_single_relationship(rel_data):
                    """Process a single relationship with cached existence data."""
                    try:
                        rel = rel_data['original']
                        src = rel_data['source']
                        tgt = rel_data['target']
                        
                        label = self._normalize_rel_label(rel.get("type"))
                        label = self.sanitize_label(label, is_label=True) if hasattr(self, "sanitize_label") else label
                        edge_key = (src, label, tgt)
                        
                        # Skip if already processed
                        if edge_key in self._processed_edges:
                            return 0
                        
                        # Use cached existence results instead of individual queries
                        source_exists = existence_map.get(src, False)
                        target_exists = existence_map.get(tgt, False)
                        
                        # Create missing vertices if needed (same logic as original)
                        if not source_exists:
                            source_type = rel_data['source_type']
                            if source_type:
                                await self._create_missing_vertex(src, source_type)
                        
                        if not target_exists:
                            target_type = rel_data['target_type']
                            if target_type:
                                await self._create_missing_vertex(tgt, target_type)
                        
                        # Create the edge
                        await self._upsert_edge_direct(src, label, tgt, rel.get("attributes", {}))
                        self._processed_edges.add(edge_key)
                        return 1
                        
                    except Exception as e:
                        log.debug(f"Failed relationship {rel_data['original']}: {e}")
                        return -1
                
                # Process batch in parallel
                log.debug(f"🔥 [PARALLEL_RELATIONSHIPS] Processing batch {batch_idx + 1}: {len(valid_relationships)} relationships concurrently")
                results = await asyncio.gather(*[process_single_relationship(rel_data) for rel_data in valid_relationships])
                
                # Update statistics
                batch_successes = sum(r for r in results if r == 1)
                batch_errors = sum(1 for r in results if r == -1)
                stats['edges'] += batch_successes
                stats['errors'] += batch_errors
                
                batch_time = time.time() - batch_individual_start
                log.debug(f"✅ [PARALLEL_RELATIONSHIPS] Batch {batch_idx + 1} complete: {batch_successes} success, {batch_errors} errors ({batch_time:.2f}s)")
                
                # Progress logging
                if stats['edges'] % 50 == 0:
                    total_batch_time = time.time() - batch_start_time
                    avg_time_per_edge = total_batch_time / stats['edges'] if stats['edges'] > 0 else 0
                    log.info(f"🚀 [COSMOS_PROGRESS] {stats['edges']} edges pushed ({avg_time_per_edge:.3f}s avg/edge)")
                    
                    remaining = len(relationships) - stats['edges'] - stats['errors']
                    if remaining > 0 and avg_time_per_edge > 0:
                        eta_seconds = remaining * avg_time_per_edge
                        eta_minutes = eta_seconds / 60
                        log.info(f"   ⏰ Estimated {remaining} remaining, ETA: {eta_minutes:.1f} minutes")
        

        # Comprehensive final summary with detailed counts
        log.info(f"")
        log.info(f"🎉 COSMOS PUSH COMPLETE - FINAL SUMMARY:")
        log.info(f"   📊 ENTITIES: {stats['vertices']:,} pushed / {total_entities_to_push:,} total ({stats['vertices']/total_entities_to_push*100:.1f}% success)" if total_entities_to_push > 0 else f"   📊 ENTITIES: {stats['vertices']:,} pushed")
        log.info(f"   📊 RELATIONSHIPS: {stats['edges']:,} pushed / {total_relationships_to_push:,} total ({stats['edges']/total_relationships_to_push*100:.1f}% success)" if total_relationships_to_push > 0 else f"   📊 RELATIONSHIPS: {stats['edges']:,} pushed")
        log.info(f"   ❌ ERRORS: {stats['errors']:,}")
        log.info(f"   📈 TOTAL RECORDS: {stats['vertices'] + stats['edges']:,} successfully pushed to Cosmos DB")
        log.info(f"   🎯 SUCCESS RATE: {((stats['vertices'] + stats['edges']) / (total_entities_to_push + total_relationships_to_push) * 100):.1f}%" if (total_entities_to_push + total_relationships_to_push) > 0 else "N/A")
        log.info(f"")
        
        return stats
    
    async def _refresh_connection(self):
        """Refresh the Cosmos DB connection to prevent timeouts during long operations."""
        try:
            # Force a connection health check
            healthy = await self.cosmos_client._check_connection_health()
            if not healthy:
                log.warning("⚠️ Connection refresh failed, but continuing with existing connection")
        except Exception as e:
            log.warning(f"⚠️ Error during connection refresh: {e}")

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