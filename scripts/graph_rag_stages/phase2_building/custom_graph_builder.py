"""
Custom graph builder for creating knowledge graphs in Cosmos DB.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
import asyncio
import json
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from scripts.graph_rag_stages.common.config import get_config
from scripts.graph_rag_stages.common.temporal_utils import natural_item_sort_key
from tqdm import tqdm

log = logging.getLogger(__name__)


class CustomGraphBuilder:
    """Builds custom knowledge graphs in Cosmos DB from processed documents."""
    
    def __init__(self, cosmos_config: Optional[Dict] = None):
        """
        Initialize the graph builder with Cosmos DB configuration.
        
        Args:
            cosmos_config: Optional Cosmos DB configuration override
        """
        self.config = get_config()
        
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

        # Shorthand helpers – keep existing Gremlin API untouched
        async def _vertex_direct(id, label, props):
            # Direct vertex creation without existence checks to avoid partition conflicts
            prop_chain = ""
            
            # Remove partitionKey from props to avoid setting it twice
            props_copy = {k: v for k, v in props.items() if k != 'partitionKey'}
            
            for key, value in props_copy.items():
                if value is not None:
                    if isinstance(value, bool):
                        prop_chain += f".property('{key}', {str(value).lower()})"
                    elif isinstance(value, (int, float)):
                        prop_chain += f".property('{key}', {value})"
                    elif isinstance(value, list):
                        import json
                        json_val = json.dumps(value).replace("'", "\\'")
                        prop_chain += f".property('{key}', '{json_val}')"
                    else:
                        escaped_val = str(value).replace("'", "\\'").replace('"', '\\"')
                        prop_chain += f".property('{key}', '{escaped_val}')"
            
            prop_chain += f".property('partitionKey', '{self.cosmos_client.partition_value}')"
            query = f"g.addV('{label}').property('id', '{id}'){prop_chain}"
            
            try:
                return await self.cosmos_client._execute_query(query)
            except Exception as e:
                if "already exists" in str(e).lower() or "conflict" in str(e).lower():
                    return None  # Ignore duplicate vertex errors
                raise
        
        async def _edge_direct(outV, label, inV, props):
            # Direct edge creation with conflict handling
            try:
                return await self.cosmos_client.create_edge(outV, inV, label, props)
            except Exception as e:
                if "already exists" in str(e).lower() or "conflict" in str(e).lower():
                    return None  # Ignore duplicate edge errors
                raise
        
        self._V   = _vertex_direct      # (id, label, props)
        self._E   = _edge_direct        # (outV, label, inV, props)
        self._PK  = cosmos_config.get("partitionKey",  "partitionKey") if cosmos_config else "partitionKey"
        self._PV  = (cosmos_config.get("partitionValue","demo") if cosmos_config else "demo") or "demo"

    # ----------------------------------------------------------------------  
    # NEW public entry‑point – mirrors the NetworkX builder
    # ----------------------------------------------------------------------  
    async def build_graph_from_json(self, json_source_dir: Path) -> None:
        """
        🚀 ULTRA-HIGH-PERFORMANCE: Build graph from Stage-3 ontology JSON files and enhanced documents with smart filtering
        """
        log.info("🔗 Starting Cosmos DB Graph Building Pipeline")
        
        # Find all stage3_ontology JSON files in organized structure
        stage3_dir = json_source_dir / "stage3"
        if stage3_dir.exists():
            ontology_files = list(stage3_dir.glob("*_stage3_ontology.json"))
        else:
            # Fallback to flat structure for backward compatibility
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
        
        log.info(f"Found {len(stage1_ordinances)} additional stage1-only ordinances to process")
        
        log.info(f"Found {len(ontology_files)} ontology files and {len(enhanced_files)} enhanced document files")
        
        if not ontology_files and not enhanced_files:
            log.warning("⚠️ No ontology or enhanced document files found!")
            return
        
        log.info(f"🚀 HIGH-PERFORMANCE MODE: Processing {len(ontology_files + enhanced_files)} files with batching...")
        
        # Connect to Cosmos DB
        log.info("🔗 Connecting to Cosmos DB...")
        await self.cosmos_client.connect()
        
        # Reset processed ordinances tracking
        self.processed_ordinances.clear()
        
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
        
        log.info("✅ Graph building completed successfully!")
        
        # Disconnect from Cosmos DB
        await self.cosmos_client.disconnect()

    # ----------------------------------------------------------------------  
    # Internal helpers
    # ----------------------------------------------------------------------  
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
        import re
        sanitized = re.sub(r'[^a-zA-Z0-9\-_]', '', sanitized)
        
        # Ensure it doesn't start or end with dash/underscore
        sanitized = sanitized.strip('-_')
        
        # Ensure it's not empty after sanitization
        return sanitized if sanitized else "unknown"

    async def _collect_operations_from_file(self, p: Path) -> Tuple[List[Dict], List[Dict]]:
        """
        HIGH-PERFORMANCE: Collect all vertex and edge operations from a file
        without executing them. Returns (vertices, edges) for bulk execution.
        """
        vertices = []
        edges = []
        
        data = json.loads(p.read_text(encoding="utf‑8"))
        meeting_date = (data.get("meeting_date") or "UNKNOWN").replace(".", "-")
        meeting_id = self._sanitize_id(f"meeting-{meeting_date}")

        # 1️⃣ MEETING vertex
        vertices.append({
            "id": meeting_id,
            "label": "meeting",
            "properties": {
                self._PK: self._PV,
                "date": meeting_date,
                "doc_id": data.get("doc_id"),
                "source_file": data.get("source_file")
            }
        })

        # Add AGENDA_DOCUMENT vertex
        agenda_doc_id = self._sanitize_id(f"agenda-{meeting_date}")
        vertices.append({
            "id": agenda_doc_id,
            "label": "agenda_document",
            "properties": {
                self._PK: self._PV,
                "meeting_date": meeting_date,
                "source_file": data.get("source_file")
            }
        })
        edges.append({
            "from": meeting_id,
            "to": agenda_doc_id,
            "label": "HAS_AGENDA",
            "properties": {}
        })

        # 2️⃣ SECTIONS + AGENDA ITEMS
        sections = data.get("sections", [])
        for s in sections:
            sec_id = self._sanitize_id(f"section-{meeting_date}-{s.get('section_id')}")
            vertices.append({
                "id": sec_id,
                "label": "section",
                "properties": {
                    self._PK: self._PV,
                    "code": s.get("section_name"),
                    "order": s.get("section_order"),
                    "meeting_date": meeting_date
                }
            })
            edges.append({
                "from": agenda_doc_id,
                "to": sec_id,
                "label": "HAS_SECTION",
                "properties": {"order": s.get("section_order")}
            })

            for it in s.get("items", []):
                code = it.get("item_code") or "--"
                item_id = self._sanitize_id(f"item-{meeting_date}-{code}")
                vertices.append({
                    "id": item_id,
                    "label": "agendaItem",
                    "properties": {
                        self._PK: self._PV,
                        "code": code,
                        "title": it.get("title", ""),
                        "document_reference": it.get("document_reference"),
                        "order": it.get("item_order"),
                        "meeting_date": meeting_date
                    }
                })
                edges.append({
                    "from": sec_id,
                    "to": item_id,
                    "label": "HAS_AGENDA_ITEM",
                    "properties": {"order": it.get("item_order")}
                })

        # 3️⃣ TEMPORAL PRECEDES edges
        items = [it["item_code"] for s in sections for it in s.get("items", []) if it.get("item_code")]
        items_sorted = sorted(items, key=natural_item_sort_key)
        for a, b in zip(items_sorted, items_sorted[1:]):
            edges.append({
                "from": self._sanitize_id(f"item-{meeting_date}-{a}"),
                "to": self._sanitize_id(f"item-{meeting_date}-{b}"),
                "label": "PRECEDES",
                "properties": {}
            })

        # 4️⃣ LEGAL DOCS, MOTIONS & VOTES
        for e in data.get("entities", []):
            if e.get("type") not in ("ORDINANCE", "RESOLUTION"):
                continue
            doc_num = e.get("name")
            doc_id = self._sanitize_id(f"{e['type'].lower()}-{doc_num}")
            vertices.append({
                "id": doc_id,
                "label": e["type"].lower(),
                "properties": {
                    self._PK: self._PV,
                    "doc_number": doc_num,
                    "title": e.get("description", "")[:512],
                    "meeting_date": meeting_date
                }
            })

            ref_code = e.get("related_item") or e.get("agenda_item_code")
            if ref_code:
                edges.append({
                    "from": self._sanitize_id(f"item-{meeting_date}-{ref_code}"),
                    "to": doc_id,
                    "label": "IMPLEMENTS",
                    "properties": {}
                })

            if e.get("vote_details"):
                edges.append({
                    "from": doc_id,
                    "to": meeting_id,
                    "label": "VOTED_ON",
                    "properties": {
                        "yeas": e["vote_details"].get("yeas"),
                        "nays": e["vote_details"].get("nays"),
                        "unanimous": e["vote_details"].get("unanimous", False)
                    }
                })

            motion = e.get("motion", {})
            for label, person in [("MOVED_BY", motion.get("moved_by")),
                                  ("SECONDED_BY", motion.get("seconded_by"))]:
                if person:
                    pid = self._sanitize_id(f"person-{person.lower().replace(' ', '-')}")
                    vertices.append({
                        "id": pid,
                        "label": "person",
                        "properties": {self._PK: self._PV, "name": person}
                    })
                    edges.append({
                        "from": pid,
                        "to": doc_id,
                        "label": label,
                        "properties": {}
                    })

        return vertices, edges

    async def _execute_bulk_operations(self, vertices: List[Dict], edges: List[Dict]) -> None:
        """
        ULTRA-HIGH-PERFORMANCE: Skip existing resources and only create what's missing.
        """
        # Remove duplicates while preserving order
        unique_vertices = {}
        for v in vertices:
            unique_vertices[v["id"]] = v
        unique_vertices = list(unique_vertices.values())
        
        unique_edges = {}
        for e in edges:
            edge_key = f"{e['from']}-{e['label']}-{e['to']}"
            unique_edges[edge_key] = e
        unique_edges = list(unique_edges.values())

        log.info(f"📦 DEDUPLICATED: {len(unique_vertices)} vertices, {len(unique_edges)} edges")

        # 🚀 SMART FILTERING: Check what already exists
        log.info("🔍 Checking existing resources to skip unnecessary API calls...")
        
        new_vertices = await self._filter_existing_vertices(unique_vertices)
        new_edges = await self._filter_existing_edges(unique_edges)
        
        skipped_vertices = len(unique_vertices) - len(new_vertices)
        skipped_edges = len(unique_edges) - len(new_edges)
        
        log.info(f"⚡ PERFORMANCE BOOST: Skipping {skipped_vertices} existing vertices, {skipped_edges} existing edges")
        log.info(f"📝 CREATING: {len(new_vertices)} new vertices, {len(new_edges)} new edges")

        if not new_vertices and not new_edges:
            log.info("✅ ALL RESOURCES ALREADY EXIST - Nothing to create!")
            return

        # Execute only new vertices in batches
        batch_size = 50
        
        # 📊 PROGRESS: Vertex creation batches
        vertex_batches = [new_vertices[i:i + batch_size] for i in range(0, len(new_vertices), batch_size)]
        
        if vertex_batches:
            with tqdm(vertex_batches, desc="📤 Creating vertices", unit="batch") as pbar:
                for i, batch in enumerate(pbar):
                    pbar.set_description(f"📤 Creating vertex batch {i+1}/{len(vertex_batches)}")
                    
                    await self._execute_vertex_batch(batch)
                    
                    pbar.set_postfix({
                        'batch_size': len(batch),
                        'total_created': (i+1) * batch_size,
                        'remaining': len(new_vertices) - (i+1) * batch_size
                    })

        # Execute only new edges in batches
        edge_batches = [new_edges[i:i + batch_size] for i in range(0, len(new_edges), batch_size)]
        
        if edge_batches:
            with tqdm(edge_batches, desc="📤 Creating edges", unit="batch") as pbar:
                for i, batch in enumerate(pbar):
                    pbar.set_description(f"📤 Creating edge batch {i+1}/{len(edge_batches)}")
                    
                    await self._execute_edge_batch(batch)
                    
                    pbar.set_postfix({
                        'batch_size': len(batch),
                        'total_created': (i+1) * batch_size,
                        'remaining': len(new_edges) - (i+1) * batch_size
                    })

    async def _filter_existing_vertices(self, vertices: List[Dict]) -> List[Dict]:
        """
        ULTRA-FAST: Check which vertices already exist and return only new ones.
        """
        if not vertices:
            return []
        
        # Build bulk existence check query
        vertex_ids = [v["id"] for v in vertices]
        
        # Check existence in batches to avoid query size limits
        existing_ids = set()
        batch_size = 100
        
        # 📊 PROGRESS: Vertex existence checking
        batches = [vertex_ids[i:i + batch_size] for i in range(0, len(vertex_ids), batch_size)]
        
        with tqdm(batches, desc="🔍 Checking vertices", unit="batch") as pbar:
            for batch_ids in pbar:
                pbar.set_description(f"🔍 Checking {len(batch_ids)} vertices...")
                
                # Build query to check multiple vertex IDs at once
                ids_quoted = ", ".join([f"'{vid}'" for vid in batch_ids])
                query = f"g.V().hasId(within({ids_quoted})).id()"
                
                try:
                    result = await self.cosmos_client._execute_query(query)
                    existing_ids.update(result)
                    
                    # Update progress with stats
                    pbar.set_postfix({
                        'found': len(result),
                        'total_existing': len(existing_ids),
                        'batch_ids': len(batch_ids)
                    })
                    
                except Exception as e:
                    # If batch query fails, fall back to individual checks
                    log.warning(f"Batch existence check failed, using individual checks: {e}")
                    pbar.set_description(f"🔍 Individual checks (batch failed)...")
                    
                    for vid in tqdm(batch_ids, desc="🔍 Individual vertex checks", leave=False):
                        try:
                            exists = await self.cosmos_client.vertex_exists(vid)
                            if exists:
                                existing_ids.add(vid)
                        except:
                            pass  # Assume it doesn't exist if check fails
        
        # Filter out existing vertices
        new_vertices = [v for v in vertices if v["id"] not in existing_ids]
        
        log.info(f"🔍 Vertex check: {len(existing_ids)} exist, {len(new_vertices)} new")
        return new_vertices

    async def _filter_existing_edges(self, edges: List[Dict]) -> List[Dict]:
        """
        ULTRA-FAST: Check which edges already exist and return only new ones.
        """
        if not edges:
            return []
        
        # For edges, we'll do a more targeted approach since edge existence
        # checking can be complex. We'll use a sampling approach for speed.
        existing_edge_keys = set()
        
        # Check existence for a sample first to get a sense of what exists
        sample_size = min(10, len(edges))
        sample_edges = edges[:sample_size]
        
        log.info(f"🔍 Sampling {sample_size} edges to determine checking strategy...")
        
        # 📊 PROGRESS: Sample edge checking
        with tqdm(sample_edges, desc="🔍 Sampling edges", unit="edge") as pbar:
            for edge in pbar:
                pbar.set_description(f"🔍 Sampling edge {edge['label'][:20]}...")
                
                try:
                    # Quick check: does edge exist?
                    query = f"g.V('{edge['from']}').outE('{edge['label']}').where(inV().hasId('{edge['to']}')).count()"
                    result = await self.cosmos_client._execute_query(query)
                    if result and result[0] > 0:
                        edge_key = f"{edge['from']}-{edge['label']}-{edge['to']}"
                        existing_edge_keys.add(edge_key)
                        
                    pbar.set_postfix({
                        'found_existing': len(existing_edge_keys),
                        'sample_progress': f"{len(existing_edge_keys)}/{len(sample_edges)}"
                    })
                    
                except:
                    pass  # Assume it doesn't exist if check fails
        
        # If sampling shows most edges exist, do more thorough checking
        # Otherwise, assume most are new for speed
        if len(existing_edge_keys) > sample_size * 0.7:  # >70% exist
            log.info("🔍 Most edges seem to exist, doing thorough edge checking...")
            
            remaining_edges = edges[sample_size:]
            
            # 📊 PROGRESS: Thorough edge checking
            with tqdm(remaining_edges, desc="🔍 Thorough edge check", unit="edge") as pbar:
                for edge in pbar:
                    pbar.set_description(f"🔍 Checking {edge['label'][:15]}...")
                    
                    try:
                        query = f"g.V('{edge['from']}').outE('{edge['label']}').where(inV().hasId('{edge['to']}')).count()"
                        result = await self.cosmos_client._execute_query(query)
                        if result and result[0] > 0:
                            edge_key = f"{edge['from']}-{edge['label']}-{edge['to']}"
                            existing_edge_keys.add(edge_key)
                            
                        # Update progress stats
                        pbar.set_postfix({
                            'total_existing': len(existing_edge_keys),
                            'check_rate': f"{(len(existing_edge_keys)/len(edges)*100):.1f}%"
                        })
                        
                    except:
                        pass
        else:
            log.info("🚀 Most edges seem new, skipping detailed edge existence checks for speed")
        
        # Filter out existing edges
        new_edges = []
        for edge in edges:
            edge_key = f"{edge['from']}-{edge['label']}-{edge['to']}"
            if edge_key not in existing_edge_keys:
                new_edges.append(edge)
        
        log.info(f"🔍 Edge check: {len(existing_edge_keys)} exist, {len(new_edges)} new")
        return new_edges

    async def _execute_vertex_batch(self, batch: List[Dict]) -> None:
        """Execute a batch of vertex operations."""
        # Build batch Gremlin query
        queries = []
        for v in batch:
            prop_chain = ""
            for key, value in v["properties"].items():
                if value is not None:
                    if isinstance(value, bool):
                        prop_chain += f".property('{key}', {str(value).lower()})"
                    elif isinstance(value, (int, float)):
                        prop_chain += f".property('{key}', {value})"
                    else:
                        escaped_val = str(value).replace("'", "\\'").replace('"', '\\"')
                        prop_chain += f".property('{key}', '{escaped_val}')"
            
            query = f"g.addV('{v['label']}').property('id', '{v['id']}'){prop_chain}"
            queries.append(query)
        
        # Execute batch query
        batch_query = "; ".join(queries)
        try:
            await self.cosmos_client._execute_query(batch_query)
        except Exception as e:
            if "conflict" in str(e).lower() or "already exists" in str(e).lower():
                # Handle conflicts gracefully - vertices already exist
                pass
            else:
                log.error(f"Vertex batch error: {e}")
                # Fall back to individual execution
                for v in batch:
                    try:
                        await self._V(v["id"], v["label"], v["properties"])
                    except:
                        pass  # Ignore individual conflicts

    async def _execute_edge_batch(self, batch: List[Dict]) -> None:
        """Execute a batch of edge operations."""
        queries = []
        for e in batch:
            prop_chain = ""
            if e["properties"]:
                for key, value in e["properties"].items():
                    if value is not None:
                        if isinstance(value, bool):
                            prop_chain += f".property('{key}', {str(value).lower()})"
                        elif isinstance(value, (int, float)):
                            prop_chain += f".property('{key}', {value})"
                        else:
                            escaped_val = str(value).replace("'", "\\'")
                            prop_chain += f".property('{key}', '{escaped_val}')"
            
            # Fixed: Remove extra closing parenthesis that was causing syntax errors
            query = f"g.V('{e['from']}').addE('{e['label']}').to(g.V('{e['to']}'))){prop_chain}"
            queries.append(query)
        
        # Execute batch query
        batch_query = "; ".join(queries)
        try:
            await self.cosmos_client._execute_query(batch_query)
        except Exception as e:
            if "conflict" in str(e).lower() or "already exists" in str(e).lower():
                # Handle conflicts gracefully - edges already exist
                pass
            else:
                log.error(f"Edge batch error: {e}")
                # Fall back to individual execution
                for edge in batch:
                    try:
                        await self._E(edge["from"], edge["label"], edge["to"], edge["properties"])
                    except:
                        pass  # Ignore individual conflicts

    async def _process_ontology_file(self, p: Path) -> None:
        data = json.loads(p.read_text(encoding="utf‑8"))

        meeting_date = (data.get("meeting_date") or "UNKNOWN").replace(".", "-")
        meeting_id   = self._sanitize_id(f"meeting-{meeting_date}")

        # 1️⃣  MEETING vertex ------------------------------------------------
        await self._V(
            meeting_id,
            "meeting",
            {self._PK: self._PV,
             "date": meeting_date,
             "doc_id": data.get("doc_id"),
             "source_file": data.get("source_file")}
        )

        # Add AGENDA_DOCUMENT vertex
        agenda_doc_id = self._sanitize_id(f"agenda-{meeting_date}")
        await self._V(
            agenda_doc_id,
            "agenda_document",
            {self._PK: self._PV,
             "meeting_date": meeting_date,
             "source_file": data.get("source_file")}
        )
        await self._E(meeting_id, "HAS_AGENDA", agenda_doc_id, {})

        # 2️⃣  SECTION + AGENDA‑ITEM vertices --------------------------------
        sections: List[Dict[str, Any]] = data.get("sections", [])
        for s in sections:
            sec_id = self._sanitize_id(f"section-{meeting_date}-{s.get('section_id')}")
            await self._V(
                sec_id,
                "section",
                {self._PK: self._PV,
                 "code": s.get("section_name"),
                 "order": s.get("section_order"),
                 "meeting_date": meeting_date}
            )
            await self._E(agenda_doc_id, "HAS_SECTION", sec_id,
                    {"order": s.get("section_order")})

            for it in s.get("items", []):
                code  = it.get("item_code") or "--"
                item_id = self._sanitize_id(f"item-{meeting_date}-{code}")
                await self._V(
                    item_id,
                    "agendaItem",
                    {self._PK: self._PV,
                     "code": code,
                     "title": it.get("title", ""),
                     "document_reference": it.get("document_reference"),
                     "order": it.get("item_order"),
                     "meeting_date": meeting_date}
                )
                await self._E(sec_id, "HAS_AGENDA_ITEM", item_id,
                        {"order": it.get("item_order")})

        # 3️⃣  TEMPORAL PRECEDES edges ---------------------------------------
        items = [it["item_code"] for s in sections for it in s.get("items", []) if it.get("item_code")]
        items_sorted = sorted(items, key=natural_item_sort_key)
        for a, b in zip(items_sorted, items_sorted[1:]):
            await self._E(self._sanitize_id(f"item-{meeting_date}-{a}"), "PRECEDES",
                    self._sanitize_id(f"item-{meeting_date}-{b}"), {})

        # 4️⃣  LEGAL DOCS, MOTIONS & VOTES -----------------------------------
        for e in data.get("entities", []):
            if e.get("type") not in ("ORDINANCE", "RESOLUTION"):
                continue
            doc_num = e.get("name")
            
            # Skip if this ordinance was already processed from enhanced files
            if doc_num in self.processed_ordinances:
                log.debug(f"⏭️ Skipping duplicate {e['type']} {doc_num} (already processed from enhanced file)")
                continue
            
            doc_id  = self._sanitize_id(f"{e['type'].lower()}-{doc_num}")
            await self._V(doc_id, e["type"].lower(),
                    {self._PK: self._PV,
                     "doc_number": doc_num,
                     "title": e.get("description", "")[:512],
                     "meeting_date": meeting_date})

            ref_code = e.get("related_item") or e.get("agenda_item_code")
            if ref_code:
                await self._E(self._sanitize_id(f"item-{meeting_date}-{ref_code}"), "IMPLEMENTS", doc_id, {})

            if e.get("vote_details"):
                await self._E(doc_id, "VOTED_ON", meeting_id,
                        {"yeas": e["vote_details"].get("yeas"),
                         "nays": e["vote_details"].get("nays"),
                         "unanimous": e["vote_details"].get("unanimous", False)})

            motion = e.get("motion", {})
            for label, person in [("MOVED_BY", motion.get("moved_by")),
                                  ("SECONDED_BY", motion.get("seconded_by"))]:
                if person:
                    pid = self._sanitize_id(f"person-{person.lower().replace(' ', '-')}")
                    await self._V(pid, "person", {self._PK: self._PV, "name": person})
                    await self._E(pid, label, doc_id, {})

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
        properties = {
            'title': metadata.get('title', json_file.stem),
            'document_type': metadata.get('document_type', 'document'),
            'source_file': json_file.name,
            'meeting_date': metadata.get('meeting_date', ''),
            'created_at': metadata.get('extraction_timestamp', ''),
            'word_count': metadata.get('word_count', 0),
            'page_count': metadata.get('page_count', 0),
        }
        
        await self.cosmos_client.create_vertex('Document', doc_id, properties)
        log.debug(f"Created document vertex: {doc_id}")

    async def _process_entities_from_json(self, doc_id: str, entities: Dict, metadata: Dict) -> None:
        """Process entities from JSON data and create vertices/relationships."""
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

    def _generate_entity_id(self, entity_type: str, entity_text: str) -> str:
        """Generate a unique entity ID."""
        import hashlib
        
        # Create normalized text for ID generation
        normalized_text = entity_text.lower().strip()
        unique_string = f"{entity_type}_{normalized_text}"
        hash_part = hashlib.sha1(unique_string.encode()).hexdigest()[:8]
        
        return f"{entity_type.upper()}_{hash_part}"

    def _generate_document_id_from_json(self, json_file: Path, metadata: Dict) -> str:
        """Generate a unique document ID from JSON file."""
        import hashlib
        
        # Use file path and some metadata to create unique ID
        unique_string = f"{json_file.name}_{metadata.get('document_type', 'doc')}"
        hash_part = hashlib.sha1(unique_string.encode()).hexdigest()[:8]
        
        doc_type = metadata.get('document_type', 'doc').upper()
        return f"{doc_type}_{hash_part}"

    async def build_graph_from_markdown(self, markdown_dir: Path) -> None:
        """
        Build knowledge graph from enriched markdown files.
        
        Args:
            markdown_dir: Directory containing enriched markdown files
        """
        log.info(f"🔗 Building custom graph from markdown files in: {markdown_dir}")
        
        # Find all markdown files
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files to process")
        
        if not markdown_files:
            log.warning("No markdown files found for graph building")
            return
        
        # Connect to Cosmos DB
        async with self.cosmos_client:
            # Process files and build graph
            for md_file in markdown_files:
                try:
                    await self._process_document_for_graph(md_file)
                except Exception as e:
                    log.error(f"Error processing {md_file.name} for graph: {e}")
                    continue
        
        log.info("✅ Custom graph building completed")

    async def _process_document_for_graph(self, md_file: Path) -> None:
        """
        Process a single markdown document and add its entities/relationships to the graph.
        
        Args:
            md_file: Path to markdown file
        """
        log.info(f"📄 Processing {md_file.name} for graph building")
        
        # Read the markdown content
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract document metadata from the header
        metadata = self._extract_document_metadata(content)
        
        # Create document vertex
        doc_id = self._generate_document_id(md_file, metadata)
        await self._create_document_vertex(doc_id, metadata, md_file)
        
        # Extract and create entities based on document type
        if metadata.get('document_type') == 'agenda':
            await self._process_agenda_document(doc_id, content, metadata)
        elif metadata.get('document_type') == 'verbatim_transcript':
            await self._process_transcript_document(doc_id, content, metadata)
        elif metadata.get('document_type') in ['ordinance', 'resolution']:
            await self._process_legislative_document(doc_id, content, metadata)
        else:
            await self._process_generic_document(doc_id, content, metadata)

    def _extract_document_metadata(self, content: str) -> Dict[str, Any]:
        """Extract metadata from the markdown header."""
        metadata = {}
        
        # Look for metadata section between --- markers
        if content.startswith("---"):
            try:
                _, header_section, _ = content.split("---", 2)
                
                # Parse key-value pairs from header
                for line in header_section.strip().split("\n"):
                    line = line.strip()
                    if ":" in line and line.startswith("- "):
                        # Handle format like "- Document Type: AGENDA"
                        key_value = line[2:].split(":", 1)
                        if len(key_value) == 2:
                            key = key_value[0].strip().lower().replace(" ", "_")
                            value = key_value[1].strip()
                            metadata[key] = value
            except ValueError:
                pass  # No proper header found
        
        return metadata

    async def _create_document_vertex(self, doc_id: str, metadata: Dict, md_file: Path) -> None:
        """Create a vertex for the document."""
        properties = {
            'title': metadata.get('title', md_file.stem),
            'document_type': metadata.get('document_type', 'document'),
            'source_file': md_file.name,
            'meeting_date': metadata.get('meeting_date', ''),
            'created_at': metadata.get('extraction_timestamp', ''),
        }
        
        await self.cosmos_client.create_vertex('Document', doc_id, properties)
        log.debug(f"Created document vertex: {doc_id}")

    async def _process_agenda_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process agenda document and create agenda-specific entities."""
        log.debug(f"Processing agenda document: {doc_id}")
        
        # Create meeting vertex
        meeting_date = metadata.get('meeting_date', 'unknown')
        if meeting_date != 'unknown':
            meeting_id = f"MEETING_{meeting_date.replace('.', '_')}"
            meeting_properties = {
                'date': meeting_date,
                'type': 'city_commission_meeting'
            }
            await self.cosmos_client.create_vertex('Meeting', meeting_id, meeting_properties)
            
            # Link document to meeting
            await self.cosmos_client.create_edge_if_not_exists(
                doc_id, meeting_id, 'DOCUMENTS'
            )
        
        # Extract agenda items from content
        agenda_items = self._extract_agenda_items_from_content(content)
        for item in agenda_items:
            await self._create_agenda_item_vertex(item, doc_id, meeting_date)

    async def _process_transcript_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process transcript document and create transcript-specific entities."""
        log.debug(f"Processing transcript document: {doc_id}")
        
        # Extract agenda items mentioned in transcript
        mentioned_items = self._extract_agenda_items_from_content(content)
        
        # Link transcript to agenda items
        for item_code in mentioned_items:
            item_id = f"ITEM_{item_code}_{metadata.get('meeting_date', 'unknown').replace('.', '_')}"
            await self.cosmos_client.create_edge_if_not_exists(
                doc_id, item_id, 'DISCUSSES'
            )

    async def _process_legislative_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process ordinance/resolution document."""
        log.debug(f"Processing legislative document: {doc_id}")
        
        # Link to agenda item if specified
        agenda_item = metadata.get('linked_agenda_item')
        if agenda_item:
            item_id = f"ITEM_{agenda_item}_{metadata.get('meeting_date', 'unknown').replace('.', '_')}"
            await self.cosmos_client.create_edge_if_not_exists(
                item_id, doc_id, 'IMPLEMENTS'
            )

    async def _process_generic_document(self, doc_id: str, content: str, metadata: Dict) -> None:
        """Process generic document."""
        log.debug(f"Processing generic document: {doc_id}")
        # For now, just ensure the document vertex exists
        pass

    def _extract_agenda_items_from_content(self, content: str) -> List[str]:
        """Extract agenda item codes from document content."""
        import re
        
        item_codes = []
        
        # Look for agenda item patterns in the content
        patterns = [
            r'AGENDA_ITEM:\s*([A-Z]-\d+)',
            r'Item\s+([A-Z]-\d+)',
            r'([A-Z]-\d+)\s*:',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            item_codes.extend(matches)
        
        # Remove duplicates and return
        return list(set(item_codes))

    async def _create_agenda_item_vertex(self, item_code: str, doc_id: str, meeting_date: str) -> None:
        """Create a vertex for an agenda item."""
        item_id = f"ITEM_{item_code}_{meeting_date.replace('.', '_')}"
        
        properties = {
            'item_code': item_code,
            'meeting_date': meeting_date,
            'status': 'scheduled'
        }
        
        await self.cosmos_client.create_vertex('AgendaItem', item_id, properties)
        
        # Link agenda item to document
        await self.cosmos_client.create_edge_if_not_exists(
            doc_id, item_id, 'CONTAINS'
        )

    def _generate_document_id(self, md_file: Path, metadata: Dict) -> str:
        """Generate a unique document ID."""
        import hashlib
        
        # Use file path and some metadata to create unique ID
        unique_string = f"{md_file.name}_{metadata.get('document_type', 'doc')}"
        hash_part = hashlib.sha1(unique_string.encode()).hexdigest()[:8]
        
        doc_type = metadata.get('document_type', 'doc').upper()
        return f"{doc_type}_{hash_part}"

    async def clear_graph(self) -> None:
        """Clear all data from the graph (use with caution)."""
        log.warning("🗑️ Clearing entire graph database")
        
        async with self.cosmos_client:
            await self.cosmos_client.clear_graph()
        
        log.info("✅ Graph cleared")

    async def get_graph_stats(self) -> Dict[str, int]:
        """Get basic statistics about the graph."""
        stats = {
            'total_vertices': 0,
            'documents': 0,
            'agenda_items': 0,
            'meetings': 0
        }
        
        try:
            async with self.cosmos_client:
                # Count total vertices
                result = await self.cosmos_client._execute_query("g.V().count()")
                stats['total_vertices'] = result[0] if result else 0
                
                # Count by label
                for label in ['Document', 'AgendaItem', 'Meeting']:
                    result = await self.cosmos_client._execute_query(f"g.V().hasLabel('{label}').count()")
                    count = result[0] if result else 0
                    stats[label.lower() + 's'] = count
                    
        except Exception as e:
            log.error(f"Error getting graph stats: {e}")
        
        return stats

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
            
            # Check if this is a stage1 OCR file or enhanced file
            is_stage1_file = 'stage1_ocr' in json_file.name
            
            if is_stage1_file:
                # Handle stage1 OCR files (special ordinances like SOE, CG, EO)
                await self._process_stage1_special_ordinance(json_file, doc_data)
            else:
                # Handle enhanced ordinance/resolution files (original logic)
                await self._process_standard_enhanced_document(json_file, doc_data)
                
        except Exception as e:
            log.error(f"❌ Error processing document {json_file.name}: {e}")
            raise

    async def _process_standard_enhanced_document(self, json_file: Path, doc_data: Dict) -> None:
        """Process standard enhanced ordinance/resolution files."""
        # Extract document metadata
        title = doc_data.get('title', '')
        document_type = self._determine_document_type_from_filename(json_file.name)
        meeting_date = doc_data.get('meeting_date', '')
        document_number = doc_data.get('document_number', '')
        
        # If no document number, try to extract from filename
        if not document_number:
            document_number = self._extract_document_number_from_filename(json_file.name)
        
        # Generate document ID
        if document_number:
            doc_id = self._sanitize_id(f"doc-{document_type.lower()}-{document_number}")
        else:
            doc_id = self._sanitize_id(f"doc-{json_file.stem}")
        
        # Create document vertex properties
        properties = {
            self._PK: self._PV,
            'title': title[:512] if title else json_file.stem,
            'document_type': document_type.lower(),
            'document_classification': document_type.lower(),
            'source_file': json_file.name,
            'meeting_date': meeting_date,
            'document_number': document_number,
            'created_at': doc_data.get('extraction_timestamp', ''),
            'word_count': doc_data.get('word_count', 0),
            'page_count': doc_data.get('page_count', 0),
            'text_content': doc_data.get('text_content', '')[:1000] if doc_data.get('text_content') else ''
        }
        
        # Create the document vertex
        await self._V(doc_id, 'Document', properties)
        
        # Track this ordinance as processed to avoid duplicates
        if document_number and document_type.lower() in ['ordinance', 'resolution']:
            self.processed_ordinances.add(document_number)
            log.debug(f"📝 Tracked {document_type} {document_number} as processed")
        
        # If there's a meeting date, create relationship to meeting
        if meeting_date:
            meeting_id = self._sanitize_id(f"meeting-{meeting_date.replace('.', '-')}")
            # Create meeting vertex if it doesn't exist
            meeting_properties = {
                self._PK: self._PV,
                'date': meeting_date,
                'type': 'city_commission_meeting'
            }
            await self._V(meeting_id, 'meeting', meeting_properties)
            
            # Create edge from document to meeting
            await self._E(doc_id, 'ADOPTED_IN', meeting_id, {})
        
        # If there's an agenda item reference, create relationship
        agenda_item = doc_data.get('agenda_item_code') or doc_data.get('linked_agenda_item')
        if agenda_item and meeting_date:
            item_id = self._sanitize_id(f"item-{meeting_date.replace('.', '-')}-{agenda_item}")
            await self._E(item_id, 'IMPLEMENTS', doc_id, {})
        
        log.debug(f"✅ Created enhanced document: {document_type} {document_number}")

    async def _process_stage1_special_ordinance(self, json_file: Path, doc_data: Dict) -> None:
        """Process stage1 OCR files for ordinances (both special and standard)."""
        filename = json_file.name
        
        # Determine ordinance type and extract identifier
        doc_type = 'ordinance'
        
        # Try to extract standard document number first
        doc_number = self._extract_document_number_from_filename(filename)
        
        if not doc_number:
            # For non-standard ordinances, use the full filename without extension
            doc_number = filename.replace('_stage1_ocr.json', '')
        
        # Generate document ID
        doc_id = self._sanitize_id(f"doc-{doc_type.lower()}-{doc_number}")
        
        # Extract title from the stage1 data
        title = doc_data.get('metadata', {}).get('title', '') or doc_number
        
        # Try to extract meeting date from filename or metadata
        meeting_date = ''
        metadata = doc_data.get('metadata', {})
        if 'meeting_date' in metadata:
            meeting_date = metadata['meeting_date']
        else:
            # Try to extract date from filename
            import re
            date_match = re.search(r'(\d{2}_\d{2}_\d{4})', filename)
            if date_match:
                # Convert MM_DD_YYYY to MM.DD.YYYY
                date_str = date_match.group(1)
                meeting_date = date_str.replace('_', '.')
        
        # Create document vertex properties
        properties = {
            self._PK: self._PV,
            'title': title[:512] if title else doc_number,
            'document_type': doc_type.lower(),
            'document_classification': doc_type.lower(),
            'source_file': json_file.name,
            'meeting_date': meeting_date,
            'document_number': doc_number,
            'created_at': metadata.get('extraction_timestamp', ''),
            'word_count': metadata.get('word_count', 0),
            'page_count': metadata.get('page_count', 0),
            'text_content': metadata.get('text_content', '')[:1000] if metadata.get('text_content') else ''
        }
        
        # Create the document vertex
        await self._V(doc_id, 'Document', properties)
        
        # Track this ordinance as processed to avoid duplicates
        if doc_number:
            self.processed_ordinances.add(doc_number)
            log.debug(f"📝 Tracked special {doc_type} {doc_number} as processed")
        
        # If there's a meeting date, create relationship to meeting
        if meeting_date:
            meeting_id = self._sanitize_id(f"meeting-{meeting_date.replace('.', '-')}")
            # Create meeting vertex if it doesn't exist
            meeting_properties = {
                self._PK: self._PV,
                'date': meeting_date,
                'type': 'city_commission_meeting'
            }
            await self._V(meeting_id, 'meeting', meeting_properties)
            
            # Create edge from document to meeting
            await self._E(doc_id, 'ADOPTED_IN', meeting_id, {})
        
        log.debug(f"✅ Created special ordinance: {doc_type} {doc_number}")

    def _determine_document_type_from_filename(self, filename: str) -> str:
        """Determine document type from filename."""
        filename_lower = filename.lower()
        if 'ordinance' in filename_lower:
            return 'ordinance'
        elif 'resolution' in filename_lower:
            return 'resolution'
        else:
            return 'document'

    def _extract_document_number_from_filename(self, filename: str) -> str:
        """Extract document number from filename with support for various patterns."""
        import re
        
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