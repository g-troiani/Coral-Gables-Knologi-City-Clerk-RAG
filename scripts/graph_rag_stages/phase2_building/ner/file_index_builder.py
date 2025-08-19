# scripts/graph_rag_stages/phase2_building/ner/file_index_builder.py
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

log = logging.getLogger(__name__)

class NERFileIndexBuilder:
    """Builds indices for NER output files to support later stages."""
    
    def __init__(self, ner_root: Path):
        self.ner_root = Path(ner_root)
        self.indices_dir = self.ner_root / "indices"
        self.indices_dir.mkdir(parents=True, exist_ok=True)
    
    async def build_all_indices(self):
        """Build all required indices for NER outputs."""
        log.info("📚 [INDEX_BUILDER] Starting NER file index building")
        log.info(f"   📁 NER root directory: {self.ner_root}")
        log.info(f"   📁 Indices output directory: {self.indices_dir}")
        
        try:
            # Build entity index
            log.info("🔍 [INDEX_BUILDER] Building entity index...")
            entity_index = await self._build_entity_index()
            self._save_index("entity_index.json", entity_index)
            log.info(f"✅ [INDEX_BUILDER] Entity index completed: {entity_index.get('total_count', 0)} total entities")
            
            # Build chunk index  
            log.info("📄 [INDEX_BUILDER] Building chunk index...")
            chunk_index = await self._build_chunk_index()
            self._save_index("chunk_index.json", chunk_index)
            log.info(f"✅ [INDEX_BUILDER] Chunk index completed: {chunk_index.get('total_count', 0)} total chunks")
            
            # Build relationship index
            log.info("🔗 [INDEX_BUILDER] Building relationship index...")
            relationship_index = await self._build_relationship_index()
            self._save_index("relationship_index.json", relationship_index)
            log.info(f"✅ [INDEX_BUILDER] Relationship index completed: {relationship_index.get('total_count', 0)} total relationships")
            
            log.info(f"💾 [INDEX_BUILDER] All indices saved to {self.indices_dir}")
            log.info(f"📊 [INDEX_BUILDER] Index building summary:")
            log.info(f"   📂 Entities: {entity_index.get('total_count', 0)}")
            log.info(f"   📄 Chunks: {chunk_index.get('total_count', 0)}")
            log.info(f"   🔗 Relationships: {relationship_index.get('total_count', 0)}")
            
        except Exception as e:
            log.error(f"❌ [INDEX_BUILDER] Failed to build NER indices: {e}")
            log.exception(f"❌ [INDEX_BUILDER] Full exception details:")
            raise
    
    async def _build_entity_index(self) -> Dict[str, Any]:
        """Build index of all entities by type."""
        log.info("📂 [ENTITY_INDEX] Building entity index")
        entity_index = {"by_type": {}, "total_count": 0}
        
        entities_dir = self.ner_root / "entities"
        log.info(f"   📁 Scanning entities directory: {entities_dir}")
        
        if not entities_dir.exists():
            log.warning(f"⚠️ [ENTITY_INDEX] Entities directory does not exist: {entities_dir}")
            return entity_index
        
        entity_types_found = []
        for entity_type_dir in entities_dir.iterdir():
            if entity_type_dir.is_dir():
                entity_files = list(entity_type_dir.glob("*.json"))
                entity_index["by_type"][entity_type_dir.name] = len(entity_files)
                entity_index["total_count"] += len(entity_files)
                entity_types_found.append(entity_type_dir.name)
                log.info(f"   📂 {entity_type_dir.name}: {len(entity_files)} files")
        
        log.info(f"📊 [ENTITY_INDEX] Entity index summary:")
        log.info(f"   📂 Entity types found: {len(entity_types_found)}")
        log.info(f"   📄 Total entity files: {entity_index['total_count']}")
        
        return entity_index
    
    async def _build_chunk_index(self) -> Dict[str, Any]:
        """Build index of all chunks (supports both JSON and TXT formats)."""
        chunk_index = {"chunks": [], "total_count": 0}
        
        chunks_dir = self.ner_root / "document_chunks"
        if not chunks_dir.exists():
            return chunk_index
        
        # Support both JSON and TXT chunk formats
        for chunk_file in chunks_dir.iterdir():
            if chunk_file.suffix in [".json", ".txt"]:
                chunk_info = {
                    "file": chunk_file.name,
                    "type": chunk_file.suffix[1:],  # json or txt
                    "size": chunk_file.stat().st_size
                }
                
                # For TXT files, try to parse header metadata
                if chunk_file.suffix == ".txt":
                    try:
                        content = chunk_file.read_text(encoding='utf-8')
                        if content.startswith("---"):
                            # Parse header until next ---
                            lines = content.split('\n')
                            header_lines = []
                            in_header = True
                            for i, line in enumerate(lines[1:], 1):
                                if line.strip() == "---":
                                    break
                                header_lines.append(line)
                            
                            # Basic metadata extraction
                            for line in header_lines:
                                if ":" in line:
                                    key, value = line.split(":", 1)
                                    chunk_info[key.strip().lower()] = value.strip()
                    except Exception:
                        pass  # Skip metadata parsing if it fails
                
                chunk_index["chunks"].append(chunk_info)
                chunk_index["total_count"] += 1
        
        return chunk_index
    
    async def _build_relationship_index(self) -> Dict[str, Any]:
        """Build index of relationships (supports .jsonl and per-chunk .json files)."""
        relationship_index = {"relationships": [], "total_count": 0}

        rel_dir = self.ner_root / "relationships"
        if not rel_dir.exists():
            return relationship_index

        # 1) relationships.jsonl (legacy)
        relationships_file = rel_dir / "relationships.jsonl"
        if relationships_file.exists():
            try:
                content = relationships_file.read_text(encoding='utf-8')
                for line_num, line in enumerate(content.strip().split('\n'), 1):
                    if line.strip():
                        try:
                            rel_data = json.loads(line)
                            relationship_index["relationships"].append({
                                "file": relationships_file.name,
                                "line": line_num,
                                "type": rel_data.get("type", "unknown"),
                                "source": rel_data.get("source", "unknown"),
                                "target": rel_data.get("target", "unknown")
                            })
                            relationship_index["total_count"] += 1
                        except json.JSONDecodeError:
                            log.warning(f"Invalid JSON in relationships file line {line_num}")
            except Exception as e:
                log.warning(f"Could not read relationships file: {e}")

        # 2) Per-chunk JSONs written by ThreePassExtractor
        try:
            for jf in rel_dir.glob("*.json"):
                try:
                    data = json.loads(jf.read_text(encoding='utf-8'))
                except Exception:
                    continue
                rels = data.get("relationships", []) if isinstance(data, dict) else []
                for rel in rels:
                    if not isinstance(rel, dict):
                        continue
                    relationship_index["relationships"].append({
                        "file": jf.name,
                        "type": rel.get("type", "unknown"),
                        "source": rel.get("source", "unknown"),
                        "target": rel.get("target", "unknown")
                    })
                    relationship_index["total_count"] += 1
        except Exception as e:
            log.warning(f"Failed scanning per-chunk relationships: {e}")

        return relationship_index
    
    def _save_index(self, filename: str, index_data: Dict[str, Any]):
        """Save index data to file."""
        index_file = self.indices_dir / filename
        with open(index_file, 'w') as f:
            json.dump(index_data, f, indent=2)
        log.debug(f"Saved {filename} with {index_data.get('total_count', 0)} items")
