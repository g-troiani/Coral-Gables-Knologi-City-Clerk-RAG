# File: scripts/graph_rag_stages/common/ontology_attributes.py

import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

class OntologyAttributesRegistry:
    """
    Loads and caches per-entity attribute lists from ontology_model_final.txt.
    Parsing is heuristic but resilient:
      - Supports headers like "## Person" or "Person:"
      - Accepts bullet lists ("- name", "* title") or comma-separated lines
      - Ignores descriptions after ":" or "-" in bullet lines
    """

    _attrs: Dict[str, List[str]] = {}
    _loaded: bool = False

    @classmethod
    def _candidate_paths(cls) -> List[Path]:
        here = Path(__file__).resolve()
        candidates = []

        # Env override
        env_path = os.getenv("ONTOLOGY_MODEL_PATH")
        if env_path:
            candidates.append(Path(env_path))

        # Common locations relative to repo
        project_root = here.parents[3] if len(here.parents) >= 4 else here.parents[-1]
        for rel in ["ontology_model_final.txt", "docs/ontology_model_final.txt", "config/ontology_model_final.txt"]:
            candidates.append(project_root / rel)

        return [p for p in candidates if p.exists()]

    @classmethod
    def load(cls) -> None:
        if cls._loaded:
            return

        paths = cls._candidate_paths()
        if not paths:
            log.info("Ontology file not found (ONTOLOGY_MODEL_PATH or default locations). Attribute filling will be a no-op.")
            cls._attrs, cls._loaded = {}, True
            return

        path = paths[0]
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            cls._attrs = cls._parse(text)
            cls._loaded = True
            log.info("Loaded ontology attributes from %s (entities: %d)", path, len(cls._attrs))
        except Exception as e:
            log.warning("Failed to load ontology attributes from %s: %s", path, e)
            cls._attrs, cls._loaded = {}, True

    @classmethod
    def _parse(cls, text: str) -> Dict[str, List[str]]:
        attrs: Dict[str, List[str]] = {}

        current_entity: Optional[str] = None
        lines = text.splitlines()

        # Simple normalizer for attribute names
        def norm_attr(a: str) -> str:
            a = a.strip()
            a = re.sub(r'^[\-\*\u2022]\s*', '', a)  # leading bullets
            a = a.split("—")[0].split(" - ")[0].split(":")[0]  # drop descriptions
            a = a.strip()
            a = a.replace(" ", "_")
            a = re.sub(r'[^a-zA-Z0-9_]', '', a)
            return a

        for raw in lines:
            line = raw.strip()
            if not line:
                continue

            # Markdown-ish headers, e.g. "## Person" or "# Document"
            m_hdr = re.match(r'^\#{1,6}\s*([A-Za-z][A-Za-z0-9_]*)\s*$', line)
            if m_hdr:
                current_entity = m_hdr.group(1)
                attrs.setdefault(current_entity, [])
                continue

            # Section "Entity:" or "Person:" or "Document:" etc.
            m_colon = re.match(r'^([A-Za-z][A-Za-z0-9_]*)\s*:\s*(.*)$', line)
            if m_colon and ',' in m_colon.group(2):
                current_entity = m_colon.group(1)
                items = [norm_attr(x) for x in m_colon.group(2).split(',')]
                attrs.setdefault(current_entity, [])
                for a in items:
                    if a and a not in attrs[current_entity]:
                        attrs[current_entity].append(a)
                continue

            # Bullet attributes under a current entity
            if current_entity and re.match(r'^[\-\*\u2022]\s+', line):
                a = norm_attr(line)
                if a:
                    attrs.setdefault(current_entity, [])
                    if a not in attrs[current_entity]:
                        attrs[current_entity].append(a)
                continue

            # Lines like "Attributes: name, title, ..." under an entity
            if current_entity and line.lower().startswith("attributes:"):
                rest = line.split(":", 1)[1]
                items = [norm_attr(x) for x in rest.split(",")]
                for a in items:
                    if a:
                        attrs.setdefault(current_entity, [])
                        if a not in attrs[current_entity]:
                            attrs[current_entity].append(a)
                continue

        return attrs

    @classmethod
    def get_attrs(cls, entity_type: str) -> List[str]:
        cls.load()
        return cls._attrs.get(entity_type, [])

    @classmethod
    def ensure_defaults(cls, entity_type: str, entity: dict) -> dict:
        """
        Add missing attributes defined by the ontology for this entity_type with value None.
        Returns the same dict (mutated) for convenience.
        """
        if not isinstance(entity, dict):
            return entity
        for a in cls.get_attrs(entity_type):
            entity.setdefault(a, None)
        return entity
