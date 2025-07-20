# graph_rag_stages/phase3_querying/smart_query_router.py
"""
Intelligent router that analyses a user query and decides which GraphRAG
query-method (local / global / drift) to invoke, plus the parameter
block required by the downstream engine.

* Extracts entity references (agenda-items, ordinances, resolutions)
* Detects intent  – ENTITY_SPECIFIC / HOLISTIC / EXPLORATORY / TEMPORAL
* Computes focus (single-entity detail vs contextual vs comparison)
* Emits `track_sources=True` & `citation_style='inline'` so that later
  stages can add inline citations automatically.
"""
from __future__ import annotations

import logging, re
from enum import Enum
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


class QueryIntent(Enum):
    ENTITY_SPECIFIC = "entity_specific"
    HOLISTIC = "holistic"
    EXPLORATORY = "exploratory"
    TEMPORAL = "temporal"


class QueryFocus(Enum):
    SPECIFIC_ENTITY = "specific_entity"
    MULTIPLE_SPECIFIC = "multiple_specific"
    COMPARISON = "comparison"
    CONTEXTUAL = "contextual"
    GENERAL = "general"


class SmartQueryRouter:
    """
    Drop-in replacement for the legacy router.  Heuristic-driven (regex +
    keyword scoring) – **no LLM calls required**.
    """

    # ── regex tables ──────────────────────────────────────────────
    _ENTITY_PATTERNS = {
        "agenda_item": [
            r"(?:agenda\s+)?(?:item|items)\s+([A-Z]-?\d+)",
            r"(?:item|items)\s+([A-Z]-?\d+)",
            r"\b([A-Z]-\d+)\b",
        ],
        "ordinance": [
            r"ordinance(?:\s+(?:number|no\.?|#))?\s*(\d{4}-\d+|\d+)",
            r"\b(\d{4}-\d+)\b(?=.*ordinance)",
        ],
        "resolution": [
            r"resolution(?:\s+(?:number|no\.?|#))?\s*(\d{4}-\d+|\d+)",
            r"\b(\d{4}-\d+)\b(?=.*resolution)",
        ],
        # Add patterns for new entity types
        "person": [
            r"(?:Mayor|Commissioner|Manager|Attorney|Clerk)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"(?:Mr\.|Ms\.|Mrs\.)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)"
        ],
        "organization": [
            r"(?:Department of|City of)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Department|Division|Commission)"
        ],
        "asset": [
            r"(\$[\d,]+(?:\.\d+)?(?:\s*(?:million|billion))?)",
            r"([\d,]+(?:\.\d+)?)\s+dollars"
        ]
    }

    _HOLISTIC_PATTERNS = [
        r"what are the (?:main|top|key) (themes|topics|issues)",
        r"summarise (?:the|all) (.*)",
        r"overall (.*)",
        r"trends in (.*)",
        r"patterns across (.*)",
        r"all.*agenda.*items",
        r"complete.*agenda",
        r"agenda.*items.*(?:discussed|presented|covered)",
        r"what.*items.*(?:meeting|agenda)",
        r"list.*agenda.*items",
        r"agenda.*items.*(?:august|september|october|november|december|january|february|march|april|may|june|july)",
    ]

    _TEMPORAL_PATTERNS = [
        r"how has (.*) (?:changed|evolved)",
        r"timeline of (.*)",
        r"history of (.*)",
        r"development of (.*) over time",
        r"evolution of (.*)",
        r"changes in (.*)",
    ]

    # keyword buckets for focus scoring
    _SPECIFIC = {
        "limiting": ["only", "just", "specifically", "exactly", "precisely"],
        "detail_nouns": [
            "details",
            "information",
            "content",
            "provision",
            "summary",
            "description",
        ],
    }
    _COMPARISON = {
        "verbs": ["compare", "contrast", "differ", "distinguish"],
        "words": ["versus", "vs", "against", "difference", "similarity"],
    }
    _CONTEXTUAL = {
        "relationship": [
            "related",
            "connected",
            "associated",
            "linked",
            "between",
            "among",
        ]
    }

    # ── public API ────────────────────────────────────────────────
    def determine_query_method(self, query: str) -> Dict[str, Any]:
        ql = query.lower()

        # 1️⃣ holistic?
        for p in self._HOLISTIC_PATTERNS:
            if re.search(p, ql):
                return self._mk(
                    "global",
                    QueryIntent.HOLISTIC,
                    {
                        "community_level": self._community_level(query),
                        "response_type": "multiple paragraphs",
                    },
                )

        # 2️⃣ temporal / drift?
        for p in self._TEMPORAL_PATTERNS:
            if re.search(p, ql):
                return self._mk(
                    "drift",
                    QueryIntent.TEMPORAL,
                    {"initial_community_level": 2, "max_follow_ups": 5},
                )

        # 3️⃣ entity detection
        entities = self._extract_entities(ql)
        if not entities:
            return self._mk(
                "local",
                QueryIntent.EXPLORATORY,
                {"top_k_entities": 10, "include_community_context": True},
            )

        if len(entities) == 1:
            # single-entity branch
            focus = self._single_focus(ql)
            strict = focus == QueryFocus.SPECIFIC_ENTITY
            return self._mk(
                "local",
                QueryIntent.ENTITY_SPECIFIC,
                {
                    "entity_filter": {
                        "type": entities[0]["type"].upper(),
                        "value": entities[0]["value"],
                    },
                    "top_k_entities": 1 if strict else 10,
                    "include_community_context": not strict,
                    "strict_entity_focus": strict,
                    "disable_community": strict,
                },
            )

        # multi-entity branch
        focus = self._multi_focus(ql)
        params: Dict[str, Any] = {
            "multiple_entities": entities,
            "top_k_entities": 10,
            "include_community_context": True,
            "strict_entity_focus": False,
            "disable_community": False,
        }
        if focus == QueryFocus.MULTIPLE_SPECIFIC:
            params.update(
                {
                    "top_k_entities": 1,
                    "aggregate_results": True,
                    "disable_community": True,
                    "strict_entity_focus": True,
                }
            )
        elif focus == QueryFocus.COMPARISON:
            params["comparison_mode"] = True
            params["top_k_entities"] = 5
        else:  # contextual relationships
            params["focus_on_relationships"] = True

        return self._mk("local", QueryIntent.ENTITY_SPECIFIC, params)

    # ── helpers ───────────────────────────────────────────────────
    def _mk(self, method: str, intent: QueryIntent, params: Dict[str, Any]):
        params |= {
            "track_sources": True,
            "include_source_metadata": True,
            "citation_style": "inline",
        }
        return {"method": method, "intent": intent, "params": params}

    # entity extraction
    def _extract_entities(self, ql: str) -> List[Dict[str, str]]:
        found: List[Tuple[int, Dict[str, str]]] = []
        for typ, patterns in self._ENTITY_PATTERNS.items():
            for pat in patterns:
                for m in re.finditer(pat, ql):
                    val = m.group(1)
                    if typ == "agenda_item":
                        val = val.upper()
                        if "-" not in val and len(val) > 1:
                            val = f"{val[0]}-{val[1:]}"
                    found.append((m.start(), {"type": typ, "value": val}))
        # dedupe preserving order
        seen, out = set(), []
        for _, ent in sorted(found, key=lambda t: t[0]):
            key = (ent["type"], ent["value"])
            if key not in seen:
                seen.add(key)
                out.append(ent)
        return out

    # focus scorers ------------------------------------------------
    def _single_focus(self, ql: str) -> QueryFocus:
        tokens = set(ql.split())
        spec = sum(w in tokens for w in self._SPECIFIC["limiting"]) * 3
        for n in self._SPECIFIC["detail_nouns"]:
            if n in ql:
                spec += 1
        ctx = sum(w in tokens for w in self._CONTEXTUAL["relationship"]) * 3
        return (
            QueryFocus.SPECIFIC_ENTITY
            if spec >= ctx
            else QueryFocus.CONTEXTUAL
        )

    def _multi_focus(self, ql: str) -> QueryFocus:
        tokens = set(ql.split())
        cmp_score = sum(w in tokens for w in self._COMPARISON["verbs"]) * 3 + sum(
            w in ql for w in self._COMPARISON["words"]
        ) * 2
        if re.search(r"between.*and", ql):
            cmp_score += 3

        spec_score = (
            2
            if re.match(r"^(what|what's|whats)\s+(are|is)\s+", ql)
            else 0
        )
        if any(w in tokens for w in ["separately", "individually", "each"]):
            spec_score += 3
        for n in self._SPECIFIC["detail_nouns"]:
            if n in ql:
                spec_score += 1

        ctx_score = sum(
            w in tokens for w in self._CONTEXTUAL["relationship"]
        ) * 2

        if cmp_score >= spec_score and cmp_score >= ctx_score:
            return QueryFocus.COMPARISON
        return (
            QueryFocus.MULTIPLE_SPECIFIC
            if spec_score > ctx_score
            else QueryFocus.CONTEXTUAL
        )

    # community-level heuristic ------------------------------------
    def _community_level(self, q: str) -> int:
        ql = q.lower()
        if any(w in ql for w in ("entire", "all", "overall", "whole")):
            return 0
        if any(w in ql for w in ("department", "district", "area")):
            return 1
        return 2 