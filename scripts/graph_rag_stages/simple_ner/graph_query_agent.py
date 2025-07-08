import logging
import json
from typing import Any, Dict, List
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser

log = logging.getLogger(__name__)

class GraphQueryAgent:
    """
    Translates a query analysis into a Gremlin query, executes it against
    Cosmos DB, and returns the structured result.
    """

    def __init__(self, cosmos_client: CosmosGraphClient):
        """Initializes the agent with a CosmosDB client."""
        self.cosmos = cosmos_client
        self.temporal_parser = TemporalParser()

    def _build_gremlin_query(self, analysis: Dict[str, Any]) -> str:
        """
        Programmatically builds a Gremlin query from the query analysis.

        Args:
            analysis: The output from SimpleNERQueryEngine._analyze_query.

        Returns:
            A Gremlin query string.
        """
        entities = analysis.get('entities', {})
        intent = analysis.get('intent', 'specific_lookup')
        hints = analysis.get('structural_hints', {})
        
        # Start with a base traversal - prioritize document types for temporal queries
        base_traversals = []
        
        # Handle document type queries FIRST (ordinances, resolutions, etc.)
        if entities.get('document_types') or hints.get('document_type'):
            doc_types = set(entities.get('document_types', []))  # Use set to avoid duplicates
            if hints.get('document_type'):
                doc_types.add(hints['document_type'])
            
            for doc_type in doc_types:
                if doc_type.lower() in ['ordinance', 'ordinances']:
                    base_traversals.append("g.V().hasLabel('document').has('document_classification', 'ordinance')")
                elif doc_type.lower() in ['resolution', 'resolutions']:
                    base_traversals.append("g.V().hasLabel('document').has('document_classification', 'resolution')")
                elif doc_type.lower() in ['agenda', 'agendas']:
                    base_traversals.append("g.V().hasLabel('agenda_item')")
                elif doc_type.lower() in ['verbatim', 'transcript', 'transcripts']:
                    base_traversals.append("g.V().hasLabel('document').has('document_classification', 'verbatim')")
        
        # Only add people/agenda/official_records if no document types specified
        elif entities.get('people'):
            pids = [f"'person-{p.lower().replace(' ', '-')}'" for p in entities['people']]
            base_traversals.append(f"g.V({', '.join(pids)})")

        elif entities.get('agenda_items'):
            codes = [f"'{c}'" for c in entities['agenda_items']]
            base_traversals.append(f"g.V().hasLabel('agenda_item').has('code', within({', '.join(codes)}))")
        
        elif entities.get('official_records'):
            doc_nums = [f"'{rec.replace('Ord. ', '').replace('Res. ', '')}'" for rec in entities['official_records']]
            ord_ids = [f"'doc-ordinance-{n}'" for n in doc_nums]
            res_ids = [f"'doc-resolution-{n}'" for n in doc_nums]
            base_traversals.append(f"g.V({', '.join(ord_ids + res_ids)})")

        if not base_traversals and hints.get('date_range'):
            # Pure temporal query - look for documents in date range
            start, end = hints['date_range']
            doc_type = hints.get('document_type', '')
            start_year = start.split('-')[0]
            end_year = end.split('-')[0]
            
            if doc_type.lower() in ['ordinance', 'ordinances']:
                # Try meeting_date first, fall back to document name pattern for year
                return f"g.V().hasLabel('document').has('document_classification', 'ordinance').or(has('meeting_date', between('{start}','{end}')), has('title', containing('{start_year}')).has('title', regex('({start_year}|{end_year}|20[12][0-9])'))).valueMap(true)"
            elif doc_type.lower() in ['resolution', 'resolutions']:
                return f"g.V().hasLabel('document').has('document_classification', 'resolution').or(has('meeting_date', between('{start}','{end}')), has('title', containing('{start_year}')).has('title', regex('({start_year}|{end_year}|20[12][0-9])'))).valueMap(true)"
            else:
                # General temporal query for all document types - get ALL if dates are missing
                return f"g.V().hasLabel('document').or(has('meeting_date', between('{start}','{end}')), has('title', regex('20[12][0-9]'))).valueMap(true)"

        if not base_traversals:
            return "g.V().limit(0)" # No entities to query

        # Combine base traversals
        if len(base_traversals) > 1:
            query = "g.union(" + ", ".join(base_traversals) + ").dedup()"
        else:
            query = base_traversals[0]

        # Add temporal filters if present
        if hints.get('date_range'):
            start, end = hints['date_range']
            query += f".has('meeting_date', between('{start}','{end}'))"

        # Add intent-based traversals
        if intent == 'relationship_query' and len(base_traversals) > 1:
            query += ".path().by(valueMap('title', 'code', 'name'))"
        elif intent == 'temporal_search':
            # For temporal searches, get all matching documents without limits for comprehensive results
            query += ".dedup().valueMap(true)"
        else: # Default to getting related entities for specific lookups
            query += ".union(identity(), both().limit(5)).dedup().valueMap(true).limit(25)"
            
        log.info(f"Constructed Gremlin Query: {query}")
        return query

    async def generate_and_run(self, analysis: Dict[str, Any]) -> Any:
        """
        Generates and executes the Gremlin query, returning the database results.
        """
        if not self.cosmos._client:
            await self.cosmos.connect()

        gremlin_query = self._build_gremlin_query(analysis)
        
        try:
            results = await self.cosmos._execute_query(gremlin_query)
            log.info(f"Gremlin query returned {len(results)} results.")
            return {"results": results, "query": gremlin_query}
        except Exception as e:
            log.error(f"Failed to execute Gremlin query '{gremlin_query}': {e}")
            return {"error": str(e), "query": gremlin_query} 