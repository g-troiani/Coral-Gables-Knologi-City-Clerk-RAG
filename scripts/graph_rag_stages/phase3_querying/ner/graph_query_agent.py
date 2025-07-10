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
    
    def _convert_iso_to_db_format(self, iso_date: str) -> str:
        """Convert ISO date (YYYY-MM-DD) to database format (MM.DD.YYYY)."""
        try:
            # Parse YYYY-MM-DD and convert to MM.DD.YYYY
            parts = iso_date.split('-')
            if len(parts) == 3:
                year, month, day = parts
                return f"{month}.{day}.{year}"
            return iso_date
        except:
            return iso_date

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
                    # Use both document_classification and document_type for ordinances
                    base_traversals.append("g.V().hasLabel('Document').or(has('document_type', 'ordinance'), has('document_classification', 'ordinance'))")
                elif doc_type.lower() in ['resolution', 'resolutions']:
                    # Use both document_classification and document_type for resolutions
                    base_traversals.append("g.V().hasLabel('Document').or(has('document_type', 'resolution'), has('document_classification', 'resolution'))")
                elif doc_type.lower() in ['agenda', 'agendas']:
                    # Use document_type for agendas
                    base_traversals.append("g.V().hasLabel('Document').has('document_type', 'agenda')")
                elif doc_type.lower() in ['verbatim', 'transcript', 'transcripts']:
                    # Use document_type for transcripts
                    base_traversals.append("g.V().hasLabel('Document').has('document_type', 'verbatim')")
        
        # Only add people/agenda/official_records if no document types specified
        elif entities.get('people'):
            pids = [f"'person-{p.lower().replace(' ', '-')}'" for p in entities['people']]
            base_traversals.append(f"g.V({', '.join(pids)})")

        elif entities.get('agenda_items'):
            codes = [f"'{c}'" for c in entities['agenda_items']]
            base_traversals.append(f"g.V().hasLabel('agendaItem').has('code', within({', '.join(codes)}))")
        
        elif entities.get('official_records'):
            # Use both document_classification and document_type
            for record in entities['official_records']:
                if 'ordinance' in record.lower() or 'ord' in record.lower():
                    base_traversals.append("g.V().hasLabel('Document').or(has('document_type', 'ordinance'), has('document_classification', 'ordinance'))")
                elif 'resolution' in record.lower() or 'res' in record.lower():
                    base_traversals.append("g.V().hasLabel('Document').or(has('document_type', 'resolution'), has('document_classification', 'resolution'))")

        # FIXED: Handle pure temporal queries - get ALL documents and filter dates in Python
        if not base_traversals and hints.get('date_range'):
            doc_type = hints.get('document_type', '')
            
            if doc_type.lower() in ['ordinance', 'ordinances']:
                return f"g.V().hasLabel('Document').or(has('document_type', 'ordinance'), has('document_classification', 'ordinance')).valueMap(true)"
            elif doc_type.lower() in ['resolution', 'resolutions']:
                return f"g.V().hasLabel('Document').or(has('document_type', 'resolution'), has('document_classification', 'resolution')).valueMap(true)"
            else:
                # General temporal query for all document types
                return f"g.V().hasLabel('Document').valueMap(true)"

        if not base_traversals:
            return "g.V().limit(0)" # No entities to query

        # Combine base traversals
        if len(base_traversals) > 1:
            query = "g.union(" + ", ".join(base_traversals) + ").dedup()"
        else:
            query = base_traversals[0]

        # REMOVED: Date filtering in Gremlin - will be done in Python for proper date comparison

        # Add intent-based traversals - REMOVED HARD LIMITS for comprehensive queries
        if intent == 'relationship_query' and len(base_traversals) > 1:
            query += ".path().by(valueMap('title', 'code', 'name'))"
        elif intent == 'temporal_search':
            # For temporal searches, get ALL matching documents without limits
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
            
            # Apply date filtering in Python for proper date comparison
            filtered_results = self._filter_results_by_date(results, analysis)
            
            return {"results": filtered_results, "query": gremlin_query, "raw_count": len(results)}
        except Exception as e:
            log.error(f"Failed to execute Gremlin query '{gremlin_query}': {e}")
            return {"error": str(e), "query": gremlin_query}
    
    def _filter_results_by_date(self, results: List[Dict], analysis: Dict[str, Any]) -> List[Dict]:
        """Filter results by date range using proper date comparison."""
        hints = analysis.get('structural_hints', {})
        date_range = hints.get('date_range')
        
        if not date_range:
            return results
            
        start_date, end_date = date_range
        filtered_results = []
        
        for result in results:
            meeting_date = result.get('meeting_date', [''])[0] if isinstance(result.get('meeting_date'), list) else result.get('meeting_date', '')
            
            if meeting_date:
                # Convert MM.DD.YYYY to YYYY-MM-DD for proper comparison
                normalized_date = self._normalize_db_date(meeting_date)
                
                if normalized_date and start_date <= normalized_date <= end_date:
                    filtered_results.append(result)
        
        log.info(f"Date filtering: {len(results)} -> {len(filtered_results)} results (range: {start_date} to {end_date})")
        return filtered_results
    
    def _normalize_db_date(self, db_date: str) -> str:
        """Convert MM.DD.YYYY to YYYY-MM-DD for proper comparison."""
        try:
            parts = db_date.split('.')
            if len(parts) == 3:
                month, day, year = parts
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            return ""
        except:
            return "" 