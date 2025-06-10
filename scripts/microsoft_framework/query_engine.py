import subprocess
import sys
import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Set
from enum import Enum
import logging
from .query_router import SmartQueryRouter, QueryIntent
from .source_tracker import SourceTracker

logger = logging.getLogger(__name__)

class QueryType(Enum):
    LOCAL = "local"
    GLOBAL = "global"
    DRIFT = "drift"

class CityClerkQueryEngine:
    """Enhanced query engine with inline source citations."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.source_tracker = SourceTracker()  # New component
        
    def _get_python_executable(self):
        """Get the correct Python executable."""
        from pathlib import Path
        
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        
        venv_python = project_root / "venv" / "bin" / "python3"
        if venv_python.exists():
            return str(venv_python)
        
        return sys.executable
        
    async def query(self, query: str, method: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """Execute query with source tracking and inline citations."""
        
        # Enable source tracking
        kwargs['track_sources'] = True
        
        # Route query
        if not method:
            router = SmartQueryRouter()
            route_info = router.determine_query_method(query)
            method = route_info['method']
            kwargs.update(route_info.get('params', {}))
        
        # Execute query with source tracking
        if method == 'local':
            result = await self._local_search_with_sources(query, **kwargs)
        elif method == 'global':
            result = await self._global_search_with_sources(query, **kwargs)
        elif method == 'drift':
            result = await self._drift_search_with_sources(query, **kwargs)
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Clean up any JSON artifacts from the answer
        result['answer'] = self._clean_json_artifacts(result['answer'])
        
        # Process answer to add inline citations
        result['answer'] = self._add_inline_citations(result['answer'], result['sources_used'])
        
        return result
    
    async def _local_search_with_sources(self, query: str, **kwargs) -> Dict[str, Any]:
        """Local search with comprehensive source tracking."""
        
        # Use the existing working local search implementation
        result = await self._execute_local_query(query, kwargs)
        
        # Extract sources from the GraphRAG response
        sources_used = self._extract_sources_from_local_response(result['answer'])
        
        # Add source tracking to the result
        result['sources_used'] = sources_used
        result['data_sources'] = self._format_data_sources(sources_used)
        
        return result
    
    async def _global_search_with_sources(self, query: str, **kwargs) -> Dict[str, Any]:
        """Global search with source tracking."""
        
        # Use the existing working global search implementation
        result = await self._execute_global_query(query, kwargs)
        
        # Extract sources from the GraphRAG response
        sources_used = self._extract_sources_from_global_response(result['answer'])
        
        # Add source tracking to the result
        result['sources_used'] = sources_used
        result['data_sources'] = self._format_data_sources(sources_used)
        
        return result
    
    async def _drift_search_with_sources(self, query: str, **kwargs) -> Dict[str, Any]:
        """DRIFT search with source tracking."""
        
        # Use the existing working drift search implementation
        result = await self._execute_drift_query(query, kwargs)
        
        # Extract sources from the GraphRAG response
        sources_used = self._extract_sources_from_drift_response(result['answer'])
        
        # Add source tracking to the result
        result['sources_used'] = sources_used
        result['data_sources'] = self._format_data_sources(sources_used)
        
        return result
    
    def _clean_json_artifacts(self, answer: str) -> str:
        """Clean JSON artifacts and metadata from GraphRAG response."""
        if not answer:
            return answer
            
        import re
        import json
        
        # Remove JSON blocks that might appear in the response
        # Pattern 1: Remove standalone JSON objects
        json_pattern = r'\{[^{}]*"[^"]*":\s*[^{}]*\}'
        answer = re.sub(json_pattern, '', answer)
        
        # Pattern 2: Remove array-like structures
        array_pattern = r'\[[^\[\]]*"[^"]*"[^\[\]]*\]'
        answer = re.sub(array_pattern, '', answer)
        
        # Pattern 3: Remove configuration-like strings
        config_patterns = [
            r'"[^"]*":\s*"[^"]*"',  # "key": "value"
            r'"[^"]*":\s*\d+',      # "key": 123
            r'"[^"]*":\s*true|false', # "key": true/false
            r'"[^"]*":\s*null',     # "key": null
        ]
        
        for pattern in config_patterns:
            answer = re.sub(pattern, '', answer)
        
        # Remove metadata headers that sometimes appear
        metadata_patterns = [
            r'SUCCESS:\s*.*?\n',
            r'INFO:\s*.*?\n',
            r'DEBUG:\s*.*?\n',
            r'WARNING:\s*.*?\n',
            r'ERROR:\s*.*?\n',
            r'METADATA:\s*.*?\n',
            r'RESPONSE:\s*',
            r'QUERY:\s*.*?\n',
        ]
        
        for pattern in metadata_patterns:
            answer = re.sub(pattern, '', answer, flags=re.IGNORECASE)
        
        # Remove any lines that look like JSON structure
        lines = answer.split('\n')
        cleaned_lines = []
        
        for line in lines:
            line = line.strip()
            # Skip lines that are purely JSON-like
            if (line.startswith('{') and line.endswith('}')) or \
               (line.startswith('[') and line.endswith(']')) or \
               (line.startswith('"') and line.endswith('"') and ':' in line):
                continue
            # Skip empty lines created by removal
            if line:
                cleaned_lines.append(line)
        
        # Rejoin and clean up extra whitespace
        cleaned_answer = '\n'.join(cleaned_lines)
        
        # Remove multiple consecutive newlines
        cleaned_answer = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned_answer)
        
        # Remove leading/trailing whitespace
        cleaned_answer = cleaned_answer.strip()
        
        return cleaned_answer
    
    def _add_inline_citations(self, answer: str, sources_used: Dict[str, Any]) -> str:
        """Add inline citations to answer text."""
        
        # Extract entity and relationship IDs for citation
        entity_ids = list(sources_used.get('entities', {}).keys())
        rel_ids = list(sources_used.get('relationships', {}).keys())
        source_ids = list(sources_used.get('sources', {}).keys())
        
        # Split answer into paragraphs
        paragraphs = answer.split('\n\n')
        cited_paragraphs = []
        
        for para in paragraphs:
            if not para.strip():
                cited_paragraphs.append(para)
                continue
            
            # Determine which sources are relevant to this paragraph
            relevant_entities = []
            relevant_rels = []
            relevant_sources = []
            
            # Simple relevance check based on entity mentions
            para_lower = para.lower()
            
            for eid, entity in sources_used.get('entities', {}).items():
                if entity['title'].lower() in para_lower or \
                   any(word in para_lower for word in entity.get('description', '').lower().split()[:10]):
                    relevant_entities.append(str(eid))
            
            for rid, rel in sources_used.get('relationships', {}).items():
                if any(word in para_lower for word in rel.get('description', '').lower().split()[:10]):
                    relevant_rels.append(str(rid))
            
            # Add generic source references
            if relevant_entities or relevant_rels:
                relevant_sources = source_ids[:3]  # Use first few sources
            
            # Build citation
            if relevant_entities or relevant_rels or relevant_sources:
                citation_parts = []
                
                if relevant_sources:
                    citation_parts.append(f"Sources ({', '.join(map(str, relevant_sources[:5]))})")
                
                if relevant_entities:
                    citation_parts.append(f"Entities ({', '.join(relevant_entities[:7])})")
                
                if relevant_rels:
                    citation_parts.append(f"Relationships ({', '.join(relevant_rels[:5])})")
                
                citation = f" Data: {'; '.join(citation_parts)}."
                cited_paragraphs.append(para + citation)
            else:
                cited_paragraphs.append(para)
        
        return '\n\n'.join(cited_paragraphs)
    
    def _format_data_sources(self, sources_used: Dict[str, Any]) -> Dict[str, List[Any]]:
        """Format sources for display."""
        return {
            'entities': list(sources_used.get('entities', {}).values()),
            'relationships': list(sources_used.get('relationships', {}).values()),
            'sources': list(sources_used.get('sources', {}).values()),
            'text_units': list(sources_used.get('text_units', {}).values())
        }
    
    def _extract_sources_from_global_response(self, response: str) -> Dict[str, Any]:
        """Extract sources from global search response with actual data lookup."""
        sources_used = {
            'entities': {},
            'relationships': {},
            'sources': {},
            'text_units': {}
        }
        
        try:
            import pandas as pd
            
            # Load GraphRAG output files
            entities_path = self.graphrag_root / "output" / "entities.parquet"
            community_reports_path = self.graphrag_root / "output" / "community_reports.parquet"
            
            entities_df = None
            reports_df = None
            
            if entities_path.exists():
                entities_df = pd.read_parquet(entities_path)
            if community_reports_path.exists():
                reports_df = pd.read_parquet(community_reports_path)
            
            # Parse community references and entity IDs from response
            import re
            entity_matches = re.findall(r'Entities\s*\(([^)]+)\)', response)
            
            for match in entity_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for entity_id_str in ids:
                    entity_id = int(entity_id_str)
                    
                    # Look up actual entity data
                    if entities_df is not None and entity_id in entities_df.index:
                        entity_row = entities_df.loc[entity_id]
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': entity_row.get('title', f'Entity {entity_id}'),
                            'type': entity_row.get('type', 'Unknown'),
                            'description': entity_row.get('description', 'No description available')[:200]
                        }
                    else:
                        # Fallback if not found
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': f'Entity {entity_id}',
                            'type': 'Unknown',
                            'description': 'Entity not found in output'
                        }
            
            # Parse community report references
            report_matches = re.findall(r'Reports\s*\(([^)]+)\)', response)
            
            for match in report_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for report_id_str in ids:
                    report_id = int(report_id_str)
                    
                    # Look up actual community report data
                    if reports_df is not None and report_id in reports_df.index:
                        report_row = reports_df.loc[report_id]
                        sources_used['sources'][report_id] = {
                            'id': report_id,
                            'title': f"Community Report #{report_id}",
                            'type': 'community_report',
                            'text_preview': report_row.get('summary', 'No summary available')[:100]
                        }
                    else:
                        # Fallback if not found
                        sources_used['sources'][report_id] = {
                            'id': report_id,
                            'title': f'Community Report {report_id}',
                            'type': 'community_report',
                            'text_preview': 'Report not found in output'
                        }
        
        except Exception as e:
            logger.error(f"Error loading GraphRAG data for global source extraction: {e}")
            import traceback
            traceback.print_exc()
        
        return sources_used
    
    def _extract_sources_from_drift_response(self, response: str) -> Dict[str, Any]:
        """Extract sources from DRIFT search response with actual data lookup."""
        sources_used = {
            'entities': {},
            'relationships': {},
            'sources': {},
            'text_units': {}
        }
        
        try:
            import pandas as pd
            
            # Load GraphRAG output files
            entities_path = self.graphrag_root / "output" / "entities.parquet"
            relationships_path = self.graphrag_root / "output" / "relationships.parquet"
            text_units_path = self.graphrag_root / "output" / "text_units.parquet"
            
            entities_df = None
            relationships_df = None
            text_units_df = None
            
            if entities_path.exists():
                entities_df = pd.read_parquet(entities_path)
            if relationships_path.exists():
                relationships_df = pd.read_parquet(relationships_path)
            if text_units_path.exists():
                text_units_df = pd.read_parquet(text_units_path)
            
            # Parse entity references from DRIFT response
            import re
            entity_matches = re.findall(r'Entities\s*\(([^)]+)\)', response)
            
            for match in entity_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for entity_id_str in ids:
                    entity_id = int(entity_id_str)
                    
                    # Look up actual entity data
                    if entities_df is not None and entity_id in entities_df.index:
                        entity_row = entities_df.loc[entity_id]
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': entity_row.get('title', f'Entity {entity_id}'),
                            'type': entity_row.get('type', 'Unknown'),
                            'description': entity_row.get('description', 'No description available')[:200]
                        }
                    else:
                        # Fallback if not found
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': f'Entity {entity_id}',
                            'type': 'Unknown',
                            'description': 'Entity not found in output'
                        }
            
            # Parse relationship references
            rel_matches = re.findall(r'Relationships\s*\(([^)]+)\)', response)
            
            for match in rel_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for rel_id_str in ids:
                    rel_id = int(rel_id_str)
                    
                    # Look up actual relationship data
                    if relationships_df is not None and rel_id in relationships_df.index:
                        rel_row = relationships_df.loc[rel_id]
                        sources_used['relationships'][rel_id] = {
                            'id': rel_id,
                            'source': rel_row.get('source', f'Relationship {rel_id}'),
                            'target': rel_row.get('target', 'Unknown'),
                            'description': rel_row.get('description', 'No description available')[:200],
                            'weight': rel_row.get('weight', 0.0)
                        }
                    else:
                        # Fallback if not found
                        sources_used['relationships'][rel_id] = {
                            'id': rel_id,
                            'source': f'Relationship {rel_id}',
                            'target': 'Unknown',
                            'description': 'Relationship not found in output',
                            'weight': 0.0
                        }
            
            # Parse source references (text units)
            source_matches = re.findall(r'Sources\s*\(([^)]+)\)', response)
            
            for match in source_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for source_id_str in ids:
                    source_id = int(source_id_str)
                    
                    # Look up actual text unit data
                    if text_units_df is not None and source_id in text_units_df.index:
                        source_row = text_units_df.loc[source_id]
                        sources_used['sources'][source_id] = {
                            'id': source_id,
                            'title': source_row.get('document_ids', f'Source {source_id}'),
                            'type': 'text_unit',
                            'text_preview': source_row.get('text', 'No text available')[:100]
                        }
                    else:
                        # Fallback if not found
                        sources_used['sources'][source_id] = {
                            'id': source_id,
                            'title': f'Source {source_id}',
                            'type': 'document',
                            'text_preview': 'Source not found in output'
                        }
        
        except Exception as e:
            logger.error(f"Error loading GraphRAG data for DRIFT source extraction: {e}")
            import traceback
            traceback.print_exc()
        
        return sources_used
    
    def _extract_sources_from_local_response(self, response: str) -> Dict[str, Any]:
        """Extract sources from local search response with actual data lookup."""
        sources_used = {
            'entities': {},
            'relationships': {},
            'sources': {},
            'text_units': {}
        }
        
        try:
            import pandas as pd
            
            # Load GraphRAG output files
            entities_path = self.graphrag_root / "output" / "entities.parquet"
            relationships_path = self.graphrag_root / "output" / "relationships.parquet"
            text_units_path = self.graphrag_root / "output" / "text_units.parquet"
            
            entities_df = None
            relationships_df = None
            text_units_df = None
            
            if entities_path.exists():
                entities_df = pd.read_parquet(entities_path)
            if relationships_path.exists():
                relationships_df = pd.read_parquet(relationships_path)
            if text_units_path.exists():
                text_units_df = pd.read_parquet(text_units_path)
            
            # Parse entity references from local response
            import re
            entity_matches = re.findall(r'Entities\s*\(([^)]+)\)', response)
            
            for match in entity_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for entity_id_str in ids:
                    entity_id = int(entity_id_str)
                    
                    # Look up actual entity data
                    if entities_df is not None and entity_id in entities_df.index:
                        entity_row = entities_df.loc[entity_id]
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': entity_row.get('title', f'Entity {entity_id}'),
                            'type': entity_row.get('type', 'Unknown'),
                            'description': entity_row.get('description', 'No description available')[:200]
                        }
                    else:
                        # Fallback if not found
                        sources_used['entities'][entity_id] = {
                            'id': entity_id,
                            'title': f'Entity {entity_id}',
                            'type': 'Unknown',
                            'description': 'Entity not found in output'
                        }
            
            # Parse relationship references
            rel_matches = re.findall(r'Relationships\s*\(([^)]+)\)', response)
            
            for match in rel_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for rel_id_str in ids:
                    rel_id = int(rel_id_str)
                    
                    # Look up actual relationship data
                    if relationships_df is not None and rel_id in relationships_df.index:
                        rel_row = relationships_df.loc[rel_id]
                        sources_used['relationships'][rel_id] = {
                            'id': rel_id,
                            'source': rel_row.get('source', f'Relationship {rel_id}'),
                            'target': rel_row.get('target', 'Unknown'),
                            'description': rel_row.get('description', 'No description available')[:200],
                            'weight': rel_row.get('weight', 0.0)
                        }
                    else:
                        # Fallback if not found
                        sources_used['relationships'][rel_id] = {
                            'id': rel_id,
                            'source': f'Relationship {rel_id}',
                            'target': 'Unknown',
                            'description': 'Relationship not found in output',
                            'weight': 0.0
                        }
            
            # Parse source references (text units)
            source_matches = re.findall(r'Sources\s*\(([^)]+)\)', response)
            
            for match in source_matches:
                ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
                for source_id_str in ids:
                    source_id = int(source_id_str)
                    
                    # Look up actual text unit data
                    if text_units_df is not None and source_id in text_units_df.index:
                        source_row = text_units_df.loc[source_id]
                        sources_used['sources'][source_id] = {
                            'id': source_id,
                            'title': source_row.get('document_ids', f'Source {source_id}'),
                            'type': 'text_unit',
                            'text_preview': source_row.get('text', 'No text available')[:100]
                        }
                    else:
                        # Fallback if not found
                        sources_used['sources'][source_id] = {
                            'id': source_id,
                            'title': f'Source {source_id}',
                            'type': 'document',
                            'text_preview': 'Source not found in output'
                        }
            
        except Exception as e:
            logger.error(f"Error loading GraphRAG data for source extraction: {e}")
            import traceback
            traceback.print_exc()
        
        return sources_used
    

    
    async def _execute_query(self, question: str, method: str, **kwargs) -> Dict[str, Any]:
        """Execute the actual query with original functionality."""
        
        # Auto-route if method not specified
        if method is None:
            route_info = self.router.determine_query_method(question)
            method = route_info['method']
            params = route_info['params']
            intent = route_info['intent']
            
            # Log the routing decision
            logger.info(f"Query: {question}")
            logger.info(f"Routed to: {method} (intent: {intent.value})")
            
            # Check if multiple entities detected
            if 'multiple_entities' in params:
                entity_count = len(params['multiple_entities'])
                logger.info(f"Detected {entity_count} entities in query")
                logger.info(f"Query focus: {'comparison' if params.get('comparison_mode') else 'specific' if params.get('strict_entity_focus') else 'contextual'}")
        else:
            params = kwargs
            intent = None
        
        # Execute query based on method
        if method == "global":
            result = await self._execute_global_query(question, params)
        elif method == "local":
            result = await self._execute_local_query(question, params)
        elif method == "drift":
            result = await self._execute_drift_query(question, params)
        else:
            raise ValueError(f"Unknown query method: {method}")
        
        # Add routing metadata to result
        result['routing_metadata'] = {
            'detected_intent': self._get_intent_type(params),
            'community_context_enabled': params.get('include_community_context', True),
            'query_method': method,
            'entity_count': len(params.get('multiple_entities', [])) if 'multiple_entities' in params else 1 if 'entity_filter' in params else 0
        }
        
        # Extract comprehensive source information
        sources_info = self._extract_sources_from_response(
            result.get('answer', ''), 
            result.get('query_type', method)
        )
        
        # Add both the sources info and the entity chunks if this is a local search
        result['sources_info'] = sources_info
        
        # For local searches, also get the actual entity chunks that were used
        if method == "local" or result.get('query_type') == 'local':
            result['entity_chunks'] = await self._get_entity_chunks(question, params)
        
        return result
    
    async def _extract_data_sources(self, result: Dict[str, Any], method: str) -> Dict[str, Any]:
        """Extract entities, relationships, and sources from query result."""
        data_sources = {
            'entities': [],
            'relationships': [],
            'sources': [],
            'communities': [],
            'text_units': []
        }
        
        try:
            # For local search
            if method == 'local' and 'context_data' in result:
                context = result['context_data']
                
                # Extract entity IDs and details
                if 'entities' in context:
                    entities_df = context['entities']
                    data_sources['entities'] = [
                        {
                            'id': idx,
                            'title': row.get('title', 'Unknown'),
                            'type': row.get('type', 'Unknown'),
                            'description': row.get('description', '')[:100] + '...' if len(row.get('description', '')) > 100 else row.get('description', '')
                        }
                        for idx, row in entities_df.iterrows()
                    ]
                
                # Extract relationship IDs and details
                if 'relationships' in context:
                    relationships_df = context['relationships']
                    data_sources['relationships'] = [
                        {
                            'id': idx,
                            'source': row.get('source', ''),
                            'target': row.get('target', ''),
                            'description': row.get('description', '')[:100] + '...',
                            'weight': row.get('weight', 0)
                        }
                        for idx, row in relationships_df.iterrows()
                    ]
                
                # Extract source documents
                if 'sources' in context:
                    sources_df = context['sources']
                    data_sources['sources'] = [
                        {
                            'id': idx,
                            'title': row.get('title', 'Unknown'),
                            'chunk_id': row.get('chunk_id', ''),
                            'document_type': row.get('document_type', 'Unknown')
                        }
                        for idx, row in sources_df.iterrows()
                    ]
            
            # For global search
            elif method == 'global' and 'context_data' in result:
                context = result['context_data']
                
                # Extract community information
                if 'communities' in context:
                    communities = context['communities']
                    data_sources['communities'] = [
                        {
                            'id': comm.get('id', ''),
                            'title': comm.get('title', 'Community'),
                            'level': comm.get('level', 0),
                            'entity_count': len(comm.get('entities', []))
                        }
                        for comm in communities
                    ]
                
                # Extract entities from communities
                for comm in context.get('communities', []):
                    for entity_id in comm.get('entities', []):
                        # Load entity details
                        entity = await self._get_entity_by_id(entity_id)
                        if entity:
                            data_sources['entities'].append({
                                'id': entity_id,
                                'title': entity.get('title', 'Unknown'),
                                'type': entity.get('type', 'Unknown'),
                                'from_community': comm.get('id', '')
                            })
            
            # Extract text units if available
            if 'text_units' in result.get('context_data', {}):
                text_units = result['context_data']['text_units']
                data_sources['text_units'] = [
                    {
                        'id': unit.get('id', ''),
                        'chunk_id': unit.get('chunk_id', ''),
                        'document': unit.get('document', ''),
                        'text_preview': unit.get('text', '')[:100] + '...'
                    }
                    for unit in text_units[:10]  # Limit to first 10
                ]
                
        except Exception as e:
            logger.error(f"Error extracting data sources: {e}")
            import traceback
            traceback.print_exc()
        
        return data_sources
    
    async def _get_entity_by_id(self, entity_id: int):
        """Get entity details by ID."""
        try:
            entities_path = self.graphrag_root / "output" / "entities.parquet"
            if entities_path.exists():
                import pandas as pd
                entities_df = pd.read_parquet(entities_path)
                if entity_id in entities_df.index:
                    return entities_df.loc[entity_id].to_dict()
        except Exception as e:
            logger.error(f"Error loading entity {entity_id}: {e}")
        return None

    async def _extract_local_context(self, query: str, **kwargs) -> Dict[str, Any]:
        """Manually extract context data for local search."""
        context = {
            'entities': None,
            'relationships': None,
            'sources': None,
            'text_units': []
        }
        
        try:
            import pandas as pd
            
            # Load data files
            entities_path = self.graphrag_root / "output" / "entities.parquet"
            relationships_path = self.graphrag_root / "output" / "relationships.parquet"
            text_units_path = self.graphrag_root / "output" / "text_units.parquet"
            
            # Get top-k entities
            if entities_path.exists():
                entities_df = pd.read_parquet(entities_path)
                
                # Filter based on query relevance (simple keyword matching for now)
                query_terms = query.lower().split()
                relevant_entities = []
                
                for idx, entity in entities_df.iterrows():
                    title = str(entity.get('title', '')).lower()
                    description = str(entity.get('description', '')).lower()
                    
                    # Check if any query term matches
                    if any(term in title or term in description for term in query_terms):
                        relevant_entities.append(idx)
                
                # Get top-k relevant entities
                top_k = kwargs.get('top_k_entities', 10)
                context['entities'] = entities_df.loc[relevant_entities[:top_k]]
                
                # Get relationships for these entities
                if relationships_path.exists() and len(relevant_entities) > 0:
                    relationships_df = pd.read_parquet(relationships_path)
                    
                    # Filter relationships involving our entities
                    entity_set = set(relevant_entities[:top_k])
                    relevant_rels = relationships_df[
                        relationships_df['source'].isin(entity_set) | 
                        relationships_df['target'].isin(entity_set)
                    ]
                    
                    context['relationships'] = relevant_rels
            
            # Get text units
            if text_units_path.exists():
                text_units_df = pd.read_parquet(text_units_path)
                
                # Get relevant text units (simplified - in practice would use embeddings)
                relevant_units = []
                for idx, unit in text_units_df.iterrows():
                    text = str(unit.get('text', '')).lower()
                    if any(term in text for term in query.lower().split()):
                        relevant_units.append({
                            'id': idx,
                            'text': unit.get('text', ''),
                            'chunk_id': unit.get('chunk_id', ''),
                            'document': unit.get('document', '')
                        })
                
                context['text_units'] = relevant_units[:10]
            
            # Extract source documents
            if context['entities'] is not None and not context['entities'].empty:
                # Get unique source documents from entities
                sources = []
                for idx, entity in context['entities'].iterrows():
                    if 'source_document' in entity:
                        sources.append({
                            'id': len(sources),
                            'title': entity['source_document'],
                            'document_type': entity.get('document_type', 'Unknown'),
                            'chunk_id': entity.get('chunk_id', '')
                        })
                
                # Deduplicate sources
                seen = set()
                unique_sources = []
                for source in sources:
                    key = source['title']
                    if key not in seen:
                        seen.add(key)
                        unique_sources.append(source)
                
                context['sources'] = pd.DataFrame(unique_sources)
                
        except Exception as e:
            logger.error(f"Error extracting context: {e}")
            import traceback
            traceback.print_exc()
        
        return context

    def _get_intent_type(self, params: Dict) -> str:
        """Determine intent type from parameters."""
        if params.get('comparison_mode'):
            return 'comparison'
        elif params.get('strict_entity_focus'):
            return 'specific_entity'
        elif params.get('focus_on_relationships'):
            return 'relationships'
        else:
            return 'contextual'
    
    async def _execute_global_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a global search query."""
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "global"
        ]
        
        if "community_level" in params:
            cmd.extend(["--community-level", str(params["community_level"])])
        
        cmd.extend(["--query", question])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Clean JSON artifacts from the response
        answer = self._clean_json_artifacts(result.stdout)
        
        return {
            "query": question,
            "query_type": "global",
            "answer": answer,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    async def _execute_local_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a local search query with available GraphRAG options."""
        
        # Handle multiple entities
        if "multiple_entities" in params:
            return await self._execute_multi_entity_query(question, params)
        
        # Single entity query - use available GraphRAG options
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "local"
        ]
        
        # Use community-level to control context (available option)
        if params.get("disable_community", False):
            # Use highest community level to get most specific results
            cmd.extend(["--community-level", "3"])  
            logger.info("Using high community level (3) for specific entity query")
        else:
            # Use default community level for broader context
            cmd.extend(["--community-level", "2"])
            logger.info("Using default community level (2) for contextual query")
        
        # If we have entity filtering request, modify the query to be more specific
        if "entity_filter" in params:
            filter_info = params["entity_filter"]
            entity_type = filter_info['type'].replace('_', ' ').lower()
            entity_value = filter_info['value']
            
            if params.get("strict_entity_focus", False):
                # Make query more specific to focus on just this entity
                enhanced_question = f"Tell me specifically about {entity_type} {entity_value}. Focus only on {entity_value} and do not include information about other items."
                logger.info(f"Enhanced query for strict focus on {entity_value}")
            else:
                # Keep original query but mention the entity
                enhanced_question = f"{question} (specifically about {entity_type} {entity_value})"
                logger.info(f"Enhanced query for contextual information about {entity_value}")
            
            question = enhanced_question
        
        cmd.extend(["--query", question])
        
        logger.debug(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        answer = result.stdout
        
        # Clean JSON artifacts from the response
        answer = self._clean_json_artifacts(answer)
        
        # Post-process if strict entity focus is requested
        if params.get("strict_entity_focus", False) and "entity_filter" in params:
            answer = self._filter_to_specific_entity(answer, params["entity_filter"]["value"])
        
        return {
            "query": question,
            "query_type": "local",
            "answer": answer,
            "context": self._extract_context(answer),
            "parameters": params,
            "intent_detection": {
                "specific_entity_focus": params.get("strict_entity_focus", False),
                "community_level_used": 3 if params.get("disable_community") else 2,
                "query_enhanced": "entity_filter" in params
            }
        }
    
    async def _execute_multi_entity_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute queries for multiple entities using available GraphRAG options."""
        entities = params["multiple_entities"]
        all_results = []
        
        # Determine query strategy based on intent
        if params.get("aggregate_results") and params.get("strict_entity_focus"):
            # Query each entity separately with high community level
            logger.info(f"Executing separate queries for {len(entities)} entities")
            
            for entity in entities:
                cmd = [
                    self._get_python_executable(),
                    "-m", "graphrag", "query",
                    "--root", str(self.graphrag_root),
                    "--method", "local",
                    "--community-level", "3"  # High level for specific results
                ]
                
                # Create entity-specific query
                entity_type = entity['type'].replace('_', ' ').lower()
                entity_query = f"Tell me specifically about {entity_type} {entity['value']}. Focus only on {entity['value']}."
                cmd.extend(["--query", entity_query])
                
                result = subprocess.run(cmd, capture_output=True, text=True)
                all_results.append({
                    "entity": entity,
                    "answer": result.stdout
                })
            
            # Combine results
            combined_answer = self._format_multiple_entity_results(all_results, params)
            
        elif params.get("comparison_mode"):
            # Query with all entities for comparison
            logger.info(f"Executing comparison query for entities: {[e['value'] for e in entities]}")
            
            entity_values = [e['value'] for e in entities]
            comparison_query = f"Compare and contrast {' and '.join(entity_values)}. What are the similarities and differences between these items?"
            
            cmd = [
                self._get_python_executable(),
                "-m", "graphrag", "query",
                "--root", str(self.graphrag_root),
                "--method", "local",
                "--community-level", "2",  # Medium level for comparison context
                "--query", comparison_query
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            combined_answer = self._format_comparison_results(result.stdout, entities)
            
        else:
            # Query for relationships between entities
            logger.info(f"Executing relationship query for entities: {[e['value'] for e in entities]}")
            
            entity_values = [e['value'] for e in entities]
            relationship_query = f"How do {' and '.join(entity_values)} relate to each other? What connections exist between these items?"
            
            cmd = [
                self._get_python_executable(),
                "-m", "graphrag", "query",
                "--root", str(self.graphrag_root),
                "--method", "local",
                "--community-level", "1",  # Lower level for broader relationships
                "--query", relationship_query
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            combined_answer = result.stdout
        
        # Clean JSON artifacts from the combined answer
        combined_answer = self._clean_json_artifacts(combined_answer)
        
        return {
            "query": question,
            "query_type": "local",
            "answer": combined_answer,
            "context": self._extract_context(combined_answer),
            "parameters": params,
            "intent_detection": {
                "multi_entity_query": True,
                "entity_count": len(entities),
                "query_mode": "comparison" if params.get("comparison_mode") else "aggregate" if params.get("aggregate_results") else "relationships"
            }
        }
    
    def _format_multiple_entity_results(self, results: List[Dict], params: Dict) -> str:
        """Format results from multiple individual entity queries."""
        formatted = []
        
        formatted.append(f"Information about {len(results)} requested items:\n")
        
        for i, result in enumerate(results, 1):
            entity = result['entity']
            answer = result['answer'].strip()
            
            formatted.append(f"\n{i}. {entity['type'].replace('_', ' ').title()} {entity['value']}:")
            formatted.append("-" * 50)
            
            # Clean and format the answer
            if answer:
                # Remove any GraphRAG metadata/headers and JSON artifacts
                clean_answer = self._clean_graphrag_output(answer)
                clean_answer = self._clean_json_artifacts(clean_answer)
                formatted.append(clean_answer)
            else:
                formatted.append(f"No information found for {entity['value']}")
        
        return "\n".join(formatted)
    
    def _format_comparison_results(self, raw_answer: str, entities: List[Dict]) -> str:
        """Format comparison results to highlight differences and similarities."""
        # Clean JSON artifacts from the raw answer
        clean_answer = self._clean_json_artifacts(raw_answer)
        
        # This could be enhanced with more sophisticated formatting
        formatted = [f"Comparison of {', '.join([e['value'] for e in entities])}:\n"]
        formatted.append(clean_answer)
        
        return "\n".join(formatted)
    
    def _clean_graphrag_output(self, output: str) -> str:
        """Remove GraphRAG metadata and format output cleanly."""
        # Remove common GraphRAG headers/footers
        lines = output.split('\n')
        cleaned_lines = []
        
        for line in lines:
            # Skip metadata lines
            if line.startswith('INFO:') or line.startswith('WARNING:') or line.startswith('DEBUG:'):
                continue
            # Skip empty lines at start/end
            if not line.strip() and (not cleaned_lines or len(cleaned_lines) == len(lines) - 1):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines).strip()
    
    async def _execute_drift_query(self, question: str, params: Dict) -> Dict[str, Any]:
        """Execute a DRIFT search query."""
        cmd = [
            self._get_python_executable(),
            "-m", "graphrag", "query",
            "--root", str(self.graphrag_root),
            "--method", "drift"
        ]
        
        cmd.extend(["--query", question])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Clean JSON artifacts from the response
        answer = self._clean_json_artifacts(result.stdout)
        
        return {
            "query": question,
            "query_type": "drift",
            "answer": answer,
            "context": self._extract_context(result.stdout),
            "parameters": params
        }
    
    def _filter_to_specific_entity(self, response: str, target_entity: str) -> str:
        """Aggressively filter response to ONLY information about the target entity."""
        if not target_entity or not response:
            return response
        
        # Split response into sentences
        sentences = response.split('.')
        filtered_sentences = []
        
        for sentence in sentences:
            # Only keep sentences that explicitly mention the target entity
            if target_entity in sentence:
                # Check if any other entity codes are mentioned
                other_entities = re.findall(r'\b[A-Z]-\d+\b', sentence)
                other_entities = [e for e in other_entities if e != target_entity]
                
                # Only keep if no other entities are mentioned
                if not other_entities:
                    filtered_sentences.append(sentence.strip())
        
        if filtered_sentences:
            filtered_response = '. '.join(filtered_sentences) + '.'
            filtered_response = f"Information specifically about {target_entity}:\n\n{filtered_response}"
        else:
            filtered_response = f"Specific information about {target_entity} only."
        
        return filtered_response
    
    def _is_paragraph_about_target(self, paragraph: str, target: str, all_entities: set) -> bool:
        """Determine if a paragraph should be kept in filtered response."""
        if target not in paragraph:
            return False
        
        other_entities = all_entities - {target}
        if not any(entity in paragraph for entity in other_entities):
            return True
        
        target_count = paragraph.count(target)
        other_counts = sum(paragraph.count(entity) for entity in other_entities)
        
        return target_count >= other_counts
    
    def _extract_sources_from_response(self, response: str, method: str) -> Dict[str, Any]:
        """Extract and resolve all source references from GraphRAG response."""
        sources_info = {
            'entities': [],
            'reports': [],
            'raw_references': {},
            'resolved_sources': []
        }
        
        import re
        
        # Parse all reference patterns
        entities_pattern = r'Entities\s*\(([^)]+)\)'
        reports_pattern = r'Reports\s*\(([^)]+)\)'
        sources_pattern = r'Sources\s*\(([^)]+)\)'
        data_pattern = r'Data:\s*(?:Sources\s*\([^)]+\);\s*)?(?:Entities\s*\([^)]+\)|Reports\s*\([^)]+\))'
        
        # Extract all matches
        entities_matches = re.findall(entities_pattern, response)
        reports_matches = re.findall(reports_pattern, response)
        sources_matches = re.findall(sources_pattern, response)
        
        # Store raw references
        sources_info['raw_references'] = {
            'entities': entities_matches,
            'reports': reports_matches,
            'sources': sources_matches
        }
        
        # Parse entity IDs
        all_entity_ids = []
        for match in entities_matches:
            ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
            all_entity_ids.extend(ids)
        
        # Parse report IDs
        all_report_ids = []
        for match in reports_matches:
            ids = [id.strip() for id in match.split(',') if id.strip().replace('+more', '').strip().isdigit()]
            all_report_ids.extend(ids)
        
        # Load and resolve entities
        if all_entity_ids:
            try:
                entities_path = self.graphrag_root / "output/entities.parquet"
                if entities_path.exists():
                    import pandas as pd
                    entities_df = pd.read_parquet(entities_path)
                    
                    for entity_id in all_entity_ids:
                        try:
                            entity_idx = int(entity_id)
                            if entity_idx in entities_df.index:
                                entity = entities_df.loc[entity_idx]
                                sources_info['entities'].append({
                                    'id': entity_idx,
                                    'title': entity['title'],
                                    'type': entity['type'],
                                    'description': entity.get('description', '')[:300]
                                })
                        except Exception as e:
                            logger.error(f"Failed to resolve entity {entity_id}: {e}")
            except Exception as e:
                logger.error(f"Failed to load entities: {e}")
        
        # Load and resolve community reports
        if all_report_ids:
            try:
                reports_path = self.graphrag_root / "output/community_reports.parquet"
                if reports_path.exists():
                    import pandas as pd
                    reports_df = pd.read_parquet(reports_path)
                    
                    for report_id in all_report_ids:
                        try:
                            report_idx = int(report_id)
                            if report_idx in reports_df.index:
                                report = reports_df.loc[report_idx]
                                sources_info['reports'].append({
                                    'id': report_idx,
                                    'title': f"Community Report #{report_idx}",
                                    'summary': report.get('summary', '')[:300] if 'summary' in report else str(report)[:300],
                                    'level': report.get('level', 'unknown') if 'level' in report else 'unknown'
                                })
                        except Exception as e:
                            logger.error(f"Failed to resolve report {report_id}: {e}")
            except Exception as e:
                logger.error(f"Failed to load reports: {e}")
        
        # Create resolved sources list combining everything
        sources_info['resolved_sources'] = sources_info['entities'] + sources_info['reports']
        
        return sources_info

    async def _get_retrieved_entities(self, query: str, params: Dict) -> List[Dict]:
        """Get the actual entities that GraphRAG retrieved for this query."""
        source_entities = []
        
        try:
            entities_path = self.graphrag_root / "output/entities.parquet"
            text_units_path = self.graphrag_root / "output/text_units.parquet"
            relationships_path = self.graphrag_root / "output/relationships.parquet"
            
            if entities_path.exists():
                import pandas as pd
                entities_df = pd.read_parquet(entities_path)
                
                # If we have an entity filter, find that specific entity
                if 'entity_filter' in params:
                    filter_info = params['entity_filter']
                    entity_value = filter_info['value']
                    entity_type = filter_info['type']
                    
                    # Find matching entities by type and value
                    matches = entities_df[
                        (entities_df['type'].str.upper() == entity_type.upper()) &
                        (entities_df['title'].str.contains(entity_value, case=False, na=False))
                    ]
                    
                    # Also find related entities through relationships
                    if relationships_path.exists() and not matches.empty:
                        relationships_df = pd.read_parquet(relationships_path)
                        
                        for _, entity in matches.iterrows():
                            entity_id = entity.name  # Index is the entity ID
                            
                            # Find all relationships involving this entity
                            related_rels = relationships_df[
                                (relationships_df['source'] == entity_id) | 
                                (relationships_df['target'] == entity_id)
                            ]
                            
                            # Add the main entity
                            source_entities.append({
                                'entity_id': entity_id,
                                'title': entity['title'],
                                'type': entity['type'],
                                'description': entity.get('description', '')[:500],
                                'is_primary': True,
                                'source_document': self._trace_entity_to_document(entity_id, entity['title'])
                            })
                            
                            # Add related entities
                            for _, rel in related_rels.iterrows():
                                other_id = rel['target'] if rel['source'] == entity_id else rel['source']
                                if other_id in entities_df.index:
                                    related_entity = entities_df.loc[other_id]
                                    source_entities.append({
                                        'entity_id': other_id,
                                        'title': related_entity['title'],
                                        'type': related_entity['type'],
                                        'description': related_entity.get('description', '')[:300],
                                        'is_primary': False,
                                        'relationship': rel['description'],
                                        'source_document': self._trace_entity_to_document(other_id, related_entity['title'])
                                    })
        
        except Exception as e:
            logger.error(f"Failed to get retrieved entities: {e}")
        
        return source_entities

    async def _get_entity_chunks(self, query: str, params: Dict) -> List[Dict]:
        """Get the actual text chunks/entities that were retrieved."""
        chunks = []
        
        try:
            # Load entities and text units
            entities_path = self.graphrag_root / "output/entities.parquet"
            text_units_path = self.graphrag_root / "output/text_units.parquet"
            
            if entities_path.exists():
                import pandas as pd
                entities_df = pd.read_parquet(entities_path)
                
                # If text units exist, load them too
                text_units_df = None
                if text_units_path.exists():
                    text_units_df = pd.read_parquet(text_units_path)
                
                # Get entities based on the query parameters
                if 'entity_filter' in params:
                    filter_info = params['entity_filter']
                    entity_value = filter_info['value']
                    entity_type = filter_info['type']
                    
                    # Find all matching entities
                    matches = entities_df[
                        (entities_df['type'].str.upper() == entity_type.upper()) & 
                        (entities_df['title'].str.contains(entity_value, case=False, na=False))
                    ]
                    
                    # Also get related entities by looking at descriptions
                    related = entities_df[
                        entities_df['description'].str.contains(entity_value, case=False, na=False)
                    ]
                    
                    all_matches = pd.concat([matches, related]).drop_duplicates()
                    
                    # Convert to chunks format
                    for _, entity in all_matches.iterrows():
                        chunk = {
                            'entity_id': entity.name,
                            'type': entity['type'],
                            'title': entity['title'],
                            'description': entity.get('description', ''),
                            'source': self._trace_entity_to_document(entity.name, entity['title'])
                        }
                        chunks.append(chunk)
        
        except Exception as e:
            logger.error(f"Failed to get entity chunks: {e}")
        
        return chunks

    def _trace_entity_to_document(self, entity_id, entity_title: str) -> Dict:
        """Trace an entity back to its source document."""
        try:
            csv_path = self.graphrag_root / "city_clerk_documents.csv"
            if csv_path.exists():
                import pandas as pd
                docs_df = pd.read_csv(csv_path)
                
                # Extract potential item code from entity title
                import re
                item_match = re.search(r'([A-Z]-\d+)', entity_title)
                doc_match = re.search(r'(\d{4}-\d+)', entity_title)
                
                # Try to find matching document
                for _, doc in docs_df.iterrows():
                    # Check if entity matches document identifiers
                    if (item_match and item_match.group(1) == doc.get('item_code')) or \
                       (doc_match and doc_match.group(1) in str(doc.get('document_number', ''))) or \
                       (entity_title.lower() in str(doc.get('title', '')).lower()):
                        return {
                            'document_id': doc['id'],
                            'title': doc.get('title', ''),
                            'type': doc.get('document_type', ''),
                            'meeting_date': doc.get('meeting_date', ''),
                            'source_file': doc.get('source_file', '')
                        }
        except Exception as e:
            logger.error(f"Failed to trace entity to document: {e}")
        
        return {}
    
    def _extract_context(self, response: str) -> List[Dict]:
        """Extract context and sources from response."""
        context = []
        # Parse response for entity references and sources
        return context

# Legacy compatibility class
class CityClerkGraphRAGQuery(CityClerkQueryEngine):
    """Legacy compatibility wrapper."""
    
    async def query(self, 
                    question: str, 
                    query_type: QueryType = QueryType.LOCAL,
                    community_level: int = 0) -> Dict[str, Any]:
        """Legacy query method for backward compatibility."""
        return await super().query(
            question=question,
            method=query_type.value,
            community_level=community_level
        )

# Example usage function
async def handle_user_query(question: str, graphrag_root: Path = None):
    """Handle user query with intelligent routing."""
    if graphrag_root is None:
        graphrag_root = Path("./graphrag_data")
    
    engine = CityClerkQueryEngine(graphrag_root)
    result = await engine.query(question)
    
    print(f"Query: {question}")
    print(f"Selected method: {result['query_type']}")
    print(f"Detected entities: {result['routing_metadata'].get('entity_count', 0)}")
    print(f"Query intent: {result['routing_metadata'].get('detected_intent', 'unknown')}")
    
    return result 