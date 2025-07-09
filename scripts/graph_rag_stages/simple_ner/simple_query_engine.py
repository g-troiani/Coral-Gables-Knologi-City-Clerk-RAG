"""
Simple NER-based query engine for fast entity-based retrieval.
Combines entity lookup with structural filtering.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict
from datetime import datetime
import re
import asyncio
from openai import AzureOpenAI
from difflib import SequenceMatcher
import os
import networkx as nx
from dataclasses import dataclass, field
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser, TemporalIndex
from .graph_query_agent import GraphQueryAgent
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient

log = logging.getLogger(__name__)


@dataclass
class GraphContext:
    """Context information from knowledge graph queries."""
    temporal_nodes: List[Dict] = field(default_factory=list)
    entity_nodes: List[Dict] = field(default_factory=list)
    meeting_nodes: List[Dict] = field(default_factory=list)
    document_ids: Set[str] = field(default_factory=set)
    requires_all_documents: bool = False
    query_type: str = "standard"
    date_range: Optional[Tuple[str, str]] = None


class SimpleNERQueryEngine:
    """Query engine for NER relationships + structural search."""
    
    # Constants for preventing context overflow
    MAX_CONTEXT_TOKENS = 100000  # Adjust based on model
    MAX_CHUNKS_PER_DOCUMENT = 5
    MAX_TOTAL_CHUNKS = 50
    
    def __init__(self, graph_dir: Path = Path("simple_ner_graph")):
        """Initialize the query engine with knowledge graph integration."""
        self.graph_dir = Path(graph_dir)
        self.chunks_dir = self.graph_dir / "document_chunks"
        
        # Load indices
        self.entity_index = self._load_entity_index()
        self.chunk_index = self._load_chunk_index()
        
        # Load knowledge graph
        self.graph = self._load_knowledge_graph()
        self.temporal_index = self._build_temporal_index()
        
        # Initialize Azure OpenAI for query analysis
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")

        # Initialize the GraphQueryAgent and Cosmos Client
        self.graph_query_agent = None
        try:
            cosmos_client = CosmosGraphClient()
            self.graph_query_agent = GraphQueryAgent(cosmos_client)
            log.info("✅ Graph Query Agent and Cosmos Client initialized successfully.")
        except ValueError as e:
            log.warning(f"⚠️ Cosmos DB not configured. Advanced graph queries will be unavailable: {e}")
        
    def _load_entity_index(self) -> Dict:
        """Load entity index from file."""
        index_path = self.graph_dir / "entity_index.json"
        if index_path.exists():
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            log.warning("Entity index not found")
            return {}
    
    def _load_chunk_index(self) -> Dict:
        """Load chunk index from file."""
        index_path = self.graph_dir / "chunk_index.json"
        if index_path.exists():
            with open(index_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            log.warning("Chunk index not found")
            return {}
    
    def _load_knowledge_graph(self) -> nx.DiGraph:
        """Load knowledge graph from GraphML file."""
        graph_path = Path("local_graph_data/city_clerk_graph.graphml")
        if graph_path.exists():
            try:
                graph = nx.read_graphml(str(graph_path))
                log.info(f"✅ Loaded knowledge graph with {len(graph.nodes)} nodes and {len(graph.edges)} edges")
                return graph
            except Exception as e:
                log.error(f"Failed to load knowledge graph: {e}")
                return nx.DiGraph()
        else:
            log.warning("Knowledge graph not found, using empty graph")
            return nx.DiGraph()
    
    def _build_temporal_index(self) -> TemporalIndex:
        """Build temporal index from knowledge graph nodes."""
        temporal_index = TemporalIndex()
        
        if self.graph:
            for node_id, attrs in self.graph.nodes(data=True):
                meeting_date = attrs.get('meeting_date', '')
                if meeting_date:
                    temporal_index.add_node(node_id, meeting_date)
            
            log.info(f"✅ Built temporal index with {len(temporal_index.date_to_nodes)} date entries")
        
        return temporal_index
    
    async def query(self, query_text: str, top_k: int = 10, comprehensive_mode: str = "limited") -> Dict[str, Any]:
        """
        Execute a hybrid NER + knowledge graph query with smart routing.
        
        Args:
            query_text: User's query
            top_k: Number of top chunks to retrieve (ignored for comprehensive queries)
            comprehensive_mode: Options: "limited" (default, uses smart limits), 
                               "full" (original behavior, may hit limits), 
                               "summary" (uses hierarchical summarization)
            
        Returns:
            Query results with answer and sources
        """
        # Prepend current date to query
        from datetime import datetime
        current_date = datetime.now().strftime("%B %d, %Y")
        query_text_with_date = f"The current date is {current_date}. {query_text}"
        
        log.info(f"Executing hybrid NER + graph query: {query_text_with_date}")
        
        # Step 1: Analyze query with enhanced intent classification
        query_analysis = await self._analyze_query(query_text_with_date)
        
        # Step 2: Detect comprehensive intent
        is_comprehensive = self._detect_comprehensive_intent(query_text_with_date)
        intent = query_analysis.get('intent', 'general_search')
        
        # Step 3: Route based on query type and intent
        if is_comprehensive and intent == 'temporal_search':
            return await self._temporal_comprehensive_flow(query_text_with_date, query_analysis, comprehensive_mode)
        elif is_comprehensive and 'meeting' in query_text_with_date.lower():
            return await self._meeting_comprehensive_flow(query_text_with_date, query_analysis, comprehensive_mode)
        elif intent == 'temporal_search':
            return await self._temporal_query_flow(query_text_with_date, query_analysis, top_k)
        elif 'meeting' in query_text_with_date.lower() and query_analysis.get('entities', {}).get('dates'):
            return await self._meeting_query_flow(query_text_with_date, query_analysis, top_k)
        else:
            return await self._standard_flow(query_text_with_date, query_analysis, top_k)
    
    async def _analyze_query(self, query_text: str) -> Dict[str, Any]:
        """Analyze query to extract entities and intent."""
        # First, try to extract date range using TemporalParser
        date_range = TemporalParser.extract_date_range(query_text)
        
        prompt = f"""Analyze this query and extract:
1. Named entities by category
2. Query intent (specific_lookup, temporal_search, document_filter, relationship_query)
3. Structural hints (document type, date range, etc.)
4. Detect if this is primarily a temporal/time-based query

Categories for entities:
- people: Names of officials, commissioners, citizens
- organizations: Departments, agencies, companies
- official_records: Ordinance/resolution numbers
- agenda_items: Item codes like E-1, F-10
- dates: Specific dates or date ranges
- dollar_amounts: Monetary values
- addresses: Street addresses, parcels
- named_locations: Buildings, areas
- actions: Legislative actions (approve, deny, etc.)
- document_types: agenda, ordinance, resolution, transcript

Query: {query_text}

{"Date range detected: " + str(date_range) if date_range else ""}

Instructions:
- For document_type in structural_hints, use ONLY the document type mentioned in the query (ordinance, resolution, agenda, etc.)
- If no specific document type is mentioned, leave document_type empty or null
- Do NOT default to 'agenda' - only use it if the query specifically asks for agenda items

Return JSON with this structure:
{{
    "entities": {{
        "people": ["name1", "name2"],
        "agenda_items": ["E-1"],
        "document_types": ["ordinance", "resolution"],
        ...
    }},
    "intent": "temporal_search",
    "structural_hints": {{
        "document_type": null,
        "date_range": {json.dumps(date_range) if date_range else 'null'},
        "needs_verbatim": false,
        "is_temporal_query": true
    }}
}}"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a query analyzer. Extract entities and intent from queries. Return only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "32768"))
            )
            
            result = response.choices[0].message.content.strip()
            
            # Parse JSON
            if '```json' in result:
                result = result.split('```json')[1].split('```')[0].strip()
            
            analysis = json.loads(result)
            
            # Ensure structure
            if 'entities' not in analysis:
                analysis['entities'] = {}
            if 'intent' not in analysis:
                analysis['intent'] = 'general_search'
            if 'structural_hints' not in analysis:
                analysis['structural_hints'] = {}
            
            # Override with TemporalParser results if available
            if date_range and not analysis['structural_hints'].get('date_range'):
                analysis['structural_hints']['date_range'] = list(date_range)
                analysis['structural_hints']['is_temporal_query'] = True
                if analysis['intent'] == 'general_search':
                    analysis['intent'] = 'temporal_search'
            
            # **NEW: Set document_type in structural_hints from entities if not already set**
            if not analysis['structural_hints'].get('document_type'):
                doc_types = analysis.get('entities', {}).get('document_types', [])
                if doc_types:
                    # Use the first document type found
                    analysis['structural_hints']['document_type'] = doc_types[0]
            
            return analysis
            
        except Exception as e:
            log.error(f"Query analysis failed: {e}")
            # Enhanced fallback
            fallback = self._simple_query_analysis(query_text)
            
            # Add temporal parsing to fallback
            if date_range:
                fallback['structural_hints']['date_range'] = list(date_range)
                fallback['structural_hints']['is_temporal_query'] = True
                fallback['intent'] = 'temporal_search'
            
            return fallback
    
    def _simple_query_analysis(self, query_text: str) -> Dict[str, Any]:
        """Simple fallback query analysis using patterns."""
        entities = defaultdict(list)
        
        # Extract agenda items (E-1, F-10, etc.)
        agenda_pattern = r'\b([A-Z]-?\d+)\b'
        entities['agenda_items'] = re.findall(agenda_pattern, query_text)
        
        # Extract dates (various formats) - more comprehensive
        date_patterns = [
            r'\b(\d{1,2}/\d{1,2}/\d{4})\b',  # MM/DD/YYYY
            r'\b(\d{1,2}\.\d{1,2}\.\d{4})\b',  # MM.DD.YYYY
            r'\b(\w+ \d{1,2}, \d{4})\b',  # Month DD, YYYY
            r'\b(\d{4}-\d{2}-\d{2})\b',  # YYYY-MM-DD
            r'\b(Q[1-4]\s+\d{4})\b',  # Q1 2024
            r'\b(\d{4})\b',  # Just year (20xx format)
            r'\b(first|second|third|fourth)\s+quarter\s+(\d{4})\b',  # written quarters
        ]
        for pattern in date_patterns:
            matches = re.findall(pattern, query_text, re.IGNORECASE)
            if isinstance(matches[0] if matches else None, tuple):
                # Handle tuples from patterns with multiple groups
                entities['dates'].extend([' '.join(match) for match in matches])
            else:
                entities['dates'].extend(matches)
        
        # Extract document numbers
        doc_patterns = [
            r'\b(Ord\.?\s*\d{4}-\d+)\b',
            r'\b(Res\.?\s*R?-?\d+-\d+)\b',
            r'\b(\d{4}-\d+)\b'
        ]
        for pattern in doc_patterns:
            entities['official_records'].extend(re.findall(pattern, query_text, re.IGNORECASE))
        
        # Extract document types
        doc_type_patterns = [
            (r'\bordinances?\b', 'ordinance'),
            (r'\bresolutions?\b', 'resolution'),
            (r'\bagendas?\b', 'agenda'),
            (r'\btranscripts?\b', 'transcript'),
            (r'\bverbatims?\b', 'verbatim'),
            (r'\bminutes?\b', 'minutes')
        ]
        for pattern, doc_type in doc_type_patterns:
            if re.search(pattern, query_text, re.IGNORECASE):
                entities['document_types'].append(doc_type)
        
        # Determine intent - better temporal detection
        intent = 'general_search'
        temporal_keywords = ['Q1', 'Q2', 'Q3', 'Q4', 'quarter', 'from', 'to', 'between', 'during', 'in', 'month', 'year', 'january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december']
        
        query_lower = query_text.lower()
        is_temporal = any(keyword in query_lower for keyword in temporal_keywords) or entities['dates']
        
        if entities['agenda_items']:
            intent = 'specific_lookup'
        elif 'all' in query_lower or 'list' in query_lower or 'show' in query_lower:
            if is_temporal:
                intent = 'temporal_search'
            else:
                intent = 'document_filter'
        elif is_temporal:
            intent = 'temporal_search'
        
        structural_hints = {}
        if is_temporal:
            structural_hints['is_temporal_query'] = True
            # Don't hardcode document type for temporal queries - let them find all types
        
        # **NEW: Set document_type from entities if found**
        if entities['document_types']:
            structural_hints['document_type'] = entities['document_types'][0]
        
        return {
            'entities': dict(entities),
            'intent': intent,
            'structural_hints': structural_hints
        }
    
    async def _ner_retrieval(self, entities: Dict[str, List[str]]) -> List[Tuple[str, float]]:
        """Retrieve chunks based on entity matches."""
        chunk_scores = defaultdict(float)
        
        for category, entity_list in entities.items():
            if category in self.entity_index:
                for entity in entity_list:
                    # Exact match
                    if entity in self.entity_index[category]:
                        chunk_ids = self.entity_index[category][entity]
                        for chunk_id in chunk_ids:
                            chunk_scores[chunk_id] += 1.0
                    
                    # Enhanced matching for official_records (resolutions, ordinances)
                    if category == 'official_records':
                        # Extract core number from different resolution formats
                        core_number = self._extract_resolution_number(entity)
                        if core_number:
                            for indexed_entity, chunk_ids in self.entity_index[category].items():
                                indexed_core = self._extract_resolution_number(indexed_entity)
                                if indexed_core and core_number == indexed_core:
                                    for chunk_id in chunk_ids:
                                        chunk_scores[chunk_id] += 1.0  # Full score for number match
                    
                    # Fuzzy match for partial matches
                    for indexed_entity, chunk_ids in self.entity_index[category].items():
                        similarity = self._calculate_similarity(entity.lower(), indexed_entity.lower())
                        if similarity > 0.8:  # High similarity threshold
                            for chunk_id in chunk_ids:
                                chunk_scores[chunk_id] += similarity * 0.8
        
        # Convert to sorted list
        return sorted(chunk_scores.items(), key=lambda x: x[1], reverse=True)
    
    async def _structural_retrieval(self, query_analysis: Dict) -> List[Tuple[str, float]]:
        """Retrieve chunks based on structural filters."""
        chunk_scores = defaultdict(float)
        hints = query_analysis.get('structural_hints', {})
        intent = query_analysis.get('intent', 'general_search')
        
        # Give bonus to temporal queries
        is_temporal = hints.get('is_temporal_query', False) or intent == 'temporal_search'
        
        for chunk_id, chunk_data in self.chunk_index.items():
            score = 0.0
            
            # Document type filter - be more inclusive for temporal queries
            if 'document_type' in hints:
                chunk_type = chunk_data.get('document_type', '').lower()
                hint_type = hints['document_type'].lower()
                
                if chunk_type == hint_type:
                    score += 0.5
                elif intent == 'specific_lookup' or is_temporal:
                    # For temporal queries, be more flexible and include all document types
                    if hint_type == 'agenda' and 'verbatim' in chunk_type:
                        score += 0.3
                    elif hint_type == 'agenda' and 'meeting' in chunk_type:
                        score += 0.4
            elif is_temporal:
                # For temporal queries without specific document type, include all documents
                # Give slight boost to ensure all document types are considered
                score += 0.1
            
                        # Enhanced date range filter
            if 'date_range' in hints and hints['date_range']:
                # Handle both list and string formats for date_range
                date_range_value = hints['date_range']
                target_date_start = None
                target_date_end = None
                
                if isinstance(date_range_value, list) and len(date_range_value) == 2:
                    target_date_start = date_range_value[0]
                    target_date_end = date_range_value[1]
                elif isinstance(date_range_value, str):
                    # Try to parse as JSON string
                    try:
                        import json
                        parsed_range = json.loads(date_range_value)
                        if isinstance(parsed_range, list) and len(parsed_range) == 2:
                            target_date_start = parsed_range[0]
                            target_date_end = parsed_range[1]
                    except:
                        continue
                
                if target_date_start and target_date_end:
                    # Try to get chunk date
                    chunk_date = chunk_data.get('meeting_date', '')
                    
                    if chunk_date:
                        # Use TemporalParser for robust date parsing
                        normalized_chunk_date = TemporalParser.normalize_date(chunk_date)
                        
                        if normalized_chunk_date:
                            if target_date_start <= normalized_chunk_date <= target_date_end:
                                score += 1.0 if is_temporal else 0.5  # Higher score for temporal queries
                    
                    # Also check entity dates
                    entities = chunk_data.get('entities', {})
                    entity_dates = entities.get('dates', []) + entities.get('meeting_metadata', [])
                    
                    for entity_date in entity_dates:
                        normalized_entity_date = TemporalParser.normalize_date(entity_date)
                        if normalized_entity_date and target_date_start <= normalized_entity_date <= target_date_end:
                            score += 0.7 if is_temporal else 0.4
                            break
            
            # Reduce recency boost for temporal queries (we care about the specific time period)
            if chunk_data.get('meeting_date') and not is_temporal:
                parsed_date = TemporalParser.parse_date(chunk_data['meeting_date'])
                if parsed_date:
                    days_ago = (datetime.now() - parsed_date).days
                    recency_score = max(0, 1 - (days_ago / 365))
                    score += recency_score * 0.1
            
            if score > 0:
                chunk_scores[chunk_id] = score
        
        return sorted(chunk_scores.items(), key=lambda x: x[1], reverse=True)
    
    def _calculate_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity."""
        return SequenceMatcher(None, s1, s2).ratio()
    
    def _extract_resolution_number(self, text: str) -> Optional[str]:
        """Extract core resolution/ordinance number from various formats."""
        if not text:
            return None
        
        # Patterns for different resolution/ordinance formats
        patterns = [
            r'(?:RESOLUTION NO\.|Res\.|Resolution|Resolution No\.|RES\.|ORDINANCE|Ord\.|ORD\.)\s*(\d{4}-\d+)',
            r'(\d{4}-\d+)',  # Just the number
            r'(?:RESOLUTION NO\.|Res\.|Resolution)\s*(\d{4}\s*-\s*\d+)',  # With spaces
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                # Normalize by removing spaces
                return match.group(1).replace(' ', '')
        
        return None
    
    def _fuse_and_rank(self, 
                       ner_results: List[Tuple[str, float]], 
                       structural_results: List[Tuple[str, float]], 
                       query_analysis: Dict) -> List[Dict[str, Any]]:
        """Fuse and rank results from both retrieval methods."""
        # Combine scores with weights
        combined_scores = defaultdict(float)
        
        # Weight based on query intent
        intent = query_analysis.get('intent', 'general_search')
        if intent == 'specific_lookup':
            ner_weight, structural_weight = 0.8, 0.2  # Increase NER weight for specific lookups
        elif intent == 'document_filter':
            ner_weight, structural_weight = 0.3, 0.7
        else:
            ner_weight, structural_weight = 0.5, 0.5
        
        # **NEW: Check if query asks for specific document type**
        requested_doc_types = set()
        entities = query_analysis.get('entities', {})
        structural_hints = query_analysis.get('structural_hints', {})
        
        # Extract requested document types from entities and hints
        if 'document_types' in entities:
            requested_doc_types.update(entities['document_types'])
        if 'document_type' in structural_hints:
            requested_doc_types.add(structural_hints['document_type'])
        
        # Add NER scores with boost for high-scoring exact matches
        for chunk_id, score in ner_results:
            # Special handling for specific resolution/ordinance lookups
            if intent == 'specific_lookup' and 'official_records' in query_analysis.get('entities', {}):
                # Check if this chunk contains the exact requested resolution
                requested_numbers = set()
                for entity in query_analysis['entities']['official_records']:
                    core_num = self._extract_resolution_number(entity)
                    if core_num:
                        requested_numbers.add(core_num)
                
                # Check if this chunk matches the exact requested resolution
                chunk_data = self.chunk_index.get(chunk_id, {})
                chunk_entities = chunk_data.get('entities', {})
                chunk_records = chunk_entities.get('official_records', [])
                
                is_exact_match = False
                for record in chunk_records:
                    record_num = self._extract_resolution_number(record)
                    if record_num and record_num in requested_numbers:
                        is_exact_match = True
                        break
                
                # Also check document name
                document_name = chunk_data.get('document', '')
                doc_num = self._extract_resolution_number(document_name)
                if doc_num and doc_num in requested_numbers:
                    is_exact_match = True
                
                if is_exact_match:
                    score *= 5.0  # Massive boost for exact resolution match
                elif score >= 1.0:
                    score *= 1.5  # Smaller boost for other matches
            elif intent == 'specific_lookup':
                if score >= 1.0:  # Exact or strong matches
                    score *= 2.0  # 100% boost for exact entity matches
                elif score >= 0.8:  # Good fuzzy matches
                    score *= 1.5  # 50% boost for good matches
            
            # **NEW: Apply document type boost for NER results**
            if requested_doc_types and chunk_id in self.chunk_index:
                chunk_data = self.chunk_index[chunk_id]
                chunk_doc_type = chunk_data.get('document_type', '').lower()
                
                # Check if chunk matches requested document type
                for req_type in requested_doc_types:
                    if req_type.lower() in chunk_doc_type or chunk_doc_type in req_type.lower():
                        score *= 3.0  # Significant boost for matching document type
                        break
            
            combined_scores[chunk_id] += score * ner_weight
        
        # Add structural scores with document type boost
        for chunk_id, score in structural_results:
            # **NEW: Apply document type boost for structural results**
            if requested_doc_types and chunk_id in self.chunk_index:
                chunk_data = self.chunk_index[chunk_id]
                chunk_doc_type = chunk_data.get('document_type', '').lower()
                
                # Check if chunk matches requested document type
                is_matching_type = False
                for req_type in requested_doc_types:
                    if req_type.lower() in chunk_doc_type or chunk_doc_type in req_type.lower():
                        score *= 2.5  # Strong boost for matching document type in structural results
                        is_matching_type = True
                        break
                
                # For temporal queries asking for specific document type, 
                # penalize non-matching types to prioritize requested type
                if intent == 'temporal_search' and not is_matching_type:
                    score *= 0.3  # Reduce score for non-matching document types
            
            combined_scores[chunk_id] += score * structural_weight
        
        # **NEW: Final document type priority boost**
        # For queries explicitly asking for a document type, give one more boost
        if requested_doc_types and (intent == 'temporal_search' or intent == 'document_filter'):
            for chunk_id in list(combined_scores.keys()):
                if chunk_id in self.chunk_index:
                    chunk_data = self.chunk_index[chunk_id]
                    chunk_doc_type = chunk_data.get('document_type', '').lower()
                    
                    # Apply final priority boost for exact document type match
                    for req_type in requested_doc_types:
                        if req_type.lower() == chunk_doc_type or chunk_doc_type == req_type.lower():
                            combined_scores[chunk_id] *= 1.5  # Final priority boost
                            break
        
        # Create ranked chunk list with metadata
        ranked_chunks = []
        for chunk_id, score in sorted(combined_scores.items(), key=lambda x: x[1], reverse=True):
            if chunk_id in self.chunk_index:
                chunk_data = self.chunk_index[chunk_id].copy()
                chunk_data['chunk_id'] = chunk_id
                chunk_data['relevance_score'] = score
                
                # Load actual chunk text
                chunk_text = self._load_chunk_text(chunk_id, chunk_data.get('source_file', ''))
                chunk_data['text'] = chunk_text
                
                ranked_chunks.append(chunk_data)
        
        return ranked_chunks
    
    def _load_chunk_text(self, chunk_id: str, source_file: str) -> str:
        """Load the actual text content of a chunk."""
        # Try to find chunk file
        chunk_files = list(self.chunks_dir.glob(f"{chunk_id}_*.txt"))
        
        if chunk_files:
            with open(chunk_files[0], 'r', encoding='utf-8') as f:
                content = f.read()
                
                # Extract text after header
                if "---" in content:
                    _, text = content.split("---", 1)
                    return text.strip()
                else:
                    return content
        
        return "Chunk text not found"
    
    async def _generate_response(self, 
                                query_text: str, 
                                ranked_chunks: List[Dict], 
                                query_analysis: Dict) -> Dict[str, Any]:
        """Generate final response from ranked chunks."""
        if not ranked_chunks:
            return {
                'answer': "No relevant information found for your query.",
                'sources': [],
                'query_analysis': query_analysis
            }
        
        # Prepare context from top chunks
        context_parts = []
        sources = []
        
        for i, chunk in enumerate(ranked_chunks[:5]):  # Use top 5 chunks
            doc_name = chunk.get('document', 'Unknown')
            doc_type = chunk.get('document_type', 'unknown')
            meeting_date = chunk.get('meeting_date', '')
            
            # Include document type and date in context for clarity
            context_header = f"[Source {i+1}: {doc_name} ({doc_type}"
            if meeting_date:
                context_header += f" - {meeting_date}"
            context_header += ")]"
            
            context_parts.append(f"{context_header}\n{chunk.get('text', '')}\n")
            
            # Enhanced source information with URLs
            source_info = {
                'chunk_id': chunk['chunk_id'],
                'document': doc_name,
                'document_type': doc_type,
                'meeting_date': meeting_date,
                'relevance_score': chunk.get('relevance_score', 0),
                'entities': chunk.get('entities', {}),
                'url': chunk.get('url', ''),  # Include URL if available
                'source_file': chunk.get('source_file', ''),
            }
            
            # Try to extract URL from chunk data if not directly available
            if not source_info['url']:
                # Check if URL is in the chunk's entities or metadata
                entities = chunk.get('entities', {})
                if 'urls' in entities and entities['urls']:
                    if isinstance(entities['urls'], list) and len(entities['urls']) > 0:
                        if isinstance(entities['urls'][0], dict):
                            source_info['url'] = entities['urls'][0].get('url', '')
                        else:
                            source_info['url'] = str(entities['urls'][0])
            
            sources.append(source_info)
        
        context = "\n---\n".join(context_parts)
        
        # Generate answer using LLM
        prompt = f"""Based on the following context, answer this query: {query_text}

Context:
{context}

Instructions:
1. Answer the question directly and comprehensively
2. ALWAYS reference specific sources when making claims using format [Source X]
3. Include ALL document types found: agendas, resolutions, ordinances, and verbatim transcripts
4. Group results by document type if multiple types are found
5. For each document mentioned, include its date and type
6. If the context doesn't contain enough information, say so
7. For temporal queries, organize results chronologically"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant answering questions about city government documents."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "32768"))
            )
            
            answer = response.choices[0].message.content.strip()
            
        except Exception as e:
            log.error(f"Response generation failed: {e}")
            answer = "I found relevant information but encountered an error generating the response."
        
        # Format sources with links for display
        formatted_sources = []
        for i, source in enumerate(sources):
            source_text = f"**Source {i+1}**: {source['document']} ({source['document_type']}"
            if source['meeting_date']:
                source_text += f" - {source['meeting_date']}"
            source_text += ")"
            
            if source['url']:
                source_text += f"\n📎 [View Document]({source['url']})"
            elif source['source_file']:
                source_text += f"\n📄 File: {source['source_file']}"
            
            source_text += f"\n⭐ Relevance: {source['relevance_score']:.2f}"
            formatted_sources.append(source_text)
        
        # Add source links to answer if not already included
        if formatted_sources and not any('[Source' in answer for answer in [answer]):
            answer += "\n\n**📚 Sources:**\n" + "\n\n".join(formatted_sources)
        
        return {
            'answer': answer,
            'sources': sources,
            'formatted_sources': formatted_sources,
            'chunks_retrieved': len(ranked_chunks),
            'query_analysis': query_analysis,
            'retrieval_method': 'ner_structural'
        }
    
    def _detect_comprehensive_intent(self, query_text: str) -> bool:
        """Detect if query asks for ALL/EVERY/COMPLETE results."""
        comprehensive_words = [
            'all', 'every', 'entire', 'complete', 'full', 'total',
            'everything', 'each', 'show me everything', 'list all',
            'give me all', 'find all', 'what are all', 'show all'
        ]
        query_lower = query_text.lower()
        return any(word in query_lower for word in comprehensive_words)
    
    async def _temporal_comprehensive_flow(self, query_text: str, query_analysis: Dict, comprehensive_mode: str = "limited") -> Dict[str, Any]:
        """Handle comprehensive temporal queries like 'all documents from Q1 2024'."""
        log.info(f"📅 Executing comprehensive temporal flow for: {query_text}")
        
        # Get date range from analysis
        date_range = query_analysis.get('structural_hints', {}).get('date_range')
        if not date_range:
            return await self._standard_flow(query_text, query_analysis, 10)
        
        # Convert to tuple for date-based matching
        date_range_tuple = tuple(date_range)
        
        # Get ALL chunks from the date range (date-based matching)
        chunks = await self._get_all_chunks_for_dates(date_range_tuple)
        
        if not chunks:
            return {
                'answer': f"No documents found in the specified time period ({date_range[0]} to {date_range[1]}).",
                'sources': [],
                'query_analysis': query_analysis,
                'retrieval_method': 'temporal_comprehensive'
            }
        
        log.info(f"📊 Found {len(chunks)} chunks from the date range {date_range[0]} to {date_range[1]}")
        
        # Create a mock graph context for compatibility with existing response generation
        graph_context = GraphContext()
        graph_context.query_type = "temporal"
        graph_context.date_range = date_range_tuple
        
        # Extract document count from chunks
        unique_docs = set()
        for chunk in chunks:
            doc = chunk.get('document', '')
            if doc:
                unique_docs.add(doc)
        graph_context.document_ids = unique_docs
        
        # Generate comprehensive temporal response
        return await self._generate_temporal_response(query_text, chunks, graph_context, comprehensive_mode)
    
    async def _meeting_comprehensive_flow(self, query_text: str, query_analysis: Dict, comprehensive_mode: str = "limited") -> Dict[str, Any]:
        """Handle comprehensive meeting queries like 'everything from 01/23/2024 meeting'."""
        log.info(f"🏛️ Executing comprehensive meeting flow for: {query_text}")
        
        # Extract meeting date
        dates = query_analysis.get('entities', {}).get('dates', [])
        if not dates:
            return await self._standard_flow(query_text, query_analysis, 10)
        
        meeting_date = dates[0]
        normalized_date = TemporalParser.normalize_date(meeting_date)
        
        if not normalized_date:
            return {
                'answer': f"Could not parse the meeting date: {meeting_date}.",
                'sources': [],
                'query_analysis': query_analysis,
                'retrieval_method': 'meeting_comprehensive'
            }
        
        # Use date-based matching for single meeting date
        date_range = (normalized_date, normalized_date)
        chunks = await self._get_all_chunks_for_dates(date_range)
        
        if not chunks:
            return {
                'answer': f"No documents found for the meeting on {meeting_date}.",
                'sources': [],
                'query_analysis': query_analysis,
                'retrieval_method': 'meeting_comprehensive'
            }
        
        log.info(f"📊 Found {len(chunks)} chunks for meeting on {meeting_date}")
        
        # Create meeting context for response generation
        meeting_context = GraphContext()
        meeting_context.query_type = "meeting"
        unique_docs = set()
        for chunk in chunks:
            doc = chunk.get('document', '')
            if doc:
                unique_docs.add(doc)
        meeting_context.document_ids = unique_docs
        
        # Generate meeting response
        return await self._generate_meeting_response(query_text, chunks, meeting_context, comprehensive_mode)
    
    async def _temporal_query_flow(self, query_text: str, query_analysis: Dict, top_k: int) -> Dict[str, Any]:
        """Handle regular temporal queries with graph-informed ranking."""
        log.info(f"📅 Executing temporal query flow for: {query_text}")
        
        # Get graph context for filtering
        date_range = query_analysis.get('structural_hints', {}).get('date_range')
        if date_range:
            graph_context = self._query_knowledge_graph_temporal(date_range)
            
            # Filter chunks based on graph context
            if graph_context.document_ids:
                filtered_chunks = await self._get_chunks_with_graph_filter(graph_context)
                
                # Rank filtered chunks
                ranked_chunks = self._rank_chunks_by_relevance(filtered_chunks, query_analysis)[:top_k]
                
                return await self._generate_response(query_text, ranked_chunks, query_analysis)
        
        # Fallback to standard flow
        return await self._standard_flow(query_text, query_analysis, top_k)
    
    async def _meeting_query_flow(self, query_text: str, query_analysis: Dict, top_k: int) -> Dict[str, Any]:
        """Handle regular meeting queries with graph-informed ranking."""
        log.info(f"🏛️ Executing meeting query flow for: {query_text}")
        
        dates = query_analysis.get('entities', {}).get('dates', [])
        if dates:
            meeting_date = dates[0]
            meeting_context = self._find_meeting_context(meeting_date)
            
            if meeting_context.document_ids:
                filtered_chunks = await self._get_chunks_with_graph_filter(meeting_context)
                ranked_chunks = self._rank_chunks_by_relevance(filtered_chunks, query_analysis)[:top_k]
                
                return await self._generate_response(query_text, ranked_chunks, query_analysis)
        
        # Fallback to standard flow
        return await self._standard_flow(query_text, query_analysis, top_k)
    
    async def _standard_flow(self, query_text: str, query_analysis: Dict, top_k: int) -> Dict[str, Any]:
        """Handle standard queries using original NER + structural approach."""
        log.info(f"🔍 Executing standard flow for: {query_text}")
        
        # Original dual retrieval
        ner_chunks = await self._ner_retrieval(query_analysis['entities'])
        structural_chunks = await self._structural_retrieval(query_analysis)
        
        # Fusion and ranking
        ranked_chunks = self._fuse_and_rank(ner_chunks, structural_chunks, query_analysis)[:top_k]
        
        # Generate response
        return await self._generate_response(query_text, ranked_chunks, query_analysis)
    
    def _query_knowledge_graph_temporal(self, date_range: List[str]) -> GraphContext:
        """Query knowledge graph for nodes within date range."""
        context = GraphContext()
        context.query_type = "temporal"
        context.date_range = tuple(date_range)
        
        if not self.graph:
            return context
        
        start_date, end_date = date_range
        
        for node_id, attrs in self.graph.nodes(data=True):
            meeting_date = attrs.get('meeting_date', '')
            if meeting_date:
                normalized = TemporalParser.normalize_date(meeting_date)
                if normalized and start_date <= normalized <= end_date:
                    node_info = {
                        'id': node_id,
                        'type': attrs.get('label', 'unknown'),
                        'attrs': attrs
                    }
                    context.temporal_nodes.append(node_info)
                    
                    # Extract document IDs
                    document = attrs.get('document', '')
                    source_id = attrs.get('source_id', '')
                    title = attrs.get('title', '')
                    
                    # Add various document identifiers
                    if document:
                        context.document_ids.add(document)
                    if source_id:
                        context.document_ids.add(source_id)
                    if title and 'agenda' in title.lower():
                        context.document_ids.add(title)
        
        log.info(f"📊 Graph temporal query found {len(context.temporal_nodes)} nodes, {len(context.document_ids)} documents")
        
        return context
    
    def _find_meeting_context(self, meeting_date: str) -> GraphContext:
        """Find all documents related to a specific meeting date."""
        context = GraphContext()
        context.query_type = "meeting"
        
        if not self.graph:
            return context
        
        normalized_date = TemporalParser.normalize_date(meeting_date)
        if not normalized_date:
            return context
        
        for node_id, attrs in self.graph.nodes(data=True):
            node_meeting_date = attrs.get('meeting_date', '')
            if node_meeting_date:
                node_normalized = TemporalParser.normalize_date(node_meeting_date)
                if node_normalized == normalized_date:
                    node_info = {
                        'id': node_id,
                        'type': attrs.get('label', 'unknown'),
                        'attrs': attrs
                    }
                    context.meeting_nodes.append(node_info)
                    
                    # Extract document IDs
                    document = attrs.get('document', '')
                    source_id = attrs.get('source_id', '')
                    title = attrs.get('title', '')
                    
                    if document:
                        context.document_ids.add(document)
                    if source_id:
                        context.document_ids.add(source_id)
                    if title:
                        context.document_ids.add(title)
        
        log.info(f"🏛️ Meeting context found {len(context.meeting_nodes)} nodes, {len(context.document_ids)} documents")
        
        return context
    
    async def _get_all_chunks_for_documents(self, document_ids: List[str]) -> List[Dict]:
        """Retrieve ALL chunks for specified documents without scoring limits."""
        all_chunks = []
        
        for chunk_id, chunk_data in self.chunk_index.items():
            # Check if chunk belongs to any of our target documents
            chunk_document = chunk_data.get('document', '')
            chunk_source = chunk_data.get('source_file', '')
            
            if (chunk_document in document_ids or 
                chunk_source in document_ids or
                any(doc_id in chunk_document for doc_id in document_ids) or
                any(doc_id in chunk_source for doc_id in document_ids)):
                
                chunk_data_copy = chunk_data.copy()
                chunk_data_copy['chunk_id'] = chunk_id
                chunk_data_copy['relevance_score'] = 1.0  # All equally relevant for comprehensive queries
                
                # Load chunk text
                chunk_text = self._load_chunk_text(chunk_id, chunk_data.get('source_file', ''))
                chunk_data_copy['text'] = chunk_text
                
                all_chunks.append(chunk_data_copy)
        
        # Sort by document and chunk index for coherent reading
        all_chunks.sort(key=lambda x: (x.get('document', ''), x.get('chunk_index', 0)))
        
        return all_chunks
    
    async def _get_all_chunks_for_dates(self, date_range: Tuple[str, str]) -> List[Dict]:
        """Retrieve ALL chunks for specified date range - date-based matching."""
        all_chunks = []
        start_date, end_date = date_range
        
        for chunk_id, chunk_data in self.chunk_index.items():
            chunk_meeting_date = chunk_data.get('meeting_date', '')
            if chunk_meeting_date:
                normalized_date = TemporalParser.normalize_date(chunk_meeting_date)
                if normalized_date and start_date <= normalized_date <= end_date:
                    chunk_data_copy = chunk_data.copy()
                    chunk_data_copy['chunk_id'] = chunk_id
                    chunk_data_copy['relevance_score'] = 1.0  # All equally relevant for comprehensive queries
                    
                    # Load chunk text
                    chunk_text = self._load_chunk_text(chunk_id, chunk_data.get('source_file', ''))
                    chunk_data_copy['text'] = chunk_text
                    
                    all_chunks.append(chunk_data_copy)
        
        # Sort by date and then by document and chunk index for coherent reading
        all_chunks.sort(key=lambda x: (
            TemporalParser.normalize_date(x.get('meeting_date', '')) or '1900-01-01',
            x.get('document', ''), 
            x.get('chunk_index', 0)
        ))
        
        return all_chunks
    
    async def _get_chunks_with_graph_filter(self, graph_context: GraphContext) -> List[Tuple[str, Dict]]:
        """Get chunks filtered by graph context with smart document matching."""
        filtered_chunks = []
        
        # If graph context has temporal filtering, use it for document type matching
        if graph_context.query_type == "temporal" and graph_context.date_range:
            start_date, end_date = graph_context.date_range
            
            for chunk_id, chunk_data in self.chunk_index.items():
                # Check temporal filter first
                chunk_meeting_date = chunk_data.get('meeting_date', '')
                if chunk_meeting_date:
                    normalized_date = TemporalParser.normalize_date(chunk_meeting_date)
                    if normalized_date and start_date <= normalized_date <= end_date:
                        # For temporal queries, include all matching chunks regardless of graph document IDs
                        filtered_chunks.append((chunk_id, chunk_data))
        
        else:
            # Original logic for non-temporal queries
            for chunk_id, chunk_data in self.chunk_index.items():
                # Check if chunk matches graph context
                if graph_context.document_ids:
                    chunk_document = chunk_data.get('document', '')
                    chunk_source = chunk_data.get('source_file', '')
                    
                    # Try exact match first
                    if (chunk_document in graph_context.document_ids or 
                        chunk_source in graph_context.document_ids):
                        filtered_chunks.append((chunk_id, chunk_data))
                        continue
                    
                    # Try smart matching for ordinances/resolutions
                    is_match = False
                    for doc_id in graph_context.document_ids:
                        if self._smart_document_match(chunk_data, doc_id):
                            is_match = True
                            break
                    
                    if is_match:
                        filtered_chunks.append((chunk_id, chunk_data))
        
        return filtered_chunks
    
    def _smart_document_match(self, chunk_data: Dict, graph_doc_id: str) -> bool:
        """Smart matching between chunk document and graph document ID."""
        chunk_document = chunk_data.get('document', '').lower()
        chunk_type = chunk_data.get('document_type', '').lower()
        graph_doc_lower = graph_doc_id.lower()
        
        # Match by document type
        if 'ordinance' in chunk_type and 'ordinance' in graph_doc_lower:
            return True
        if 'resolution' in chunk_type and 'resolution' in graph_doc_lower:
            return True
        if 'agenda' in chunk_type and ('agenda' in graph_doc_lower or 'item' in graph_doc_lower):
            return True
        
        # Extract resolution/ordinance numbers and compare
        chunk_number = self._extract_resolution_number(chunk_document)
        graph_number = self._extract_resolution_number(graph_doc_id)
        
        if chunk_number and graph_number and chunk_number == graph_number:
            return True
        
        # Fuzzy keyword matching
        chunk_keywords = set(chunk_document.split('_'))
        graph_keywords = set(graph_doc_lower.split())
        
        # Look for significant overlap in keywords
        overlap = chunk_keywords.intersection(graph_keywords)
        if len(overlap) >= 2:  # At least 2 matching keywords
            return True
        
        return False
    
    def _rank_chunks_by_relevance(self, filtered_chunks: List[Tuple[str, Dict]], query_analysis: Dict) -> List[Dict]:
        """Rank filtered chunks by relevance to query."""
        ranked_chunks = []
        
        for chunk_id, chunk_data in filtered_chunks:
            chunk_data_copy = chunk_data.copy()
            chunk_data_copy['chunk_id'] = chunk_id
            
            # Calculate relevance score based on entity matches
            score = 0.0
            entities = query_analysis.get('entities', {})
            chunk_entities = chunk_data.get('entities', {})
            
            # Score based on entity overlaps
            for entity_type, entity_list in entities.items():
                if entity_type in chunk_entities:
                    chunk_entity_list = chunk_entities[entity_type]
                    for entity in entity_list:
                        if entity in chunk_entity_list:
                            score += 1.0
                        else:
                            # Fuzzy matching
                            for chunk_entity in chunk_entity_list:
                                similarity = self._calculate_similarity(entity.lower(), chunk_entity.lower())
                                if similarity > 0.8:
                                    score += similarity * 0.8
            
            chunk_data_copy['relevance_score'] = score
            
            # Load chunk text
            chunk_text = self._load_chunk_text(chunk_id, chunk_data.get('source_file', ''))
            chunk_data_copy['text'] = chunk_text
            
            ranked_chunks.append(chunk_data_copy)
        
        # Sort by relevance score
        ranked_chunks.sort(key=lambda x: x.get('relevance_score', 0), reverse=True)
        
        return ranked_chunks
    
    async def _generate_temporal_response(self, query_text: str, chunks: List[Dict], graph_context: GraphContext, comprehensive_mode: str = "limited") -> Dict[str, Any]:
        """Generate response for comprehensive temporal queries with context limits."""
        if not chunks:
            return {
                'answer': f"No documents found in the specified time period.",
                'sources': [],
                'retrieval_method': 'temporal_comprehensive'
            }
        
        # Group chunks by document and date
        docs_by_date = defaultdict(list)
        for chunk in chunks:
            meeting_date = chunk.get('meeting_date', 'Unknown Date')
            docs_by_date[meeting_date].append(chunk)
        
        # Prepare context with smart limiting
        context_parts = []
        sources = []
        
        # Sort dates chronologically
        sorted_dates = sorted(docs_by_date.keys(), key=lambda x: TemporalParser.normalize_date(x) or '1900-01-01')
        
        source_counter = 1
        total_chunks_used = 0
        
        for date in sorted_dates:
            date_chunks = docs_by_date[date]
            docs_by_type = defaultdict(list)
            
            # Group by document type
            for chunk in date_chunks:
                doc_type = chunk.get('document_type', 'unknown')
                docs_by_type[doc_type].append(chunk)
            
            # Add date header
            context_parts.append(f"\n=== {date} ===")
            
            # Add documents by type
            for doc_type, type_chunks in docs_by_type.items():
                context_parts.append(f"\n--- {doc_type.title()} Documents ---")
                
                # Group by document name
                docs_by_name = defaultdict(list)
                for chunk in type_chunks:
                    doc_name = chunk.get('document', 'Unknown Document')
                    docs_by_name[doc_name].append(chunk)
                
                for doc_name, doc_chunks in docs_by_name.items():
                    if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                        context_parts.append(f"\n[Reached chunk limit. {len(chunks) - total_chunks_used} chunks omitted]")
                        break
                    
                    context_parts.append(f"\n[Source {source_counter}: {doc_name}]")
                    
                    # Apply smart chunk limiting based on comprehensive_mode
                    if comprehensive_mode == "limited":
                        # Limit chunks per document
                        limited_chunks = doc_chunks[:self.MAX_CHUNKS_PER_DOCUMENT]
                        
                        if total_chunks_used + len(limited_chunks) > self.MAX_TOTAL_CHUNKS:
                            remaining_slots = self.MAX_TOTAL_CHUNKS - total_chunks_used
                            limited_chunks = limited_chunks[:remaining_slots]
                        
                        for chunk in limited_chunks:
                            context_parts.append(chunk.get('text', ''))
                            total_chunks_used += 1
                    
                    elif comprehensive_mode == "summary":
                        # Use hierarchical summarization
                        if len(doc_chunks) > self.MAX_CHUNKS_PER_DOCUMENT:
                            summary = await self._generate_document_summary(doc_chunks)
                            context_parts.append(f"[Summary of {len(doc_chunks)} chunks]: {summary}")
                        else:
                            for chunk in doc_chunks:
                                context_parts.append(chunk.get('text', ''))
                                total_chunks_used += 1
                    
                    elif comprehensive_mode == "full":
                        # Original behavior - include all chunks
                        for chunk in doc_chunks:
                            context_parts.append(chunk.get('text', ''))
                            total_chunks_used += 1
                    
                    # Add to sources
                    sources.append({
                        'source_number': source_counter,
                        'document': doc_name,
                        'document_type': doc_type,
                        'meeting_date': date,
                        'chunks_count': len(doc_chunks),
                        'url': doc_chunks[0].get('url', '') if doc_chunks else '',
                        'source_file': doc_chunks[0].get('source_file', '') if doc_chunks else ''
                    })
                    
                    source_counter += 1
                    
                    if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                        break
            
            if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                break
        
        # Use progressive context building for limited mode
        if comprehensive_mode == "limited":
            context = await self._build_context_progressively(
                [{'text': part} for part in context_parts], 
                self.MAX_CONTEXT_TOKENS
            )
        else:
            context = "\n".join(context_parts)
        
        # Generate comprehensive answer
        prompt = f"""Based on the comprehensive temporal context below, answer this query: {query_text}

{context}

Instructions:
1. Provide a complete overview of ALL documents found in the time period
2. Organize results chronologically by date
3. Group documents by type (agendas, resolutions, ordinances, verbatim transcripts)
4. For each document, mention its date and key contents
5. Reference sources using [Source X] format
6. Include document counts and summary statistics
7. Be thorough and comprehensive - the user wants to see everything from this time period"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a comprehensive city government document assistant. Provide thorough, well-organized responses for temporal queries."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "32768"))  # From environment variable
            )
            
            answer = response.choices[0].message.content.strip()
            
        except Exception as e:
            log.error(f"Temporal response generation failed: {e}")
            answer = f"Found {len(chunks)} chunks from {len(graph_context.document_ids)} documents, but encountered an error generating the response."
        
        return {
            'answer': answer,
            'sources': sources,
            'chunks_retrieved': len(chunks),
            'documents_found': len(graph_context.document_ids),
            'time_period': f"{graph_context.date_range[0]} to {graph_context.date_range[1]}" if graph_context.date_range else "Unknown",
            'retrieval_method': 'temporal_comprehensive'
        }
    
    async def _generate_meeting_response(self, query_text: str, chunks: List[Dict], meeting_context: GraphContext, comprehensive_mode: str = "limited") -> Dict[str, Any]:
        """Generate response for comprehensive meeting queries with context limits."""
        if not chunks:
            return {
                'answer': f"No documents found for the specified meeting.",
                'sources': [],
                'retrieval_method': 'meeting_comprehensive'
            }
        
        # Group chunks by document type
        docs_by_type = defaultdict(list)
        for chunk in chunks:
            doc_type = chunk.get('document_type', 'unknown')
            docs_by_type[doc_type].append(chunk)
        
        # Prepare context organized by document type with smart limiting
        context_parts = []
        sources = []
        source_counter = 1
        total_chunks_used = 0
        
        # Process each document type
        for doc_type, type_chunks in docs_by_type.items():
            if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                context_parts.append(f"\n[Reached chunk limit. More document types omitted]")
                break
                
            context_parts.append(f"\n=== {doc_type.title()} Documents ===")
            
            # Group by document name
            docs_by_name = defaultdict(list)
            for chunk in type_chunks:
                doc_name = chunk.get('document', 'Unknown Document')
                docs_by_name[doc_name].append(chunk)
            
            for doc_name, doc_chunks in docs_by_name.items():
                if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                    context_parts.append(f"\n[Reached chunk limit. {len(chunks) - total_chunks_used} chunks omitted]")
                    break
                    
                context_parts.append(f"\n[Source {source_counter}: {doc_name}]")
                
                # Apply smart chunk limiting based on comprehensive_mode
                if comprehensive_mode == "limited":
                    # Limit chunks per document
                    limited_chunks = doc_chunks[:self.MAX_CHUNKS_PER_DOCUMENT]
                    
                    if total_chunks_used + len(limited_chunks) > self.MAX_TOTAL_CHUNKS:
                        remaining_slots = self.MAX_TOTAL_CHUNKS - total_chunks_used
                        limited_chunks = limited_chunks[:remaining_slots]
                    
                    for chunk in limited_chunks:
                        context_parts.append(chunk.get('text', ''))
                        total_chunks_used += 1
                
                elif comprehensive_mode == "summary":
                    # Use hierarchical summarization
                    if len(doc_chunks) > self.MAX_CHUNKS_PER_DOCUMENT:
                        summary = await self._generate_document_summary(doc_chunks)
                        context_parts.append(f"[Summary of {len(doc_chunks)} chunks]: {summary}")
                    else:
                        for chunk in doc_chunks:
                            context_parts.append(chunk.get('text', ''))
                            total_chunks_used += 1
                
                elif comprehensive_mode == "full":
                    # Original behavior - include all chunks
                    for chunk in doc_chunks:
                        context_parts.append(chunk.get('text', ''))
                        total_chunks_used += 1
                
                # Add to sources
                sources.append({
                    'source_number': source_counter,
                    'document': doc_name,
                    'document_type': doc_type,
                    'meeting_date': doc_chunks[0].get('meeting_date', '') if doc_chunks else '',
                    'chunks_count': len(doc_chunks),
                    'url': doc_chunks[0].get('url', '') if doc_chunks else '',
                    'source_file': doc_chunks[0].get('source_file', '') if doc_chunks else ''
                })
                
                source_counter += 1
                
                if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                    break
            
            if total_chunks_used >= self.MAX_TOTAL_CHUNKS and comprehensive_mode == "limited":
                break
        
        # Use progressive context building for limited mode
        if comprehensive_mode == "limited":
            context = await self._build_context_progressively(
                [{'text': part} for part in context_parts], 
                self.MAX_CONTEXT_TOKENS
            )
        else:
            context = "\n".join(context_parts)
        
        # Generate meeting response
        prompt = f"""Based on the complete meeting context below, answer this query: {query_text}

{context}

Instructions:
1. Provide a comprehensive overview of ALL documents from this meeting
2. Organize by document type (agenda, resolutions, ordinances, verbatim transcripts)
3. Include all agenda items, resolutions, and key discussion points
4. Reference sources using [Source X] format
5. Be thorough - the user wants to see everything from this specific meeting"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a comprehensive city government meeting assistant. Provide thorough meeting summaries."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "32768"))
            )
            
            answer = response.choices[0].message.content.strip()
            
        except Exception as e:
            log.error(f"Meeting response generation failed: {e}")
            answer = f"Found {len(chunks)} chunks from {len(meeting_context.document_ids)} meeting documents, but encountered an error generating the response."
        
        return {
            'answer': answer,
            'sources': sources,
            'chunks_retrieved': len(chunks),
            'documents_found': len(meeting_context.document_ids),
            'retrieval_method': 'meeting_comprehensive'
        }
    
    def _limit_chunks_by_relevance(self, chunks: List[Dict], query_analysis: Dict, max_chunks: int) -> List[Dict]:
        """Limit chunks while preserving most relevant content."""
        # Score chunks by relevance
        scored_chunks = []
        for chunk in chunks:
            score = self._calculate_chunk_relevance(chunk, query_analysis)
            scored_chunks.append((score, chunk))
        
        # Sort by relevance and limit
        scored_chunks.sort(key=lambda x: x[0], reverse=True)
        return [chunk for _, chunk in scored_chunks[:max_chunks]]
    
    def _calculate_chunk_relevance(self, chunk: Dict, query_analysis: Dict) -> float:
        """Calculate chunk relevance score for limiting purposes."""
        score = 0.0
        entities = query_analysis.get('entities', {})
        chunk_entities = chunk.get('entities', {})
        
        # Score based on entity overlaps
        for entity_type, entity_list in entities.items():
            if entity_type in chunk_entities:
                chunk_entity_list = chunk_entities[entity_type]
                for entity in entity_list:
                    if entity in chunk_entity_list:
                        score += 1.0
                    else:
                        # Fuzzy matching
                        for chunk_entity in chunk_entity_list:
                            similarity = self._calculate_similarity(entity.lower(), chunk_entity.lower())
                            if similarity > 0.8:
                                score += similarity * 0.8
        
        # Add recency bonus
        meeting_date = chunk.get('meeting_date', '')
        if meeting_date:
            from datetime import datetime
            parsed_date = TemporalParser.parse_date(meeting_date)
            if parsed_date:
                days_ago = (datetime.now() - parsed_date).days
                recency_score = max(0, 1 - (days_ago / 365))
                score += recency_score * 0.2
        
        return score
    
    async def _generate_document_summary(self, doc_chunks: List[Dict]) -> str:
        """Generate a summary of document chunks to reduce context size."""
        # Group chunks and summarize
        chunk_text = "\n".join([c.get('text', '')[:500] for c in doc_chunks[:3]])
        
        summary_prompt = f"Summarize the key points from this document:\n{chunk_text}"
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a document summarizer. Provide concise summaries highlighting key points."
                    },
                    {
                        "role": "user",
                        "content": summary_prompt
                    }
                ],
                temperature=0,
                max_tokens=1000
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            log.error(f"Summary generation failed: {e}")
            return f"Document with {len(doc_chunks)} chunks - summary unavailable"
    
    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (rough approximation)."""
        return len(text.split()) * 1.3  # Rough approximation: 1.3 tokens per word
    
    async def _build_context_progressively(self, chunks: List[Dict], token_limit: int) -> str:
        """Build context staying within token limits."""
        context_parts = []
        current_tokens = 0
        
        for chunk in chunks:
            chunk_text = chunk.get('text', '')
            chunk_tokens = self._estimate_tokens(chunk_text)
            
            if current_tokens + chunk_tokens > token_limit:
                # Add summary of remaining chunks
                remaining_summary = f"\n[... {len(chunks) - len(context_parts)} more chunks omitted ...]"
                context_parts.append(remaining_summary)
                break
                
            context_parts.append(chunk_text)
            current_tokens += chunk_tokens
        
        return "\n---\n".join(context_parts)

    async def _interpret_graph_results(self, graph_data: Dict[str, Any], user_query: str) -> str:
        """Uses an LLM to create a natural language summary of graph query results."""
        if not graph_data.get("results"):
            return "No specific relationships or structured data were found in the knowledge graph for this query."
        
        prompt = f"""Concisely summarize the key information from the following structured graph data in a few sentences. This data was retrieved to answer the user's query.

User Query: "{user_query}"

Graph Data (JSON):
{json.dumps(graph_data['results'][:20], indent=2, default=str)}

Summary:"""
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=250,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            log.error(f"Error interpreting graph results: {e}")
            return "Could not interpret the structured data from the graph."

    async def _generate_graph_only_response(self, user_query: str, graph_summary: str, graph_data: Dict[str, Any]) -> str:
        """Generates a response based only on knowledge graph results with intelligent chunking."""
        
        graph_results = graph_data.get('results', [])
        query_lower = user_query.lower()
        
        # Check if this is asking for ordinances/resolutions (comprehensive query)
        is_ordinance_query = 'ordinance' in query_lower
        is_resolution_query = 'resolution' in query_lower
        is_comprehensive = is_ordinance_query or is_resolution_query
        
        # FAST PATH: Direct extraction for ordinance/resolution lists
        if is_comprehensive and len(graph_results) > 20:
            log.info(f"🚀 Using fast direct extraction for {len(graph_results)} {('ordinances' if is_ordinance_query else 'resolutions')}")
            return self._extract_comprehensive_list(user_query, graph_results)
        
        # CHUNKING PATH: For large non-ordinance queries, use parallel processing
        if len(graph_results) > 50:
            log.info(f"✂️  Using chunked parallel processing for {len(graph_results)} results")
            return await self._process_large_results_parallel(user_query, graph_summary, graph_results)
        
        # STANDARD PATH: For smaller queries, use single LLM call
        graph_results = graph_results[:10] if len(graph_results) > 10 else graph_results
        max_results_text = f"Sample of {len(graph_results)} results"
        
        prompt = f"""You are a helpful assistant for the City Clerk's office. Your task is to provide a comprehensive answer to the user's query based solely on structured data from a knowledge graph.

USER QUESTION: "{user_query}"

### Summary from Knowledge Graph:
{graph_summary}

### Raw Graph Data ({max_results_text}):
{json.dumps(graph_results, indent=2, default=str)}

### INSTRUCTIONS:
- Provide a comprehensive answer based solely on the knowledge graph information.
- If the graph summary contains relevant information, use it to answer the query.
- If no information is available in the graph, state that clearly.
- Include document dates, numbers, and key details for each document mentioned.
- Focus on structured relationships and facts from the graph.

Final Answer:"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=2000,
        )
        return response.choices[0].message.content.strip()

    def _extract_comprehensive_list(self, user_query: str, graph_results: List[Dict]) -> str:
        """Fast direct extraction of comprehensive ordinance/resolution lists without LLM processing."""
        
        if not graph_results:
            return "No ordinances or resolutions were found for the specified criteria."
        
        # Extract and organize ordinances/resolutions
        documents = []
        
        for result in graph_results:
            # Extract document number and meeting date
            doc_number = result.get('document_number', ['Unknown'])[0] if isinstance(result.get('document_number'), list) else result.get('document_number', 'Unknown')
            meeting_date = result.get('meeting_date', ['Unknown'])[0] if isinstance(result.get('meeting_date'), list) else result.get('meeting_date', 'Unknown')
            doc_type = result.get('document_type', ['Unknown'])[0] if isinstance(result.get('document_type'), list) else result.get('document_type', 'Unknown')
            
            # Skip if no valid document number
            if doc_number == 'Unknown' or not doc_number:
                continue
                
            documents.append({
                'number': doc_number,
                'date': meeting_date,
                'type': doc_type,
                'year': doc_number.split('-')[0] if '-' in doc_number else 'Unknown'
            })
        
        # Sort by year and document number
        documents.sort(key=lambda x: (x['year'], x['number']))
        
        # Group by year
        by_year = {}
        for doc in documents:
            year = doc['year']
            if year not in by_year:
                by_year[year] = []
            by_year[year].append(doc)
        
        # Build response
        doc_type_name = 'ordinances' if 'ordinance' in user_query.lower() else 'resolutions' if 'resolution' in user_query.lower() else 'documents'
        
        response_parts = [f"Here is a comprehensive list of all {doc_type_name} since 2010, organized chronologically by year:"]
        
        # Check for data gaps
        query_start_year = 2010
        earliest_year = min(int(year) for year in by_year.keys() if year.isdigit()) if by_year else 2024
        
        if earliest_year > query_start_year:
            response_parts.append(f"\n**Data Coverage Note**: No {doc_type_name} found for {query_start_year}-{earliest_year-1}. The earliest {doc_type_name} in the database is from {earliest_year}.")
        
        # Add ordinances by year
        for year in sorted(by_year.keys()):
            year_docs = by_year[year]
            response_parts.append(f"\n## {year} ({len(year_docs)} {doc_type_name}):")
            
            for doc in year_docs:
                response_parts.append(f"- {doc['number']}, Meeting Date: {doc['date']}")
        
        # Add summary
        total_count = len(documents)
        years_covered = len([y for y in by_year.keys() if y.isdigit()])
        
        response_parts.append(f"\n**Summary**: Found {total_count} {doc_type_name} across {years_covered} years ({min(by_year.keys())}-{max(by_year.keys())}).")
        
        return "\n".join(response_parts)

    async def _process_large_results_parallel(self, user_query: str, graph_summary: str, graph_results: List[Dict]) -> str:
        """Process large result sets using parallel LLM calls and concatenation."""
        
        # Calculate optimal chunk size (aim for ~50-100 results per chunk)
        chunk_size = 75
        chunks = [graph_results[i:i + chunk_size] for i in range(0, len(graph_results), chunk_size)]
        
        log.info(f"📊 Processing {len(graph_results)} results in {len(chunks)} parallel chunks")
        
        # Process chunks in parallel
        chunk_tasks = []
        for i, chunk in enumerate(chunks):
            task = self._process_result_chunk(user_query, graph_summary, chunk, i+1, len(chunks))
            chunk_tasks.append(task)
        
        # Wait for all chunks to complete
        chunk_responses = await asyncio.gather(*chunk_tasks)
        
        # Concatenate responses
        return await self._concatenate_chunk_responses_parallel(user_query, chunk_responses)
    
    async def _process_result_chunk(self, user_query: str, graph_summary: str, chunk_results: List[Dict], chunk_num: int, total_chunks: int) -> str:
        """Process a single chunk of results."""
        
        prompt = f"""You are a helpful assistant for the City Clerk's office. Process this chunk of results from a knowledge graph query.

USER QUESTION: "{user_query}"

### Chunk {chunk_num} of {total_chunks} ({len(chunk_results)} results):
{json.dumps(chunk_results, indent=2, default=str)}

### INSTRUCTIONS:
- Extract key information from this chunk of results
- List all relevant documents with their numbers, dates, and key details
- Focus on factual information from the data
- This is part {chunk_num} of {total_chunks} - provide detailed content for this chunk only
- Organize chronologically where possible

Chunk {chunk_num} Results:"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=3000,
        )
        return response.choices[0].message.content.strip()
    
    async def _concatenate_chunk_responses_parallel(self, user_query: str, chunk_responses: List[str]) -> str:
        """Concatenate multiple chunk responses into a cohesive final answer."""
        
        # Combine all chunk responses
        combined_content = "\n\n---\n\n".join([f"**Part {i+1}:**\n{response}" for i, response in enumerate(chunk_responses)])
        
        prompt = f"""You are a helpful assistant for the City Clerk's office. Combine these multiple partial responses into one comprehensive, well-organized final answer.

USER QUESTION: "{user_query}"

### Multiple Response Parts to Combine:
{combined_content}

### INSTRUCTIONS:
- Combine all parts into one cohesive, comprehensive response
- Remove redundant information and organize chronologically
- Maintain all specific document numbers, dates, and details
- Create a flowing, natural response that reads as one unified answer
- Provide summary statistics if appropriate

Final Comprehensive Answer:"""
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=8000,
        )
        return response.choices[0].message.content.strip()

    async def _synthesize_final_answer(self, user_query: str, graph_summary: str, chunk_context: str) -> str:
        """Generates the final answer by combining the graph summary and text chunks."""
        prompt = f"""You are a helpful assistant for the City Clerk's office. Your task is to provide a comprehensive answer to the user's query by synthesizing information from two sources: a summary of structured data from a knowledge graph and excerpts from relevant documents.

USER QUESTION: "{user_query}"

### Summary from Knowledge Graph:
{graph_summary}

### Relevant Document Excerpts:
{chunk_context}

### INSTRUCTIONS:
- Combine the information from both sources into a single, cohesive answer.
- Prioritize the structured information from the graph summary for facts and relationships.
- Use the document excerpts to add detail, context, and direct quotes.
- If the sources conflict, note the discrepancy.
- If no information is available, state that clearly.
- For temporal queries asking for "all" or "every" document, be comprehensive and list ALL documents found.
- Include document dates, numbers, and key details for each document mentioned.

Final Answer:"""
        
        # Use higher token limit for comprehensive temporal queries
        query_lower = user_query.lower()
        if ('all' in query_lower or 'every' in query_lower) and ('ordinance' in query_lower or 'resolution' in query_lower):
            max_tokens = int(os.getenv("MAX_TOKENS", "32768"))  # Use full token limit for comprehensive queries
        else:
            max_tokens = 2000  # Increased from 1000 for better responses
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()

    async def graph_query(self, query_text: str) -> Dict[str, Any]:
        """
        Answers a query using the agent-based approach with Gremlin and Cosmos DB,
        fused with NER-based text chunk retrieval.
        """
        if not self.graph_query_agent:
            return {
                "answer": "Error: The Graph Query Agent is not configured. Please check your Cosmos DB credentials.",
                "retrieval_method": "graph_query_agent",
            }

        log.info(f"🧠 Executing advanced graph query: '{query_text}'")
        
        # 1. Analyze the query to get entities and intent
        analysis = await self._analyze_query(query_text)
        
        # 2. Get structured data from the knowledge graph via the agent
        graph_data = await self.graph_query_agent.generate_and_run(analysis)
        
        # 3. Use an LLM to interpret the structured graph results into a natural language summary
        graph_summary = await self._interpret_graph_results(graph_data, query_text)
        
        # TEMPORARILY COMMENTED OUT: NER-based chunk retrieval and synthesis
        # 4. Perform standard NER-based chunk retrieval to get textual context
        # ner_chunks = await self._ner_retrieval(analysis.get('entities', {}))
        # structural_chunks = await self._structural_retrieval(analysis)
        # ranked_chunks = self._fuse_and_rank(ner_chunks, structural_chunks, analysis)
        
        # # 5. Build context from text chunks - use more chunks for comprehensive queries
        # intent = analysis.get('intent', 'general_search')
        # query_lower = query_text.lower()
        
        # # Use more chunks for comprehensive queries asking for "all" ordinances/resolutions
        # if (intent == 'temporal_search' and 
        #     ('all' in query_lower or 'every' in query_lower) and 
        #     ('ordinance' in query_lower or 'resolution' in query_lower)):
        #     # Use up to 25 chunks for comprehensive temporal queries
        #     max_chunks = min(25, len(ranked_chunks))
        #     log.info(f"🔍 Using {max_chunks} chunks for comprehensive temporal query")
        # else:
        #     max_chunks = 5  # Default limit
        
        # # 6. Build context from the selected chunks
        # chunk_context = "\n---\n".join(
        #     f"[Source: {chunk.get('source_file', 'Unknown')}]\n{chunk.get('text', '')}"
        #     for chunk in ranked_chunks[:max_chunks]
        # )
        
        # # 7. Synthesize the final answer using the graph summary and text context
        # final_answer = await self._synthesize_final_answer(query_text, graph_summary, chunk_context)

        # TEMPORARY: Return only knowledge graph results passed to LLM
        final_answer = await self._generate_graph_only_response(query_text, graph_summary, graph_data)

        return {
            "answer": final_answer,
            "source_graph_data": graph_data,
            # "source_chunks": ranked_chunks[:max_chunks],  # COMMENTED OUT
            "query_analysis": analysis,
            "retrieval_method": "graph_query_only"  # Changed to indicate graph-only mode
        } 