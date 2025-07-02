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
from openai import AzureOpenAI
from difflib import SequenceMatcher
import os
import networkx as nx
from dataclasses import dataclass, field
from scripts.graph_rag_stages.common.temporal_utils import TemporalParser, TemporalIndex

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
    
    async def query(self, query_text: str, top_k: int = 10) -> Dict[str, Any]:
        """
        Execute a hybrid NER + knowledge graph query with smart routing.
        
        Args:
            query_text: User's query
            top_k: Number of top chunks to retrieve (ignored for comprehensive queries)
            
        Returns:
            Query results with answer and sources
        """
        log.info(f"Executing hybrid NER + graph query: {query_text}")
        
        # Step 1: Analyze query with enhanced intent classification
        query_analysis = await self._analyze_query(query_text)
        
        # Step 2: Detect comprehensive intent
        is_comprehensive = self._detect_comprehensive_intent(query_text)
        intent = query_analysis.get('intent', 'general_search')
        
        # Step 3: Route based on query type and intent
        if is_comprehensive and intent == 'temporal_search':
            return await self._temporal_comprehensive_flow(query_text, query_analysis)
        elif is_comprehensive and 'meeting' in query_text.lower():
            return await self._meeting_comprehensive_flow(query_text, query_analysis)
        elif intent == 'temporal_search':
            return await self._temporal_query_flow(query_text, query_analysis, top_k)
        elif 'meeting' in query_text.lower() and query_analysis.get('entities', {}).get('dates'):
            return await self._meeting_query_flow(query_text, query_analysis, top_k)
        else:
            return await self._standard_flow(query_text, query_analysis, top_k)
    
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

Return JSON with this structure:
{{
    "entities": {{
        "people": ["name1", "name2"],
        "agenda_items": ["E-1"],
        ...
    }},
    "intent": "temporal_search",  // Use this if query is primarily about time/dates
    "structural_hints": {{
        "document_type": "agenda",
        "date_range": {json.dumps(date_range) if date_range else 'null'},
        "needs_verbatim": false,
        "is_temporal_query": true  // Set to true for time-based queries
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
                max_tokens=1024
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
            
            combined_scores[chunk_id] += score * ner_weight
        
        # Add structural scores
        for chunk_id, score in structural_results:
            combined_scores[chunk_id] += score * structural_weight
        
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
                max_tokens=1024
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
    
    async def _temporal_comprehensive_flow(self, query_text: str, query_analysis: Dict) -> Dict[str, Any]:
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
        return await self._generate_temporal_response(query_text, chunks, graph_context)
    
    async def _meeting_comprehensive_flow(self, query_text: str, query_analysis: Dict) -> Dict[str, Any]:
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
        return await self._generate_meeting_response(query_text, chunks, meeting_context)
    
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
        """Get chunks filtered by graph context."""
        filtered_chunks = []
        
        for chunk_id, chunk_data in self.chunk_index.items():
            # Check if chunk matches graph context
            if graph_context.document_ids:
                chunk_document = chunk_data.get('document', '')
                chunk_source = chunk_data.get('source_file', '')
                
                if (chunk_document in graph_context.document_ids or 
                    chunk_source in graph_context.document_ids or
                    any(doc_id in chunk_document for doc_id in graph_context.document_ids)):
                    
                    filtered_chunks.append((chunk_id, chunk_data))
        
        return filtered_chunks
    
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
    
    async def _generate_temporal_response(self, query_text: str, chunks: List[Dict], graph_context: GraphContext) -> Dict[str, Any]:
        """Generate response for comprehensive temporal queries."""
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
        
        # Prepare comprehensive context
        context_parts = []
        sources = []
        
        # Sort dates chronologically
        sorted_dates = sorted(docs_by_date.keys(), key=lambda x: TemporalParser.normalize_date(x) or '1900-01-01')
        
        source_counter = 1
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
                    context_parts.append(f"\n[Source {source_counter}: {doc_name}]")
                    
                    # Add first few chunks from this document
                    for chunk in doc_chunks[:3]:  # Limit to avoid overwhelming context
                        context_parts.append(chunk.get('text', ''))
                    
                    if len(doc_chunks) > 3:
                        context_parts.append(f"[... {len(doc_chunks) - 3} more chunks from this document ...]")
                    
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
                max_tokens=2000  # Increased for comprehensive responses
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
    
    async def _generate_meeting_response(self, query_text: str, chunks: List[Dict], meeting_context: GraphContext) -> Dict[str, Any]:
        """Generate response for comprehensive meeting queries."""
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
        
        # Prepare context organized by document type
        context_parts = []
        sources = []
        source_counter = 1
        
        # Process each document type
        for doc_type, type_chunks in docs_by_type.items():
            context_parts.append(f"\n=== {doc_type.title()} Documents ===")
            
            # Group by document name
            docs_by_name = defaultdict(list)
            for chunk in type_chunks:
                doc_name = chunk.get('document', 'Unknown Document')
                docs_by_name[doc_name].append(chunk)
            
            for doc_name, doc_chunks in docs_by_name.items():
                context_parts.append(f"\n[Source {source_counter}: {doc_name}]")
                
                # Add all chunks from this document (for meeting queries, include everything)
                for chunk in doc_chunks:
                    context_parts.append(chunk.get('text', ''))
                
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
                max_tokens=2000
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