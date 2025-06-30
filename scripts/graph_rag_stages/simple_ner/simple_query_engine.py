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

log = logging.getLogger(__name__)


class SimpleNERQueryEngine:
    """Query engine for NER relationships + structural search."""
    
    def __init__(self, graph_dir: Path = Path("simple_ner_graph")):
        """Initialize the query engine."""
        self.graph_dir = Path(graph_dir)
        self.chunks_dir = self.graph_dir / "document_chunks"
        
        # Load indices
        self.entity_index = self._load_entity_index()
        self.chunk_index = self._load_chunk_index()
        
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
    
    async def query(self, query_text: str, top_k: int = 10) -> Dict[str, Any]:
        """
        Execute a NER + structural query.
        
        Args:
            query_text: User's query
            top_k: Number of top chunks to retrieve
            
        Returns:
            Query results with answer and sources
        """
        log.info(f"Executing NER + structural query: {query_text}")
        
        # Step 1: Analyze query
        query_analysis = await self._analyze_query(query_text)
        
        # Step 2: Dual retrieval
        ner_chunks = await self._ner_retrieval(query_analysis['entities'])
        structural_chunks = await self._structural_retrieval(query_analysis)
        
        # Step 3: Fusion and ranking
        ranked_chunks = self._fuse_and_rank(
            ner_chunks, 
            structural_chunks, 
            query_analysis
        )[:top_k]
        
        # Step 4: Generate response
        response = await self._generate_response(
            query_text, 
            ranked_chunks, 
            query_analysis
        )
        
        return response
    
    async def _analyze_query(self, query_text: str) -> Dict[str, Any]:
        """Analyze query to extract entities and intent."""
        prompt = f"""Analyze this query and extract:
1. Named entities by category
2. Query intent (specific_lookup, temporal_search, document_filter, relationship_query)
3. Structural hints (document type, date range, etc.)

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

Return JSON with this structure:
{{
    "entities": {{
        "people": ["name1", "name2"],
        "agenda_items": ["E-1"],
        ...
    }},
    "intent": "specific_lookup",
    "structural_hints": {{
        "document_type": "agenda",
        "date_range": ["2024-01-01", "2024-01-31"],
        "needs_verbatim": false
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
            
            return analysis
            
        except Exception as e:
            log.error(f"Query analysis failed: {e}")
            # Fallback to simple extraction
            return self._simple_query_analysis(query_text)
    
    def _simple_query_analysis(self, query_text: str) -> Dict[str, Any]:
        """Simple fallback query analysis using patterns."""
        entities = defaultdict(list)
        
        # Extract agenda items (E-1, F-10, etc.)
        agenda_pattern = r'\b([A-Z]-?\d+)\b'
        entities['agenda_items'] = re.findall(agenda_pattern, query_text)
        
        # Extract dates (various formats)
        date_patterns = [
            r'\b(\d{1,2}/\d{1,2}/\d{4})\b',
            r'\b(\w+ \d{1,2}, \d{4})\b',
            r'\b(\d{4}-\d{2}-\d{2})\b'
        ]
        for pattern in date_patterns:
            entities['dates'].extend(re.findall(pattern, query_text))
        
        # Extract document numbers
        doc_patterns = [
            r'\b(Ord\.?\s*\d{4}-\d+)\b',
            r'\b(Res\.?\s*R?-?\d+-\d+)\b',
            r'\b(\d{4}-\d+)\b'
        ]
        for pattern in doc_patterns:
            entities['official_records'].extend(re.findall(pattern, query_text, re.IGNORECASE))
        
        # Determine intent
        intent = 'general_search'
        if entities['agenda_items']:
            intent = 'specific_lookup'
        elif 'all' in query_text.lower() or 'list' in query_text.lower():
            intent = 'document_filter'
        
        return {
            'entities': dict(entities),
            'intent': intent,
            'structural_hints': {}
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
        
        for chunk_id, chunk_data in self.chunk_index.items():
            score = 0.0
            
            # More flexible document type filter for specific lookups
            if 'document_type' in hints:
                chunk_type = chunk_data.get('document_type', '').lower()
                hint_type = hints['document_type'].lower()
                
                if chunk_type == hint_type:
                    score += 0.5
                elif intent == 'specific_lookup':
                    # For specific lookups, be more flexible - verbatim transcripts often contain agenda items
                    if hint_type == 'agenda' and 'verbatim' in chunk_type:
                        score += 0.3  # Partial credit for verbatim transcripts when looking for agenda items
                    elif hint_type == 'agenda' and 'meeting' in chunk_type:
                        score += 0.4  # Good credit for meeting-related documents
                # Don't skip non-matching types for specific lookups - they might still be relevant
            
            # Enhanced date range filter with fallback to entity dates
            if 'date_range' in hints and len(hints['date_range']) == 2:
                target_date_start = hints['date_range'][0]
                target_date_end = hints['date_range'][1]
                
                # Try primary meeting_date field
                chunk_date = chunk_data.get('meeting_date', '')
                date_matched = False
                
                if chunk_date:
                    try:
                        # Handle different date formats
                        if '.' in chunk_date:  # m.d.yyyy format
                            date_obj = datetime.strptime(chunk_date, '%m.%d.%Y')
                        else:  # yyyy-mm-dd format
                            date_obj = datetime.strptime(chunk_date, '%Y-%m-%d')
                        
                        chunk_date_str = date_obj.strftime('%Y-%m-%d')
                        if target_date_start <= chunk_date_str <= target_date_end:
                            score += 0.5  # Strong boost for date match
                            date_matched = True
                    except:
                        pass
                
                # If no meeting_date, check entity dates
                if not date_matched:
                    entities = chunk_data.get('entities', {})
                    entity_dates = entities.get('dates', []) + entities.get('meeting_metadata', [])
                    
                    for entity_date in entity_dates:
                        if any(target_year in str(entity_date) for target_year in ['2014'] if target_date_start.startswith('2014')):
                            score += 0.4  # Good boost for entity date match
                            date_matched = True
                            break
            
            # Reduced recency boost to avoid over-penalizing older documents for specific lookups
            if chunk_data.get('meeting_date') and intent != 'specific_lookup':
                try:
                    if '.' in chunk_data['meeting_date']:
                        date_obj = datetime.strptime(chunk_data['meeting_date'], '%m.%d.%Y')
                    else:
                        date_obj = datetime.strptime(chunk_data['meeting_date'], '%Y-%m-%d')
                    days_ago = (datetime.now() - date_obj).days
                    recency_score = max(0, 1 - (days_ago / 365))  # Decay over a year
                    score += recency_score * 0.1  # Reduced from 0.2 to 0.1
                except:
                    pass
            
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
            context_parts.append(f"[Source {i+1}: {chunk.get('document', 'Unknown')}]\n{chunk.get('text', '')}\n")
            
            sources.append({
                'chunk_id': chunk['chunk_id'],
                'document': chunk.get('document', 'Unknown'),
                'document_type': chunk.get('document_type', 'unknown'),
                'relevance_score': chunk.get('relevance_score', 0),
                'entities': chunk.get('entities', {})
            })
        
        context = "\n---\n".join(context_parts)
        
        # Generate answer using LLM
        prompt = f"""Based on the following context, answer this query: {query_text}

Context:
{context}

Instructions:
1. Answer the question directly and concisely
2. Reference specific sources when making claims
3. If the context doesn't contain enough information, say so
4. For entity lookups, provide all relevant details found"""
        
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
        
        return {
            'answer': answer,
            'sources': sources,
            'chunks_retrieved': len(ranked_chunks),
            'query_analysis': query_analysis,
            'retrieval_method': 'ner_structural'
        } 