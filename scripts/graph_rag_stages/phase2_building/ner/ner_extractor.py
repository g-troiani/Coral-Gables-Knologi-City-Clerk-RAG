"""
Named Entity Recognition extractor using LLM.
Extracts 15 categories of entities from document chunks.
"""

import json
import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
from openai import AzureOpenAI
import os

log = logging.getLogger(__name__)


class NERExtractor:
    """Extracts named entities from chunks using LLM."""
    
    # Entity categories as specified
    ENTITY_CATEGORIES = {
        'people': 'Full names & honorifics (Commissioners, Mayor, City Officials, citizens, stakeholders, speakers)',
        'organizations': 'Department names, agencies, businesses, non-profits, associations',
        'official_records': 'Ordinance/resolution numbers (e.g., "Ord. 2024-07", "Res. R-23-123")',
        'agenda_items': 'Agenda numbers, item codes (original and current)',
        'meeting_metadata': 'Meeting dates, start times, locations, meeting types',
        'document_titles': 'Ordinance titles, resolution titles, report titles, proclamation titles',
        'document_types': 'Document type classification (ordinance, resolution, agenda, transcript, verbatim, minutes)',
        'dates': 'Adoption dates, effective dates, transcript timestamps, deadlines',
        'dollar_amounts': 'Any monetary value plus currency (budgets, contract amounts, fines)',
        'addresses': 'Street addresses, ZIP codes, parcel IDs',
        'named_locations': 'Buildings, districts, venues, areas (e.g., "City Hall", "Coral Gables District 3")',
        'contracts': 'Contract/Proclamation numbers - unique identifiers for administrative documents',
        'document_references': 'Cross-referenced ordinances, resolutions, attachments',
        'actions': 'Verbs indicating legislative or procedural action (approve, deny, adopt, defer, amend, etc.)',
        'events': 'Ceremonies, public hearings, workshops (with event dates)',
        'products_technologies': 'Named products, software, or equipment referenced',
        'relationships': 'Triples like (entity1, relation, entity2) with context - directional relations between extracted entities',
        'outcomes': 'Vote outcomes for agenda items with status, vote counts, and details'
    }
    
    def __init__(self, output_dir: Path):
        """Initialize the NER extractor."""
        self.output_dir = Path(output_dir)
        self.chunks_dir = self.output_dir / "document_chunks"
        self.extract_relationships = os.getenv("EXTRACT_RELATIONSHIPS", "false").lower() == "true"
        
        # Create entity category directories
        for category in self.ENTITY_CATEGORIES:
            (self.output_dir / category).mkdir(parents=True, exist_ok=True)
        
        # Initialize Azure OpenAI client
        self.client = AzureOpenAI(
            api_key=os.getenv("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT")
        )
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
        if not self.model:
            raise ValueError("AZURE_OPENAI_DEPLOYMENT_NAME environment variable must be set")
        
        # Rate limiting
        self.max_concurrent = 5
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
    
    async def process_all_chunks(self) -> int:
        """Process all chunks in the chunks directory."""
        chunk_files = list(self.chunks_dir.glob("*.txt"))
        log.info(f"Found {len(chunk_files)} chunks to process for NER")
        
        # Process in batches to avoid overwhelming the API
        batch_size = 10
        total_entities = 0
        
        for i in range(0, len(chunk_files), batch_size):
            batch = chunk_files[i:i + batch_size]
            tasks = [self._process_chunk(chunk_file) for chunk_file in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    log.error(f"Error processing chunk: {result}")
                else:
                    total_entities += result
            
            # Small delay between batches
            if i + batch_size < len(chunk_files):
                await asyncio.sleep(1)
        
                return total_entities
    
    def _is_chunk_already_processed(self, chunk_id: str, doc_name: str) -> bool:
        """Check if this chunk has already been processed."""
        return False  # Force re-processing 
    
    async def _process_chunk(self, chunk_file: Path) -> int:
        """Process a single chunk file."""
        async with self.semaphore:
            try:
                # Get chunk ID and document name from filename
                filename_parts = chunk_file.stem.split("_", 1)
                chunk_id = filename_parts[0]
                doc_name = filename_parts[1] if len(filename_parts) > 1 else "unknown"
                
                # CHECK IF ALREADY PROCESSED - SKIP IF ENTITY FILES EXIST
                if self._is_chunk_already_processed(chunk_id, doc_name):
                    log.debug(f"Chunk {chunk_id} already processed, skipping")
                    return 0
                
                # Read chunk content AND metadata
                chunk_metadata = self._read_chunk_metadata(chunk_file)
                
                # Read chunk content
                with open(chunk_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Extract chunk text (skip metadata header)
                if "---" in content:
                    _, chunk_text = content.split("---", 1)
                    chunk_text = chunk_text.strip()
                else:
                    chunk_text = content
                
                # Extract entities using LLM
                entities = await self._extract_entities_llm(chunk_text)
                
                # Save entity files with metadata
                entity_count = await self._save_entity_files(chunk_id, doc_name, entities, chunk_metadata)
                
                # Log what we extracted
                log.info(f"Chunk {chunk_id} extracted:")
                for category, items in entities.items():
                    if items:
                        log.info(f"  - {category}: {len(items)} entities")
                
                log.debug(f"Extracted {entity_count} entities from {chunk_file.name}")
                return entity_count
                
            except Exception as e:
                log.error(f"Failed to process chunk {chunk_file.name}: {e}")
                return 0
    
    async def _extract_entities_llm(self, chunk_text: str) -> Dict[str, List[str]]:
        """Extract entities using LLM."""
        prompt = self._build_extraction_prompt(chunk_text)
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at extracting named entities from city government documents. Return only valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            entities = self._parse_entity_response(result_text)
            return entities
            
        except Exception as e:
            log.error(f"LLM extraction failed: {e}")
            return {}
    
    def _build_extraction_prompt(self, chunk_text: str) -> str:
        """Build the entity extraction prompt."""
        # Detect document type from chunk metadata
        if "ordinance" in chunk_text.lower():
            return self._build_ordinance_prompt(chunk_text)
        elif "resolution" in chunk_text.lower():
            return self._build_resolution_prompt(chunk_text)
        elif "verbatim" in chunk_text.lower():
            return self._build_transcript_prompt(chunk_text)
        else:
            return self._build_generic_prompt(chunk_text)

    def _build_ordinance_prompt(self, chunk_text: str) -> str:
        return f"""Extract entities from this ORDINANCE document.
        
        Focus on:
        - Zoning codes and districts
        - Property addresses
        - Setback measurements
        - Building heights
        - Legal descriptions
        - Section numbers being amended
        - Effective dates
        
        Categories to extract:
        {json.dumps(self.ENTITY_CATEGORIES, indent=2)}
        
        {chunk_text}
        
        Return ONLY valid JSON with exhaustive entity extraction.
        """

    def _build_resolution_prompt(self, chunk_text: str) -> str:
        return f"""Extract entities from this RESOLUTION document.
        
        Focus on:
        - Resolution numbers
        - Dollar amounts and budgets
        - Contract parties
        - Approval dates
        - Department references
        - Commissioner names and votes
        
        Categories to extract:
        {json.dumps(self.ENTITY_CATEGORIES, indent=2)}
        
        {chunk_text}
        
        Return ONLY valid JSON with exhaustive entity extraction.
        """

    def _build_transcript_prompt(self, chunk_text: str) -> str:
        return f"""Extract entities from this VERBATIM TRANSCRIPT document.
        
        Focus on:
        - Speaker names and titles
        - Agenda item references
        - Vote outcomes
        - Public comments
        - Procedural motions
        - Time stamps
        
        Categories to extract:
        {json.dumps(self.ENTITY_CATEGORIES, indent=2)}
        
        {chunk_text}
        
        Return ONLY valid JSON with exhaustive entity extraction.
        """

    def _build_generic_prompt(self, chunk_text: str) -> str:
        """Build the generic entity extraction prompt."""
        
        prompt = f"""You are analyzing a City of Coral Gables government document. Extract ALL named entities exhaustively.

IMPORTANT: Be extremely thorough - extract EVERY entity, even if mentioned multiple times.

Categories to extract:
{json.dumps(self.ENTITY_CATEGORIES, indent=2)}

Additional extraction rules: 
These extractions will be used to query the resulting knowledge graph. A rich extraction effort is needed. 
1. Extract ALL names, even partial references (e.g., "Commissioner Smith" AND "Smith")
2. Extract ALL monetary amounts, even estimates or ranges
3. Extract ALL addresses, including partial addresses and intersections
4. Extract ALL dates, including relative dates like "next month"
5. Extract ALL organization names, including departments and committees
6. Extract ALL document references, including internal references. 
7. For relationships, extract ALL connections between entities. This is one of the most important extractions effort

For this document type, also look for:
- City commission members and their titles
- Department heads and staff
- Citizen speakers and their affiliations
- Business names and addresses
- Contract amounts and terms
- Ordinance and resolution numbers
- Specific zoning codes and regulations
- Property addresses and parcel numbers

Text to analyze:
{chunk_text}

Return ONLY valid JSON with exhaustive entity extraction."""
        
        if self.extract_relationships:
            prompt += """
7. Also extract RELATIONSHIPS as triples: [["entity1", "relation verb/phrase", "entity2"], ...]. Relations should be directional and contextual (e.g., ["John Smith", "works at", "City Hall"]). Link only entities from the same chunk.
8. For each agenda_item, extract a relationship: [["item_code", "has_outcome", "unique_outcome_id"]] where unique_outcome_id is like "outcome_itemcode_meetingdate".
9. For each outcome, create a separate entity object: {"id": "unique_outcome_id", "type": "vote_outcome", "status": "passed/failed/tabled/deferred", "yes_votes": number, "no_votes": number, "details": "brief vote summary"}. Use chain-of-thought: Identify items → Look for "passed/failed/tabled" phrases → Count yes/no if available.
"""
        
        prompt += f"""

Text to analyze:
{chunk_text[:3000]}  # Limit to avoid token limits

Return format example:
{{
    "people": ["Commissioner Jane Doe", "Mayor John Smith"],
    "organizations": ["Planning Department", "ABC Corporation"],
    "official_records": ["Ord. 2024-01", "Res. R-23-456"],
    "agenda_items": ["E-1", "F-10"],
    "meeting_metadata": ["January 9, 2024", "5:30 PM", "City Commission Chambers"],
    "document_titles": ["An Ordinance Relating to Parking"],
    "dates": ["January 9, 2024", "February 1, 2024"],
    "dollar_amounts": ["$150,000", "$2.5 million"],
    "addresses": ["405 Biltmore Way", "33134"],
    "named_locations": ["City Hall", "Miracle Mile"],
    "contracts": ["Contract No. 2024-15"],
    "document_references": ["Ordinance 2023-45", "Resolution R-22-123"],
    "actions": ["approved", "deferred", "amended"],
    "events": ["Public Hearing on January 23, 2024"],
    "products_technologies": ["Microsoft Teams", "Granicus"]"""
        
        if self.extract_relationships:
            prompt += """,
    "relationships": [["Commissioner Jane Doe", "works for", "Planning Department"], ["Ord. 2024-01", "references", "Ordinance 2023-45"]],
    "outcomes": [{"id": "outcome_E-1_2024-01-09", "type": "vote_outcome", "status": "passed", "yes_votes": 5, "no_votes": 2, "details": "Motion passed with Commissioner X abstaining"}]"""
        
        prompt += """
}}"""
        
        return prompt
    
    def _parse_entity_response(self, response_text: str) -> Dict[str, List[str]]:
        """Parse the LLM response to extract entities."""
        # Clean up response
        if '```json' in response_text:
            response_text = response_text.split('```json')[1].split('```')[0].strip()
        elif '```' in response_text:
            parts = response_text.split('```')
            if len(parts) >= 3:
                response_text = parts[1].strip()
        
        try:
            entities = json.loads(response_text)
            
            # Ensure all categories exist and values are lists
            cleaned_entities = {}
            for category in self.ENTITY_CATEGORIES:
                if category in entities and isinstance(entities[category], list):
                    # Special handling for complex types that contain lists or dicts
                    if category in ['relationships', 'outcomes']:
                        # For relationships (list of lists) and outcomes (list of dicts)
                        seen = set()
                        unique = []
                        for item in entities[category]:
                            # Serialize to JSON string for deduplication
                            item_key = json.dumps(item, sort_keys=True)
                            if item_key not in seen:
                                seen.add(item_key)
                                unique.append(item)
                        cleaned_entities[category] = unique
                    else:
                        # For simple string entities, original logic works
                        seen = set()
                        unique = []
                        for entity in entities[category]:
                            if entity and entity not in seen:
                                seen.add(entity)
                                unique.append(entity)
                        cleaned_entities[category] = unique
                else:
                    cleaned_entities[category] = []
            
            return cleaned_entities
            
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse entity JSON: {e}")
            log.debug(f"Response was: {response_text[:500]}")
            return {category: [] for category in self.ENTITY_CATEGORIES}
    
    async def _save_entity_files(self, chunk_id: str, doc_name: str, entities: Dict[str, List[str]], chunk_metadata: Dict = None) -> int:
        """Save entity files for each category that has entities."""
        total_entities = 0
        
        # Get source file info from chunk metadata if available
        source_file_name = chunk_metadata.get('Source_File_Name', doc_name) if chunk_metadata else doc_name
        source_file_path = chunk_metadata.get('Source_File_Path', f"unknown/{doc_name}") if chunk_metadata else f"unknown/{doc_name}"
        
        for category, entity_list in entities.items():
            if entity_list:  # Only create file if there are entities
                filename = f"{chunk_id}_{doc_name}.txt"
                filepath = self.output_dir / category / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    # Write header with consistent metadata
                    f.write(f"# Entities: {category}\n")
                    f.write(f"# Chunk: {chunk_id}\n")
                    f.write(f"# Document: {doc_name}\n")
                    f.write(f"# Source_File_Name: {source_file_name}\n")
                    f.write(f"# Source_File_Path: {source_file_path}\n")
                    f.write(f"# Count: {len(entity_list)}\n")
                    f.write("\n---\n\n")
                    
                    if category == 'relationships':
                        # Save as JSON triple per line (Fix for Issue 1)
                        f.write(f"# Format: JSON triple per line\n")
                        for triple in entity_list:
                            f.write(json.dumps(triple) + "\n")
                    elif category == 'outcomes':
                        # Save outcomes as JSON entities per line
                        f.write(f"# Format: JSON entity per line\n")
                        for outcome in entity_list:
                            f.write(json.dumps(outcome) + "\n")
                    else:
                        # Existing: one per line
                        for entity in entity_list:
                            f.write(f"{entity}\n")
                
                total_entities += len(entity_list)
        
        return total_entities
    
    def _read_chunk_metadata(self, chunk_file: Path) -> Dict[str, Any]:
        """Extract metadata from chunk file header."""
        metadata = {}
        
        with open(chunk_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if "---" in content:
            header, _ = content.split("---", 1)
            
            for line in header.strip().split("\n"):
                if line.startswith("#") and ":" in line:
                    key_value = line[1:].strip().split(":", 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip()
                        metadata[key] = value
        
        return metadata
    
    def test_relationship_extraction(self):
        sample_text = "John Smith works at City Hall."
        entities = asyncio.run(self._extract_entities_llm(sample_text))
        assert 'relationships' in entities
        assert any(['John Smith', 'works at', 'City Hall'] in rel for rel in entities.get('relationships', []))