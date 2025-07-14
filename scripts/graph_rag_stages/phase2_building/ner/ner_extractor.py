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
        self.model = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME", "gpt-4o")
        
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
        """Check if this chunk has already been processed by looking for existing entity files."""
        # Check if any entity files exist for this chunk
        for category in self.ENTITY_CATEGORIES:
            category_dir = self.output_dir / category
            expected_filename = f"{chunk_id}_{doc_name}.txt"
            entity_file = category_dir / expected_filename
            
            # If we find any entity file for this chunk, consider it processed
            if entity_file.exists():
                return True
        
        return False 
    
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
                
                # Save entity files
                entity_count = await self._save_entity_files(chunk_id, doc_name, entities)
                
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
        categories_json = json.dumps(self.ENTITY_CATEGORIES, indent=2)
        
        prompt = f"""Extract all named entities from the following text and categorize them according to these categories:

{categories_json}

Important instructions:
1. Extract EXACT entity names as they appear in the text
2. Include ALL instances of each entity type
3. For people, include full names with titles (e.g., "Commissioner John Smith")
4. For dates, use the format found in the text
5. For dollar amounts, include the currency symbol
6. Return ONLY a JSON object with category names as keys and lists of entities as values"""
        
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
    
    async def _save_entity_files(self, chunk_id: str, doc_name: str, entities: Dict[str, List[str]]) -> int:
        """Save entity files for each category that has entities."""
        total_entities = 0
        
        for category, entity_list in entities.items():
            if entity_list:  # Only create file if there are entities
                filename = f"{chunk_id}_{doc_name}.txt"
                filepath = self.output_dir / category / filename
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    # Write header
                    f.write(f"# Entities: {category}\n")
                    f.write(f"# Chunk: {chunk_id}\n")
                    f.write(f"# Document: {doc_name}\n")
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
    
    def test_relationship_extraction(self):
        sample_text = "John Smith works at City Hall."
        entities = asyncio.run(self._extract_entities_llm(sample_text))
        assert 'relationships' in entities
        assert any(['John Smith', 'works at', 'City Hall'] in rel for rel in entities.get('relationships', []))