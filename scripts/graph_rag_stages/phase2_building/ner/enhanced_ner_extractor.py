"""
Enhanced NER extractor with multi-pass extraction
"""

from typing import Dict, List, Any
import asyncio
import logging
import os
from pathlib import Path
from .ner_extractor import NERExtractor

# Add debug logging at module level
log = logging.getLogger(__name__)

class EnhancedNERExtractor(NERExtractor):
    """Enhanced NER extractor with multi-pass extraction strategy."""
    
    def __init__(self, output_dir, seed_entities=None):
        super().__init__(output_dir)
        self.seed_entities = seed_entities or []
        log.info(f"🔧 DEBUG: EnhancedNERExtractor initialized with {len(self.seed_entities)} seed entities")
    
    async def _extract_entities_llm(self, chunk_text: str, chunk_metadata: Dict) -> Dict[str, Any]:
        """Extract entities using multiple targeted passes."""
        
        log.info(f"🔧 DEBUG: Starting multi-pass extraction for chunk (length: {len(chunk_text)})")
        log.info(f"🔧 DEBUG: Chunk metadata: {chunk_metadata}")
        
        # Pass 1: High-level entities
        log.info("🔧 DEBUG: Starting Pass 1 - Primary entities")
        entities_pass1 = await self._extract_primary_entities(chunk_text, chunk_metadata)
        log.info(f"🔧 DEBUG: Pass 1 completed. Entities: {len(entities_pass1)} types found")
        log.info(f"🔧 DEBUG: Pass 1 entity types: {list(entities_pass1.keys())}")
        
        # Pass 2: Relationships and secondary entities
        log.info("🔧 DEBUG: Starting Pass 2 - Relationships")
        relationships = await self._extract_relationships(chunk_text, entities_pass1, chunk_metadata)
        log.info(f"🔧 DEBUG: Pass 2 completed. Relationships: {len(relationships)}")
        
        # Pass 3: Missed entities based on relationship targets
        log.info("🔧 DEBUG: Starting Pass 3 - Missed entities")
        entities_pass2 = await self._extract_missed_entities(chunk_text, entities_pass1, relationships)
        log.info(f"🔧 DEBUG: Pass 3 completed. Additional entities: {len(entities_pass2)} types found")
        
        # Pass 4: Attributes and metadata
        log.info("🔧 DEBUG: Starting Pass 4 - Entity enhancement")
        enhanced_entities = await self._enhance_entity_attributes(chunk_text, entities_pass1, entities_pass2)
        log.info(f"🔧 DEBUG: Pass 4 completed. Enhanced entities: {len(enhanced_entities)} types")
        
        # Merge results
        log.info("🔧 DEBUG: Merging extraction results")
        final_result = self._merge_extraction_results(enhanced_entities, relationships)
        log.info(f"🔧 DEBUG: Final result - Entities: {len(final_result.get('entities', {}))}, Relationships: {len(final_result.get('relationships', []))}")
        
        return final_result
    
    async def _extract_primary_entities(self, chunk_text: str, metadata: Dict) -> Dict[str, List]:
        """First pass: Extract main entities."""
        
        # Use the parent's comprehensive prompt that lists ALL entity types
        prompt = self._build_extraction_prompt(chunk_text, metadata)
        
        messages = [
            {"role": "system", "content": "You are an expert at extracting structured entities from city government documents based on a formal ontology. Return only valid JSON."},
            {"role": "user", "content": prompt}
        ]
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=0,
            max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
        )
        
        response_text = response.choices[0].message.content.strip()
        result = self._parse_entity_response(response_text)
        
        return result
    
    async def _extract_relationships(self, chunk_text: str, entities: Dict, metadata: Dict) -> List[Dict]:
        """Second pass: Extract relationships between entities."""
        
        log.info(f"🔧 DEBUG: Starting relationship extraction with {len(entities)} entity types")
        
        # Create entity reference list for context
        entity_refs = []
        for entity_type, entity_list in entities.items():
            for entity in entity_list:
                entity_refs.append(f"{entity.get('name', '')} ({entity_type})")
        
        log.info(f"🔧 DEBUG: Built entity references: {len(entity_refs)} total")
        
        prompt = f"""Given these entities found in the text:
{', '.join(entity_refs[:20])}  # First 20 for context

Find ALL relationships between them. Also identify any MISSING entities that are targets of relationships.

Look for action words that indicate relationships:
- moved, seconded, voted, approved, denied
- located at, owned by, managed by
- reports to, works for, represents

Text: {chunk_text[:1500]}

Return JSON with relationships array."""

        log.info(f"🔧 DEBUG: Built relationship prompt (length: {len(prompt)})")
        log.info("🔧 DEBUG: Calling LLM for relationships")
        
        result = await self._call_llm_for_relationships(prompt)
        log.info(f"🔧 DEBUG: LLM call completed for relationships. Result: {result}")
        
        return result
    
    async def _extract_missed_entities(self, chunk_text: str, primary_entities: Dict, relationships: List) -> Dict[str, List]:
        """Third pass: Find entities mentioned in relationships but not in primary extraction."""
        
        log.info(f"🔧 DEBUG: Starting missed entity extraction")
        log.info(f"🔧 DEBUG: Primary entities: {len(primary_entities)} types, Relationships: {len(relationships)}")
        
        # Extract target entities from relationships that might be missing
        potential_missing = set()
        for rel in relationships:
            if 'target' in rel:
                potential_missing.add(rel['target'])
            if 'object' in rel:
                potential_missing.add(rel['object'])
        
        log.info(f"🔧 DEBUG: Found {len(potential_missing)} potential missing entities")
        
        if not potential_missing:
            log.info("🔧 DEBUG: No potential missing entities found, returning empty")
            return {}
        
        prompt = f"""Look for these specific entities that were mentioned in relationships but might have been missed:
{', '.join(potential_missing)}

Search the text carefully for ANY mention of these entities, even if they're not explicitly defined.

Text: {chunk_text}

Return JSON with entities array for any found."""

        log.info(f"🔧 DEBUG: Built missed entities prompt (length: {len(prompt)})")
        log.info("🔧 DEBUG: Calling LLM for missed entities")
        
        result = await self._call_llm_for_entities(prompt)
        log.info(f"🔧 DEBUG: LLM call completed for missed entities. Result: {result}")
        
        return result
    
    async def _enhance_entity_attributes(self, chunk_text: str, primary_entities: Dict, secondary_entities: Dict) -> Dict[str, List]:
        """Fourth pass: Enhance entity attributes and metadata."""
        
        log.info(f"🔧 DEBUG: Starting entity enhancement")
        log.info(f"🔧 DEBUG: Primary: {len(primary_entities)} types, Secondary: {len(secondary_entities)} types")
        
        # Merge primary and secondary entities
        all_entities = self._merge_entity_dicts(primary_entities, secondary_entities)
        log.info(f"🔧 DEBUG: Merged entities: {len(all_entities)} types")
        
        enhanced = {}
        for entity_type, entity_list in all_entities.items():
            enhanced[entity_type] = []
            log.info(f"🔧 DEBUG: Enhancing {len(entity_list)} entities of type {entity_type}")
            
            for entity in entity_list:
                try:
                    prompt = f"""Enhance this {entity_type} entity with additional attributes from the context:

Entity: {entity}

Context: {chunk_text[:800]}

Add any missing attributes like titles, roles, addresses, dates, etc. Return enhanced JSON."""

                    log.info(f"🔧 DEBUG: Enhancing entity: {entity.get('name', 'Unknown')}")
                    enhanced_entity = await self._call_llm_for_entity_enhancement(prompt, entity.copy())
                    enhanced[entity_type].append(enhanced_entity)
                    log.info(f"🔧 DEBUG: Enhanced entity result: {enhanced_entity}")
                    
                except Exception as e:
                    log.error(f"🔧 DEBUG: Error enhancing entity {entity}: {e}")
                    enhanced[entity_type].append(entity)  # Keep original if enhancement fails
        
        log.info(f"🔧 DEBUG: Entity enhancement completed for {len(enhanced)} types")
        return enhanced
    
    def _merge_extraction_results(self, enhanced_entities: Dict, relationships: List) -> Dict[str, Any]:
        """Merge all extraction results into final format."""
        
        return {
            "entities": enhanced_entities,
            "relationships": relationships,
            "extraction_method": "multi_pass",
            "passes_completed": 4
        }
    
    def _merge_entity_dicts(self, dict1: Dict, dict2: Dict) -> Dict[str, List]:
        """Merge two entity dictionaries."""
        merged = dict1.copy()
        
        for entity_type, entities in dict2.items():
            if entity_type in merged:
                merged[entity_type].extend(entities)
            else:
                merged[entity_type] = entities
        
        return merged
    
    async def _call_llm_for_entities(self, prompt: str) -> Dict[str, List]:
        """Call LLM for entity extraction."""
        import os
        
        log.info(f"🔧 DEBUG: Making LLM call for entities")
        log.info(f"🔧 DEBUG: Prompt preview: {prompt[:200]}...")
        log.info(f"🔧 DEBUG: Using model: {self.model}")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at extracting structured entities from city government documents. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            response_text = response.choices[0].message.content.strip()
            log.info(f"🔧 DEBUG: LLM response received (length: {len(response_text)})")
            log.info(f"🔧 DEBUG: Raw LLM response: {response_text[:500]}...")
            
            # Parse response into entities format
            result = self._parse_entity_response(response_text)
            log.info(f"🔧 DEBUG: Parsed entities result: {result}")
            log.info(f"🔧 DEBUG: Parsed entity types: {list(result.keys())}")
            
            return result
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error in LLM call for entities: {e}")
            return {}
    
    async def _call_llm_for_relationships(self, prompt: str) -> List[Dict]:
        """Call LLM for relationship extraction."""
        import os
        
        log.info(f"🔧 DEBUG: Making LLM call for relationships")
        log.info(f"🔧 DEBUG: Prompt preview: {prompt[:200]}...")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at extracting relationships from city government documents. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            response_text = response.choices[0].message.content.strip()
            log.info(f"🔧 DEBUG: LLM response for relationships received (length: {len(response_text)})")
            log.info(f"🔧 DEBUG: Raw relationship response: {response_text[:500]}...")
            
            # Parse response into relationships format
            result = self._parse_relationship_response(response_text)
            log.info(f"🔧 DEBUG: Parsed relationships result: {result}")
            
            return result
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error in LLM call for relationships: {e}")
            return []
    
    async def _call_llm_for_entity_enhancement(self, prompt: str, entity: Dict) -> Dict:
        """Call LLM for entity attribute enhancement."""
        import os
        
        log.info(f"🔧 DEBUG: Making LLM call for entity enhancement")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert at enhancing entity attributes from city government documents. Return only valid JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0,
                max_tokens=int(os.getenv("MAX_TOKENS", "16384"))
            )
            response_text = response.choices[0].message.content.strip()
            log.info(f"🔧 DEBUG: LLM response for enhancement received (length: {len(response_text)})")
            log.info(f"🔧 DEBUG: Raw enhancement response: {response_text[:300]}...")
            
            # Parse and merge with existing entity
            enhanced_attrs = self._parse_enhancement_response(response_text)
            log.info(f"🔧 DEBUG: Parsed enhancement attributes: {enhanced_attrs}")
            
            entity.update(enhanced_attrs)
            return entity
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error in LLM call for enhancement: {e}")
            return entity
    
    def _parse_entity_response(self, response: str) -> Dict[str, List]:
        """Parse LLM response into entities format."""
        log.info(f"🔧 DEBUG: Parsing entity response (length: {len(response)})")
        log.info(f"🔧 DEBUG: Response preview: {response[:300]}...")
        
        try:
            # Use parent class parsing method
            parsed_result = self._parse_extraction_response(response)
            log.info(f"🔧 DEBUG: Parent parsing result: {parsed_result}")
            log.info(f"🔧 DEBUG: Parent parsing type: {type(parsed_result)}")
            
            # Map LLM response format to expected format
            entity_mapping = {
                'Persons': 'Person',
                'Organizations': 'Organization', 
                'Locations': 'Location',
                'Assets': 'Asset',
                'Policies': 'Policy',
                'Documents': 'Document',
                'LegalReferences': 'LegalReference',
                'Events': 'Event',
                'Actions': 'Action',
                'Projects': 'Project',
                'Roles': 'Role',
                'Topics': 'Topic',
                'AgendaItems': 'AgendaItem',
                'Sections': 'Section',
                'Contracts': 'Contract',
                'Technologies': 'Technology',
                'VoteOutcomes': 'VoteOutcome'
            }
            
            entities = {}
            
            # First try to get from standard 'entities' key
            standard_entities = parsed_result.get("entities", {})
            if isinstance(standard_entities, dict) and any(standard_entities.values()):
                log.info(f"🔧 DEBUG: Found entities in standard format: {standard_entities}")
                entities = standard_entities
            else:
                # Map from LLM's plural format to singular format
                log.info(f"🔧 DEBUG: Mapping from LLM plural format to singular")
                for llm_key, standard_key in entity_mapping.items():
                    if llm_key in parsed_result:
                        entities[standard_key] = parsed_result[llm_key]
                        log.info(f"🔧 DEBUG: Mapped {llm_key} -> {standard_key}: {len(parsed_result[llm_key])} entities")
                
                # Also check for any other entity types directly
                for key, value in parsed_result.items():
                    if key not in entity_mapping and key not in ['entities', 'relationships'] and isinstance(value, list):
                        # Try to map unknown plural to singular
                        singular_key = key.rstrip('s') if key.endswith('s') else key
                        entities[singular_key] = value
                        log.info(f"🔧 DEBUG: Found additional entity type {key} -> {singular_key}: {len(value)} entities")
            
            log.info(f"🔧 DEBUG: Final mapped entities: {entities}")
            log.info(f"🔧 DEBUG: Entity types found: {list(entities.keys())}")
            log.info(f"🔧 DEBUG: Total entities: {sum(len(elist) for elist in entities.values())}")
            
            return entities
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error parsing entity response: {e}")
            log.error(f"🔧 DEBUG: Response that failed: {response}")
            return {}
    
    def _parse_relationship_response(self, response: str) -> List[Dict]:
        """Parse LLM response into relationships format."""
        log.info(f"🔧 DEBUG: Parsing relationship response (length: {len(response)})")
        log.info(f"🔧 DEBUG: Response preview: {response[:300]}...")
        
        try:
            # Use parent class parsing method
            parsed = self._parse_extraction_response(response)
            log.info(f"🔧 DEBUG: Parent parsing result for relationships: {parsed}")
            
            relationships = parsed.get("relationships", [])
            log.info(f"🔧 DEBUG: Extracted relationships: {relationships}")
            log.info(f"🔧 DEBUG: Number of relationships: {len(relationships)}")
            
            return relationships
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error parsing relationship response: {e}")
            log.error(f"🔧 DEBUG: Response that failed: {response}")
            return []
    
    def _parse_enhancement_response(self, response: str) -> Dict:
        """Parse LLM response for entity enhancement."""
        log.info(f"🔧 DEBUG: Parsing enhancement response (length: {len(response)})")
        log.info(f"🔧 DEBUG: Response preview: {response[:200]}...")
        
        try:
            # Reuse the parent's parsing logic which handles markdown
            parsed = self._parse_extraction_response(response)
            log.info(f"🔧 DEBUG: Parent parsing result for enhancement: {parsed}")
            
            # Return just the entity attributes (not wrapped in entities/relationships)
            if isinstance(parsed, dict):
                # If it has 'entities' key, extract the first entity
                if 'entities' in parsed:
                    for entity_list in parsed['entities'].values():
                        if entity_list:
                            result = entity_list[0]
                            log.info(f"🔧 DEBUG: Extracted first entity for enhancement: {result}")
                            return result
                # Otherwise return as-is
                log.info(f"🔧 DEBUG: Returning parsed result as-is: {parsed}")
                return parsed
            return {}
            
        except Exception as e:
            log.error(f"🔧 DEBUG: Error parsing enhancement response: {e}")
            log.error(f"🔧 DEBUG: Response that failed: {response}")
            return {}
    
    def _merge_extraction_results(self, entities: Dict, relationships: List) -> Dict[str, Any]:
        """Merge all extraction results into final format."""
        log.info(f"🔧 DEBUG: Merging extraction results")
        log.info(f"🔧 DEBUG: Input entities: {entities}")
        log.info(f"🔧 DEBUG: Input relationships: {relationships}")
        
        result = {
            "entities": entities,
            "relationships": relationships
        }
        
        log.info(f"🔧 DEBUG: Final merged result: {result}")
        return result
    
    def _merge_entity_dicts(self, dict1: Dict, dict2: Dict) -> Dict[str, List]:
        """Merge two entity dictionaries."""
        log.info(f"🔧 DEBUG: Merging entity dictionaries")
        log.info(f"🔧 DEBUG: Dict1: {dict1}")
        log.info(f"🔧 DEBUG: Dict2: {dict2}")
        
        merged = dict1.copy()
        for entity_type, entities in dict2.items():
            if entity_type in merged:
                merged[entity_type].extend(entities)
                log.info(f"🔧 DEBUG: Extended {entity_type}: {len(merged[entity_type])} total entities")
            else:
                merged[entity_type] = entities
                log.info(f"🔧 DEBUG: Added new type {entity_type}: {len(entities)} entities")
        
        log.info(f"🔧 DEBUG: Final merged dictionary: {merged}")
        return merged 