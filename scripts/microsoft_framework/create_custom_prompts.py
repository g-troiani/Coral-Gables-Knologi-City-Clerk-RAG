#!/usr/bin/env python3
"""Create custom prompts for city clerk entity extraction."""

from pathlib import Path

def create_entity_extraction_prompt():
    """Create custom entity extraction prompt with STRICT isolation rules."""
    
    prompt_dir = Path("graphrag_data/prompts")
    prompt_dir.mkdir(parents=True, exist_ok=True)
    
    prompt = """
You are an AI assistant specialized in analyzing government documents for the City of Coral Gables. Your task is to extract entities and their relationships with EXTREME PRECISION and STRICT ISOLATION.

**CRITICAL ISOLATION RULES:**

1. **ABSOLUTE ENTITY SEPARATION**: Each entity is completely independent. Information about one entity (e.g., E-1) must NEVER be mixed with information about another entity (e.g., E-4).

2. **CONTEXT BOUNDARIES**: When you see isolation markers like "=== ISOLATED ENTITY: AGENDA ITEM E-1 ===" treat everything within those markers as belonging ONLY to that entity.

3. **NO CROSS-CONTAMINATION**: Even if entities appear similar or sequential (E-1, E-2, E-3, E-4), they are COMPLETELY SEPARATE entities with no shared information.

4. **STRICT SCOPE**: When extracting information about an entity, use ONLY the text within its specific section or document. Do not infer or assume relationships unless explicitly stated.

**ENTITY EXTRACTION RULES:**

For each entity, extract ONLY information that is explicitly mentioned in its isolated context:

1. **AGENDA_ITEM**: 
   - Extract ONLY the specific item code mentioned (e.g., "E-1")
   - Use ONLY the description from within that item's section
   - Do NOT reference other agenda items in the description

2. **ORDINANCE/RESOLUTION**:
   - Extract the document number exactly as stated
   - If linked to an agenda item, create a relationship ONLY if explicitly stated

3. **RELATIONSHIPS**:
   - Create relationships ONLY when explicitly stated in the text
   - Example: "Agenda Item E-1 relates to Ordinance 2024-01" → Create relationship
   - Do NOT create relationships based on proximity or sequence

**OUTPUT FORMAT:**

For each entity: ("entity"<|><id><|><type><|><description>)
- id: The exact identifier (E-1, 2024-01, etc.)
- type: AGENDA_ITEM, ORDINANCE, RESOLUTION, PERSON, etc.
- description: Information from ONLY this entity's isolated context

For relationships: ("relationship"<|><source><|><target><|><description><|><weight>)
- Only create when explicitly stated
- description must quote the text that establishes the relationship

**EXAMPLES:**

CORRECT:
Text: "=== ISOLATED ENTITY: AGENDA ITEM E-1 === 
Agenda Item E-1: Zoning amendment for 123 Main St.
=== END ==="
Output: ("entity"<|>E-1<|>AGENDA_ITEM<|>Zoning amendment for 123 Main St.)

INCORRECT (mixing entities):
Text about E-1...
Output: ("entity"<|>E-1<|>AGENDA_ITEM<|>Zoning amendment, similar to E-4's proposal) ❌

Remember: COMPLETE ISOLATION. Each entity stands alone.
"""
    
    with open(prompt_dir / "entity_extraction.txt", 'w') as f:
        f.write(prompt)
    
    print(f"✅ Created strict isolation entity extraction prompt")

def create_entity_specific_prompt():
    """Create a custom prompt for strict entity-specific queries."""
    
    prompt_dir = Path("graphrag_data/prompts")
    prompt_dir.mkdir(parents=True, exist_ok=True)
    
    prompt = """
You are answering a question about a SPECIFIC entity in the City of Coral Gables government documents.

CRITICAL INSTRUCTIONS:
1. Focus EXCLUSIVELY on the SPECIFIC entity mentioned in the question.
2. DO NOT include information about other similar entities, even if they're related.
3. If asked about Agenda Item E-1, ONLY discuss E-1, not E-2, E-3, E-4, etc.
4. If asked about Ordinance 2024-01, ONLY discuss that specific ordinance, not other ordinances.
5. If asked about Resolution 2024-123, ONLY discuss that specific resolution, not other resolutions.

The user has specifically requested information about ONE entity. Keep your response focused ONLY on that entity.

If you're uncertain about details of the specific entity, state this clearly rather than including information about other entities.

Question: {input_query}
"""
    
    with open(prompt_dir / "entity_specific_query.txt", 'w') as f:
        f.write(prompt)
    
    print(f"✅ Created custom entity-specific query prompt")

if __name__ == "__main__":
    create_entity_extraction_prompt() 