#!/usr/bin/env python3
"""Create custom prompts for city clerk entity extraction."""

from pathlib import Path

def create_entity_extraction_prompt():
    """Create custom entity extraction prompt."""
    
    prompt_dir = Path("graphrag_data/prompts")
    prompt_dir.mkdir(parents=True, exist_ok=True)
    
    prompt = """
You are extracting entities from City of Coral Gables government documents.

CRITICAL: Look for these specific entity types:

1. AGENDA_ITEM: Codes like E-1, F-10, H-3, 2-1
   - Pattern: Letter-Number or Number-Number
   - Context: "Item E-1", "Agenda Item F-10", "relating to E-2"

2. ORDINANCE: Document numbers like 2024-01, 2024-15
   - Pattern: Year-Number
   - Context: "Ordinance 2024-01", "Ordinance No. 2024-15"

3. RESOLUTION: Document numbers like 2024-123, 2024-45
   - Pattern: Year-Number (usually 3 digits)
   - Context: "Resolution 2024-123", "Resolution No. 2024-45"

4. PERSON: Names of officials and citizens
5. ORGANIZATION: Departments, companies, agencies
6. MEETING: Meeting dates and types
7. MONEY: Dollar amounts, budgets
8. PROJECT: Development projects, initiatives

IMPORTANT: When you see "E-1" or "Item E-1", extract "E-1" as entity type "agenda_item"

Extract all entities with their relationships.
"""
    
    with open(prompt_dir / "entity_extraction.txt", 'w') as f:
        f.write(prompt)
    
    print(f"✅ Created custom entity extraction prompt at: {prompt_dir / 'entity_extraction.txt'}")

if __name__ == "__main__":
    create_entity_extraction_prompt() 