import subprocess
import sys
import os
from pathlib import Path
import shutil

class CityClerkPromptTuner:
    """Auto-tune GraphRAG prompts for city clerk documents."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.prompts_dir = self.graphrag_root / "prompts"
        
    def run_auto_tuning(self):
        """Run GraphRAG auto-tuning for city clerk domain."""
        
        # First, ensure prompts directory is clean
        if self.prompts_dir.exists():
            import shutil
            shutil.rmtree(self.prompts_dir)
        self.prompts_dir.mkdir(parents=True, exist_ok=True)
        
        # Create domain-specific examples file
        examples_file = self.graphrag_root / "domain_examples.txt"
        with open(examples_file, 'w') as f:
            f.write("""
Examples of entities in city clerk documents:

AGENDA_ITEM: E-1, F-10, H-3 (format: letter-number identifying agenda items)
ORDINANCE: 2024-01, 2024-15 (format: year-number for city ordinances)  
RESOLUTION: 2024-123, 2024-45 (format: year-number for city resolutions)
MEETING: January 9, 2024 City Commission Meeting
PERSON: Commissioner Smith, Mayor Johnson
ORGANIZATION: City of Coral Gables, Parks Department
MONEY: $1.5 million, $250,000
PROJECT: Waterfront Development, Parks Renovation

Example text:
"Agenda Item E-1 relates to Ordinance 2024-01 regarding the Cocoplum security district."
"Commissioner Smith moved to approve Resolution 2024-15 for $1.5 million funding."
""")
        
        # Get the correct Python executable
        python_exe = self.get_venv_python()
        print(f"🐍 Using Python: {python_exe}")
        
        # Run tuning with correct arguments
        cmd = [
            python_exe,
            "-m", "graphrag", "prompt-tune",
            "--root", str(self.graphrag_root),
            "--config", str(self.graphrag_root / "settings.yaml"),
            "--domain", "city government meetings, ordinances, resolutions, agenda items like E-1 and F-10",
            "--selection-method", "random",
            "--limit", "50",
            "--language", "English",
            "--max-tokens", "2000",
            "--chunk-size", "1200",
            "--output", str(self.prompts_dir)
        ]
        
        # Note: --examples flag might not exist in this version
        # Remove it if it causes issues
        
        subprocess.run(cmd, check=True)
        
    def get_venv_python(self):
        """Get the correct Python executable."""
        # Check if we're in a venv
        if sys.prefix != sys.base_prefix:
            return sys.executable
        
        # Try common venv locations
        venv_paths = [
            'venv/bin/python3',
            'venv/bin/python',
            '.venv/bin/python3',
            '.venv/bin/python',
            'city_clerk_rag/bin/python3',
            'city_clerk_rag/bin/python'
        ]
        
        for venv_path in venv_paths:
            full_path = os.path.join(os.getcwd(), venv_path)
            if os.path.exists(full_path):
                return full_path
        
        # Fallback
        return sys.executable
        
    def create_manual_prompts(self):
        """Create prompts manually without auto-tuning."""
        self.prompts_dir.mkdir(parents=True, exist_ok=True)
        
        # Entity extraction prompt - GraphRAG expects specific format
        entity_prompt = """
-Goal-
Given a text document that is potentially relevant to this activity and a list of entity types, identify all entities of those types from the text and all relationships among the identified entities.

-Steps-
1. Identify all entities. For each identified entity, extract the following information:
- title: Name of the entity, capitalized
- type: One of the following types: [{entity_types}]
- description: Comprehensive description of the entity's attributes and activities
Format each entity as ("entity"<|><title><|><type><|><description>)

2. From the entities identified in step 1, identify all pairs of (source_entity, target_entity) that are *clearly related* to each other.
For each pair of related entities, extract the following information:
- source: name of the source entity, as identified in step 1
- target: name of the target entity, as identified in step 1
- description: explanation as to why you think the source entity and the target entity are related to each other
- weight: a numeric score indicating strength of the relationship between the source entity and target entity
Format each relationship as ("relationship"<|><source><|><target><|><description><|><weight>)

3. Return output in English as a single list of all the entities and relationships identified in steps 1 and 2. Use **{record_delimiter}** as the list delimiter.

4. When finished, output {completion_delimiter}

-Examples-
Entity types: AGENDA_ITEM, ORDINANCE, PERSON, ORGANIZATION
Text: "Commissioner Smith moved to approve Ordinance 2024-01 relating to agenda item E-1."
Output:
("entity"<|>SMITH<|>PERSON<|>Commissioner Smith, a member of the city commission who made a motion)
{record_delimiter}
("entity"<|>2024-01<|>ORDINANCE<|>Ordinance 2024-01, a city ordinance being considered for approval)
{record_delimiter}
("entity"<|>E-1<|>AGENDA_ITEM<|>Agenda item E-1, an item on the meeting agenda that ordinance 2024-01 relates to)
{record_delimiter}
("relationship"<|>SMITH<|>2024-01<|>Commissioner Smith moved to approve Ordinance 2024-01<|>8)
{record_delimiter}
("relationship"<|>2024-01<|>E-1<|>Ordinance 2024-01 relates to agenda item E-1<|>9)
{completion_delimiter}

-Real Data-
Entity types: {entity_types}
Text: {input_text}
Output:
"""
        
        with open(self.prompts_dir / "entity_extraction.txt", 'w') as f:
            f.write(entity_prompt)
        
        # Community report prompt
        community_prompt = """
You are analyzing a community of related entities from city government documents.
Provide a comprehensive summary of the community, focusing on:
1. Key entities and their roles
2. Main relationships and interactions
3. Important decisions or actions
4. Overall significance to city governance

Community data:
{input_text}

Summary:
"""
        
        with open(self.prompts_dir / "community_report.txt", 'w') as f:
            f.write(community_prompt)
        
        print("✅ Created manual prompts with GraphRAG format")
        
    def customize_prompts(self):
        """Further customize prompts for city clerk specifics."""
        # Load and modify entity extraction prompt
        entity_prompt_path = self.prompts_dir / "entity_extraction.txt"
        
        if entity_prompt_path.exists():
            with open(entity_prompt_path, 'r') as f:
                prompt = f.read()
            
            # Add city clerk specific examples
            custom_additions = """
### City Clerk Specific Instructions:
- Pay special attention to agenda item codes (e.g., E-1, F-10, H-3)
- Extract voting records (who voted yes/no on what)
- Identify ordinance and resolution numbers (e.g., 2024-01, Resolution 2024-123)
- Extract budget amounts and financial figures
- Identify project names and development proposals
- Note public comment speakers and their concerns
"""
            
            # Insert custom additions
            prompt = prompt.replace("-Real Data-", custom_additions + "\n-Real Data-")
            
            with open(entity_prompt_path, 'w') as f:
                f.write(prompt) 