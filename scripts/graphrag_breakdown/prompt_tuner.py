import subprocess
from pathlib import Path
import shutil

class CityClerkPromptTuner:
    """Auto-tune GraphRAG prompts for city clerk documents."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.prompts_dir = self.graphrag_root / "prompts"
        
    def run_auto_tuning(self):
        """Run GraphRAG auto-tuning for city clerk domain."""
        # Run auto-tuning with city government domain
        subprocess.run([
            "graphrag", "prompt-tune",
            "--root", str(self.graphrag_root),
            "--config", str(self.graphrag_root / "settings.yaml"),
            "--domain", "city government meetings, ordinances, resolutions, and public administration",
            "--method", "random",
            "--limit", "50",
            "--language", "English",
            "--max-tokens", "2000",
            "--chunk-size", "1200",
            "--no-entity-types",  # Let it discover entity types
            "--output", str(self.prompts_dir)
        ])
        
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