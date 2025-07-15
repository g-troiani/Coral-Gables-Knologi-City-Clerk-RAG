# Debug script to inspect NER output
import json
from pathlib import Path

ner_dir = Path("simple_ner_graph")

# Check a sample of entity files
for category in ["people", "organizations", "relationships"]:
    category_dir = ner_dir / category
    if category_dir.exists():
        files = list(category_dir.glob("*.txt"))[:3]  # First 3 files
        print(f"\n=== {category.upper()} ===")
        for file in files:
            print(f"\nFile: {file.name}")
            with open(file, 'r') as f:
                content = f.read()
                print(content[:500])  # First 500 chars 