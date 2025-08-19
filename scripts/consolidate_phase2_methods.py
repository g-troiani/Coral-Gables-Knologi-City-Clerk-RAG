#!/usr/bin/env python3
"""
Consolidate redundant methods in phase2_building by delegating to common standards.
"""
import re
from pathlib import Path

def consolidate_id_methods(content: str, file_name: str) -> str:
    """Replace redundant ID methods with calls to EntityIDStandards."""
    
    if 'entity_deduplicator_extended.py' in file_name:
        # Remove redundant _canon_entity_type method (duplicate of _canon_type)
        content = re.sub(
            r'# --- Canonicalize ontology types.*?\n\s*def _canon_entity_type\(self.*?\n(?:.*?\n)*?\s*return m\.get\(t\.lower\(\), t\)\n',
            '',
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        # Remove _hash8 method (use EntityIDStandards._hash8 directly)
        content = re.sub(
            r'def _hash8\(self, s: str\) -> str:\s*\n\s*return hashlib\.sha256.*?\n',
            '',
            content,
            flags=re.MULTILINE
        )
        
        # Replace self._hash8 calls with EntityIDStandards._hash8
        content = re.sub(r'self\._hash8\(', 'EntityIDStandards._hash8(', content)
        
        # Remove redundant helper methods that just delegate
        content = re.sub(
            r'def _extract_e_code\(self.*?\n.*?return EntityIDStandards\._extract_e_code.*?\n',
            '',
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        content = re.sub(
            r'def _extract_ordres_number\(self.*?\n.*?return EntityIDStandards\._extract_ordres_number.*?\n',
            '',
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        # Replace method calls
        content = re.sub(r'self\._extract_e_code\(', 'EntityIDStandards._extract_e_code(', content)
        content = re.sub(r'self\._extract_ordres_number\(', 'EntityIDStandards._extract_ordres_number(', content)
        
    elif 'taxonomy_synthesizer.py' in file_name:
        # Remove local _hash8 function
        content = re.sub(
            r'def _hash8\(s: str\) -> str:\s*\n\s*return EntityIDStandards\._hash8.*?\n',
            '',
            content,
            flags=re.MULTILINE
        )
        
        # Replace _hash8 calls
        content = re.sub(r'_hash8\(', 'EntityIDStandards._hash8(', content)
        
        # Remove duplicate _policy_id_from_ordinance
        content = re.sub(
            r'def _policy_id_from_ordinance\(ordinance_number:.*?\n(?:.*?\n)*?return.*?\n',
            '',
            content,
            flags=re.MULTILINE | re.DOTALL
        )
        
        # Replace calls to _policy_id_from_ordinance
        content = re.sub(
            r'_policy_id_from_ordinance\(([^,]+),\s*([^)]+)\)',
            r'EntityIDStandards.make_policy_id("ordinance", "", \1, \2)',
            content
        )
        
    elif 'custom_graph_builder.py' in file_name:
        # Remove _sanitize_id if it duplicates EntityIDStandards functionality
        if 'def _sanitize_id(' in content and 'EntityIDStandards' in content:
            content = re.sub(
                r'def _sanitize_id\(self, id_str: str\) -> str:.*?return.*?\n',
                '',
                content,
                flags=re.MULTILINE | re.DOTALL
            )
            content = re.sub(r'self\._sanitize_id\(', 'EntityIDStandards.sanitize_id(', content)
    
    return content

def remove_unused_methods(content: str, file_name: str) -> str:
    """Remove methods that are never called."""
    
    # Find all method definitions
    method_pattern = r'def\s+(\w+)\s*\('
    methods = re.findall(method_pattern, content)
    
    unused_methods = []
    for method in methods:
        if method.startswith('_') and method not in ['__init__', '__str__', '__repr__']:
            # Check if method is called anywhere
            # Look for self.method( or method(
            call_pattern1 = rf'self\.{method}\s*\('
            call_pattern2 = rf'(?<!def\s){method}\s*\('
            
            if not re.search(call_pattern1, content) and not re.search(call_pattern2, content):
                unused_methods.append(method)
    
    # Remove unused methods
    for method in unused_methods:
        # Remove the entire method definition
        pattern = rf'def\s+{method}\s*\([^)]*\).*?(?=\n\s*def|\n\s*async def|\nclass|\Z)'
        content = re.sub(pattern, '', content, flags=re.MULTILINE | re.DOTALL)
    
    return content

def consolidate_normalization_keys(content: str, file_name: str) -> str:
    """Consolidate multiple normalization key methods into one."""
    
    if 'entity_deduplicator_extended.py' in file_name:
        # Check if there are multiple get_normalization_key methods
        if '_get_document_normalization_key' in content and '_get_normalization_key' in content:
            # Update _get_normalization_key to handle documents specially
            content = re.sub(
                r'(def _get_normalization_key.*?\n(?:.*?\n)*?)(\s*return.*?\n)',
                r'\1        # Handle documents with special logic\n'
                r'        if entity_type == "Document":\n'
                r'            return self._get_document_normalization_key(entity)\n\2',
                content,
                flags=re.MULTILINE | re.DOTALL
            )
    
    return content

def clean_empty_sections(content: str) -> str:
    """Remove empty comment sections and clean up spacing."""
    # Remove comment lines that serve as empty section headers
    content = re.sub(r'\n\s*# ---.*?---\s*\n(?=\s*\n)', '\n', content)
    
    # Clean up multiple blank lines
    content = re.sub(r'\n\n\n+', '\n\n', content)
    
    # Remove trailing whitespace
    content = re.sub(r' +$', '', content, flags=re.MULTILINE)
    
    return content

def main():
    """Main consolidation function."""
    phase2_dir = Path("scripts/graph_rag_stages/phase2_building")
    
    files_to_process = [
        phase2_dir / "entity_deduplicator_extended.py",
        phase2_dir / "taxonomy_synthesizer.py", 
        phase2_dir / "custom_graph_builder.py",
    ]
    
    print("=== Consolidating Redundant Methods in Phase2 Building ===\n")
    
    modified_count = 0
    for file_path in files_to_process:
        if file_path.exists():
            print(f"Processing: {file_path}")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            original_content = content
            
            # Apply consolidations
            content = consolidate_id_methods(content, str(file_path))
            content = remove_unused_methods(content, str(file_path))
            content = consolidate_normalization_keys(content, str(file_path))
            content = clean_empty_sections(content)
            
            if content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"  ✓ Modified: Consolidated redundant methods")
                modified_count += 1
            else:
                print(f"  - No changes needed")
    
    print(f"\n✅ Complete! Modified {modified_count} files.")

if __name__ == "__main__":
    main()
