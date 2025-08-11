#!/usr/bin/env python3
"""
Simple script to enable debugging flags in main_pipeline.py
"""

import re
from pathlib import Path

def enable_debugging():
    """Enable all debugging flags in main_pipeline.py"""
    pipeline_file = Path("scripts/graph_rag_stages/main_pipeline.py")
    
    if not pipeline_file.exists():
        print(f"❌ Pipeline file not found: {pipeline_file}")
        return False
    
    print(f"📝 Enabling debug flags in {pipeline_file}")
    
    # Read the file
    with open(pipeline_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Enable debug flags
    debug_flags = [
        "DEBUG_DOCUMENT_FLOW",
        "DEBUG_RELATIONSHIP_LINKING", 
        "DEBUG_ENTITY_DEDUPLICATION",
        "DEBUG_FILE_DISCOVERY"
    ]
    
    changes_made = False
    for flag in debug_flags:
        # Replace False with True for each debug flag
        pattern = f"{flag}\\s*=\\s*False"
        replacement = f"{flag} = True"
        
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            print(f"✅ Enabled {flag}")
            changes_made = True
        else:
            print(f"⚠️  Flag {flag} not found or already enabled")
    
    if changes_made:
        # Write back the modified content
        with open(pipeline_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Debug flags enabled successfully!")
        return True
    else:
        print("ℹ️  No changes made - flags may already be enabled")
        return False

def disable_debugging():
    """Disable all debugging flags in main_pipeline.py"""
    pipeline_file = Path("scripts/graph_rag_stages/main_pipeline.py")
    
    if not pipeline_file.exists():
        print(f"❌ Pipeline file not found: {pipeline_file}")
        return False
    
    print(f"📝 Disabling debug flags in {pipeline_file}")
    
    # Read the file
    with open(pipeline_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Disable debug flags
    debug_flags = [
        "DEBUG_DOCUMENT_FLOW",
        "DEBUG_RELATIONSHIP_LINKING", 
        "DEBUG_ENTITY_DEDUPLICATION",
        "DEBUG_FILE_DISCOVERY"
    ]
    
    changes_made = False
    for flag in debug_flags:
        # Replace True with False for each debug flag
        pattern = f"{flag}\\s*=\\s*True"
        replacement = f"{flag} = False"
        
        if re.search(pattern, content):
            content = re.sub(pattern, replacement, content)
            print(f"✅ Disabled {flag}")
            changes_made = True
    
    if changes_made:
        # Write back the modified content
        with open(pipeline_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Debug flags disabled successfully!")
        return True
    else:
        print("ℹ️  No changes made - flags may already be disabled")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "disable":
        disable_debugging()
    else:
        enable_debugging()
        
    print("\n🚀 Run the pipeline with:")
    print("   python -m scripts.graph_rag_stages.main_pipeline")
    print("\n🔧 To disable debugging later:")
    print("   python enable_debug.py disable")
