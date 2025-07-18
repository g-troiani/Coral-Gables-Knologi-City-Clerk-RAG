#!/usr/bin/env python3
"""
Aggregate API Response Files

This script:
1. Finds all folders starting with "API_" in the 2019 directory
2. For each API_ folder, concatenates all .md files within it
3. Saves the concatenated content to a new .md file named "AGGREGATE_[ORIGINAL_NAME]"
4. Output files are saved in the 2019 folder
"""

import os
import logging
from pathlib import Path
from typing import List, Dict
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('aggregate_api_responses.log')
    ]
)
log = logging.getLogger(__name__)

class APIResponseAggregator:
    def __init__(self, base_path: str):
        """Initialize the aggregator with the base 2019 folder path."""
        self.base_path = Path(base_path)
        self.output_path = self.base_path  # Output files go in the 2019 folder
        
        if not self.base_path.exists():
            raise ValueError(f"❌ Base path does not exist: {self.base_path}")
            
    def get_api_folders(self) -> List[Path]:
        """Get all folders starting with 'API_' in the 2019 directory."""
        api_folders = [d for d in self.base_path.iterdir() 
                      if d.is_dir() and d.name.startswith("API_")]
        
        log.info(f"📁 Found {len(api_folders)} API folders: {[d.name for d in api_folders]}")
        return api_folders
    
    def get_original_name(self, api_folder_name: str) -> str:
        """Extract the original folder name from API_ folder name."""
        # Remove "API_" prefix
        if api_folder_name.startswith("API_"):
            return api_folder_name[4:]  # Remove "API_" (4 characters)
        return api_folder_name
    
    def read_all_md_files(self, folder_path: Path) -> List[Dict[str, str]]:
        """Read all .md files from a folder and return their content with metadata."""
        md_files = list(folder_path.glob("*.md"))
        file_contents = []
        
        log.info(f"📄 Found {len(md_files)} .md files in {folder_path.name}")
        
        for md_file in sorted(md_files):
            try:
                with open(md_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                file_contents.append({
                    'filename': md_file.name,
                    'filepath': str(md_file),
                    'content': content,
                    'size': len(content)
                })
                
            except Exception as e:
                log.error(f"❌ Error reading {md_file}: {str(e)}")
                continue
                
        return file_contents
    
    def create_aggregate_content(self, file_contents: List[Dict[str, str]], 
                               original_folder_name: str) -> str:
        """Create the aggregated content with proper formatting."""
        
        # Header for the aggregate file
        header = f"""# AGGREGATE: {original_folder_name}

**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Total Files:** {len(file_contents)}  
**Source Folder:** API_{original_folder_name}

---

## Table of Contents

"""
        
        # Add table of contents
        toc = ""
        for i, file_info in enumerate(file_contents, 1):
            toc += f"{i}. [{file_info['filename']}](#{file_info['filename'].replace(' ', '-').replace('.', '').lower()})\n"
        
        header += toc + "\n---\n\n"
        
        # Add each file's content
        aggregated_content = header
        
        for i, file_info in enumerate(file_contents, 1):
            file_section = f"""
## {i}. {file_info['filename']}

**File:** `{file_info['filename']}`  
**Size:** {file_info['size']} characters  
**Source:** `{file_info['filepath']}`

### Content:

{file_info['content']}

---

"""
            aggregated_content += file_section
        
        # Add footer
        footer = f"""
---

## Summary

**Total Files Aggregated:** {len(file_contents)}  
**Total Content Length:** {len(aggregated_content)} characters  
**Generation Complete:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        aggregated_content += footer
        return aggregated_content
    
    def save_aggregate_file(self, content: str, original_folder_name: str) -> str:
        """Save the aggregated content to a file."""
        output_filename = f"AGGREGATE_{original_folder_name}.md"
        output_filepath = self.output_path / output_filename
        
        try:
            with open(output_filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            log.info(f"✅ Saved aggregate file: {output_filepath}")
            return str(output_filepath)
            
        except Exception as e:
            log.error(f"❌ Error saving aggregate file {output_filepath}: {str(e)}")
            raise
    
    def process_api_folder(self, api_folder: Path) -> Dict[str, any]:
        """Process a single API folder and create its aggregate file."""
        log.info(f"🔄 Processing folder: {api_folder.name}")
        
        # Get original folder name
        original_name = self.get_original_name(api_folder.name)
        
        # Read all .md files
        file_contents = self.read_all_md_files(api_folder)
        
        if not file_contents:
            log.warning(f"⚠️ No .md files found in {api_folder.name}")
            return {
                'folder': api_folder.name,
                'original_name': original_name,
                'files_processed': 0,
                'output_file': None,
                'status': 'no_files'
            }
        
        # Create aggregated content
        aggregated_content = self.create_aggregate_content(file_contents, original_name)
        
        # Save aggregate file
        output_file = self.save_aggregate_file(aggregated_content, original_name)
        
        return {
            'folder': api_folder.name,
            'original_name': original_name,
            'files_processed': len(file_contents),
            'output_file': output_file,
            'total_characters': len(aggregated_content),
            'status': 'success'
        }
    
    def aggregate_all(self) -> Dict[str, any]:
        """Process all API folders and create aggregate files."""
        log.info("🚀 Starting API response aggregation...")
        
        api_folders = self.get_api_folders()
        
        if not api_folders:
            log.warning("⚠️ No API folders found!")
            return {'status': 'no_folders', 'results': []}
        
        results = []
        total_files_processed = 0
        
        for api_folder in api_folders:
            try:
                result = self.process_api_folder(api_folder)
                results.append(result)
                total_files_processed += result['files_processed']
                
            except Exception as e:
                log.error(f"❌ Error processing {api_folder.name}: {str(e)}")
                results.append({
                    'folder': api_folder.name,
                    'status': 'error',
                    'error': str(e)
                })
        
        # Summary
        summary = {
            'status': 'completed',
            'folders_processed': len(api_folders),
            'total_files_aggregated': total_files_processed,
            'results': results,
            'aggregate_files_created': [r['output_file'] for r in results if r.get('output_file')]
        }
        
        log.info(f"✅ Aggregation complete!")
        log.info(f"📊 Processed {len(api_folders)} folders")
        log.info(f"📄 Aggregated {total_files_processed} files")
        log.info(f"📝 Created {len(summary['aggregate_files_created'])} aggregate files")
        
        return summary

def main():
    """Main function to run the aggregation."""
    base_path = "city_clerk_documents/global/City Comissions 2024/2019"
    
    try:
        aggregator = APIResponseAggregator(base_path)
        results = aggregator.aggregate_all()
        
        print("\n" + "="*60)
        print("📊 AGGREGATION SUMMARY")
        print("="*60)
        
        for result in results['results']:
            if result['status'] == 'success':
                print(f"✅ {result['folder']} -> {result['files_processed']} files -> {Path(result['output_file']).name}")
            elif result['status'] == 'no_files':
                print(f"⚠️ {result['folder']} -> No .md files found")
            else:
                print(f"❌ {result['folder']} -> Error: {result.get('error', 'Unknown error')}")
        
        print(f"\n📈 Total: {results['total_files_aggregated']} files aggregated into {len(results['aggregate_files_created'])} files")
        print("="*60)
        
    except Exception as e:
        log.error(f"❌ Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main() 