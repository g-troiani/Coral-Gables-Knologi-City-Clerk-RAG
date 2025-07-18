#!/usr/bin/env python3
"""
Split Large Aggregate Files

This script:
1. Finds all files starting with "AGGREGATE_" in the 2019 directory
2. Checks if the file size exceeds a specified character limit (300,000)
3. If it does, splits the file into smaller parts, each under the limit
4. Saves the smaller parts with a " - part X" suffix
"""

import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('split_large_files.log')
    ]
)
log = logging.getLogger(__name__)

class FileSplitter:
    def __init__(self, base_path: str, max_chars: int = 300_000):
        """Initialize the splitter."""
        self.base_path = Path(base_path)
        self.max_chars = max_chars
        
        if not self.base_path.exists():
            raise ValueError(f"❌ Base path does not exist: {self.base_path}")
            
    def get_aggregate_files(self) -> list[Path]:
        """Get all AGGREGATE files."""
        return list(self.base_path.glob("AGGREGATE_*.md"))
    
    def split_file(self, file_path: Path):
        """Split a single file if it exceeds the max character limit."""
        try:
            content = file_path.read_text(encoding='utf-8')
            file_size = len(content)
            
            if file_size <= self.max_chars:
                log.info(f"👍 Skipping '{file_path.name}': size ({file_size}) is within limit ({self.max_chars}).")
                return
            
            log.info(f"✂️ Splitting '{file_path.name}': size ({file_size}) exceeds limit ({self.max_chars}).")
            
            base_name = file_path.stem
            chunks = []
            current_chunk = ""
            
            for line in content.splitlines(keepends=True):
                if len(current_chunk) + len(line) > self.max_chars:
                    chunks.append(current_chunk)
                    current_chunk = ""
                current_chunk += line
            
            if current_chunk:
                chunks.append(current_chunk)
            
            log.info(f"📄 Created {len(chunks)} chunks for '{file_path.name}'.")
            
            for i, chunk in enumerate(chunks, 1):
                part_name = f"{base_name} - part {i}.md"
                part_path = self.base_path / part_name
                part_path.write_text(chunk, encoding='utf-8')
                log.info(f"✅ Saved part {i}: '{part_path.name}' ({len(chunk)} chars)")
            
            # Optional: remove original large file after splitting
            # file_path.unlink()
            # log.info(f"🗑️ Removed original file: '{file_path.name}'")
                
        except Exception as e:
            log.error(f"❌ Error processing file '{file_path.name}': {e}")
            
    def run(self):
        """Run the splitting process for all aggregate files."""
        log.info("🚀 Starting file splitting process...")
        log.info(f"📏 Max characters per file: {self.max_chars}")
        
        aggregate_files = self.get_aggregate_files()
        
        if not aggregate_files:
            log.warning("⚠️ No 'AGGREGATE_' files found to process.")
            return
            
        log.info(f"📁 Found {len(aggregate_files)} aggregate files to check.")
        
        for file_path in aggregate_files:
            self.split_file(file_path)
            
        log.info("✅ File splitting process complete.")

def main():
    """Main function to run the splitter."""
    base_path = "city_clerk_documents/global/City Comissions 2024/2019"
    
    try:
        splitter = FileSplitter(base_path)
        splitter.run()
    except Exception as e:
        log.fatal(f"❌ A fatal error occurred: {e}")

if __name__ == "__main__":
    main() 