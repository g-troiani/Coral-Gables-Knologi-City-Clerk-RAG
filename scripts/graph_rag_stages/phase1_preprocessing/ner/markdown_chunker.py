"""
Markdown document chunker for Simple NER pipeline.
Splits documents into manageable chunks while preserving context.
"""

import hashlib
import logging
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import asyncio

log = logging.getLogger(__name__)


class MarkdownChunker:
    """Chunks markdown documents for NER processing."""
    
    def __init__(self, output_dir: Path, chunk_size: int = 1000, chunk_overlap: int = 100):
        """
        Initialize the chunker.
        
        Args:
            output_dir: Base directory for output
            chunk_size: Target chunk size in tokens
            chunk_overlap: Overlap between chunks in tokens
        """
        self.output_dir = Path(output_dir)
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        
        # Create directory structure
        self.chunks_dir = self.output_dir / "document_chunks"
        self.chunks_dir.mkdir(parents=True, exist_ok=True)
        
        # Simple token estimation (can be replaced with tiktoken)
        self.avg_token_length = 4  # Average characters per token
    
    async def process_directory(self, markdown_dir: Path) -> int:
        """
        Process all markdown files in a directory.
        Prioritizes enhanced versions over basic versions.
        
        Returns:
            Total number of chunks created
        """
        # Get all markdown files
        all_markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(all_markdown_files)} total markdown files")
        
        # Separate enhanced and basic files
        enhanced_files = {}  # key: base_name, value: file_path
        basic_files = {}     # key: base_name, value: file_path
        
        for md_file in all_markdown_files:
            if "_enhanced_" in md_file.name:
                # Extract base name for enhanced files
                # E.g., "2017-09 - 03_28_2017_enhanced_ordinance.md" -> "2017-09"
                base_name = md_file.name.split("_enhanced_")[0]
                # Handle cases like "2017-09 - 03_28_2017_enhanced_ordinance.md"
                if " - " in base_name:
                    base_name = base_name.split(" - ")[0]
                enhanced_files[base_name] = md_file
            else:
                # Basic files like "Ordinance_2017-09.md"
                base_name = md_file.stem
                # Extract document number from basic file names
                if base_name.startswith("Ordinance_"):
                    base_name = base_name.replace("Ordinance_", "")
                elif base_name.startswith("Resolution_"):
                    base_name = base_name.replace("Resolution_", "")
                elif base_name.startswith("Agenda_"):
                    base_name = base_name.replace("Agenda_", "")
                basic_files[base_name] = md_file
        
        # Prioritize enhanced files, fallback to basic files
        files_to_process = []
        
        for base_name, enhanced_file in enhanced_files.items():
            files_to_process.append(enhanced_file)
            if base_name in basic_files:
                log.debug(f"Skipping basic file {basic_files[base_name].name} - using enhanced version {enhanced_file.name}")
        
        # Add basic files that don't have enhanced versions
        for base_name, basic_file in basic_files.items():
            if base_name not in enhanced_files:
                files_to_process.append(basic_file)
                log.debug(f"Using basic file {basic_file.name} - no enhanced version available")
        
        log.info(f"Processing {len(files_to_process)} markdown files ({len(enhanced_files)} enhanced, {len(files_to_process) - len(enhanced_files)} basic)")
        
        # Process selected files
        total_chunks = 0
        for md_file in files_to_process:
            chunks = await self.chunk_document(md_file)
            total_chunks += len(chunks)
        
        return total_chunks
    
    async def chunk_document(self, md_file: Path) -> List[Dict[str, str]]:
        """
        Chunk a single markdown document.
        
        Returns:
            List of chunk dictionaries with metadata
        """
        log.debug(f"Chunking document: {md_file.name}")
        
        # Read content
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract metadata from header if present
        metadata = self._extract_metadata(content)
        
        # Validate that document has meeting_date - skip if missing
        if not metadata.get('meeting_date'):
            log.warning(f"Skipping {md_file.name} - no meeting date found in metadata")
            return []
        
        # Validate meeting date format
        meeting_date = metadata['meeting_date']
        if not self._is_valid_date(meeting_date):
            log.warning(f"Skipping {md_file.name} - invalid meeting date format: {meeting_date}")
            return []
        
        # Split into chunks
        chunks = self._split_into_chunks(content)
        
        # Save chunks
        saved_chunks = []
        for i, chunk_text in enumerate(chunks):
            chunk_data = {
                'text': chunk_text,
                'source_document': md_file.name,
                'chunk_index': i,
                'total_chunks': len(chunks),
                'metadata': metadata
            }
            
            # Generate hash for chunk
            chunk_id = self._generate_chunk_hash(chunk_text)
            chunk_data['chunk_id'] = chunk_id
            
            # Save chunk
            await self._save_chunk(chunk_id, md_file.stem, chunk_data)
            saved_chunks.append(chunk_data)
        
        log.info(f"Created {len(chunks)} chunks for {md_file.name}")
        return saved_chunks
    
    def _extract_metadata(self, content: str) -> Dict[str, str]:
        """Extract metadata from markdown header."""
        metadata = {}
        
        # Look for metadata section between --- markers
        if content.startswith("---"):
            try:
                _, header_section, _ = content.split("---", 2)
                
                # Parse key-value pairs from header
                for line in header_section.strip().split("\n"):
                    line = line.strip()
                    if ":" in line and line.startswith("- "):
                        # Handle format like "- Document Type: AGENDA"
                        key_value = line[2:].split(":", 1)
                        if len(key_value) == 2:
                            key = key_value[0].strip().lower().replace(" ", "_")
                            value = key_value[1].strip()
                            metadata[key] = value
            except ValueError:
                pass  # No proper header found
        
        # Validate extracted metadata
        from scripts.graph_rag_stages.common.metadata_standards import MetadataStandards
        metadata = MetadataStandards.validate_metadata(metadata)
        
        return metadata
    
    def _split_into_chunks(self, content: str) -> List[str]:
        """Split content into chunks with overlap."""
        # Estimate tokens (simple approach)
        words = content.split()
        tokens_per_word = 1.3  # Rough estimate
        
        chunk_size_words = int(self.chunk_size / tokens_per_word)
        overlap_words = int(self.chunk_overlap / tokens_per_word)
        
        chunks = []
        start = 0
        
        while start < len(words):
            end = start + chunk_size_words
            
            # Try to end at sentence boundary
            if end < len(words):
                chunk_text = ' '.join(words[start:end])
                # Find last sentence boundary
                last_period = chunk_text.rfind('. ')
                if last_period > len(chunk_text) * 0.8:  # If near end
                    end = start + len(chunk_text[:last_period + 1].split())
            
            chunk = ' '.join(words[start:min(end, len(words))])
            chunks.append(chunk)
            
            # Move start with overlap
            start = end - overlap_words
            if start >= len(words):
                break
        
        return chunks
    
    def _generate_chunk_hash(self, text: str) -> str:
        """Generate a unique hash for chunk identification."""
        return hashlib.sha256(text.encode()).hexdigest()[:12]
    
    async def _save_chunk(self, chunk_id: str, doc_name: str, chunk_data: Dict) -> None:
        """Save chunk to file."""
        filename = f"{chunk_id}_{doc_name}.txt"
        filepath = self.chunks_dir / filename
        
        # Save as simple text with metadata header
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"# Chunk ID: {chunk_id}\n")
            f.write(f"# Source: {chunk_data['source_document']}\n")
            f.write(f"# Index: {chunk_data['chunk_index'] + 1}/{chunk_data['total_chunks']}\n")
            
            # Add metadata if available
            for key, value in chunk_data['metadata'].items():
                f.write(f"# {key}: {value}\n")
            
            f.write("\n---\n\n")
            f.write(chunk_data['text'])
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Check if date string is in valid format."""
        import re
        
        # Check for common date formats
        date_patterns = [
            r'^\d{2}\.\d{2}\.\d{4}$',  # 03.28.2017
            r'^\d{4}-\d{2}-\d{2}$',    # 2017-03-28
            r'^\d{1,2}/\d{1,2}/\d{4}$' # 3/28/2017
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, date_str.strip()):
                return True
        
        return False 