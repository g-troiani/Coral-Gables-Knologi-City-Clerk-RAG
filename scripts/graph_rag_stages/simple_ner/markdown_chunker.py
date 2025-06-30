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
        
        Returns:
            Total number of chunks created
        """
        markdown_files = list(markdown_dir.glob("*.md"))
        log.info(f"Found {len(markdown_files)} markdown files to chunk")
        
        total_chunks = 0
        for md_file in markdown_files:
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
        
        # Look for YAML-style header
        if content.startswith("---"):
            try:
                _, header, _ = content.split("---", 2)
                for line in header.strip().split("\n"):
                    if ":" in line and line.strip().startswith("- "):
                        key_value = line[2:].split(":", 1)
                        if len(key_value) == 2:
                            key = key_value[0].strip().lower().replace(" ", "_")
                            value = key_value[1].strip()
                            metadata[key] = value
            except ValueError:
                pass
        
        # Extract document type from content patterns
        if not metadata.get('document_type'):
            content_lower = content.lower()
            if 'agenda' in content_lower[:500]:
                metadata['document_type'] = 'agenda'
            elif 'ordinance' in content_lower[:500]:
                metadata['document_type'] = 'ordinance'
            elif 'resolution' in content_lower[:500]:
                metadata['document_type'] = 'resolution'
            elif 'transcript' in content_lower[:500]:
                metadata['document_type'] = 'transcript'
        
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