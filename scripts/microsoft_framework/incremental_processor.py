from pathlib import Path
import json
from typing import List, Set
import asyncio

class IncrementalGraphRAGProcessor:
    """Handle incremental updates to GraphRAG index."""
    
    def __init__(self, graphrag_root: Path):
        self.graphrag_root = Path(graphrag_root)
        self.processed_files = self._load_processed_files()
        
    async def process_new_documents(self, new_docs_dir: Path):
        """Process only new documents added since last run."""
        
        # Find new documents
        new_files = []
        for doc_path in new_docs_dir.glob("*.pdf"):
            if doc_path.name not in self.processed_files:
                new_files.append(doc_path)
        
        if not new_files:
            print("✅ No new documents to process")
            return
        
        print(f"📄 Processing {len(new_files)} new documents...")
        
        # Extract text using existing Docling pipeline
        from scripts.graph_stages.pdf_extractor import PDFExtractor
        extractor = PDFExtractor(new_docs_dir)
        
        # Process and add to GraphRAG
        # ... implementation details ...
        
        # Update processed files list
        self._update_processed_files(new_files)
    
    def _load_processed_files(self) -> Set[str]:
        """Load list of previously processed files."""
        processed_files_path = self.graphrag_root / "processed_files.json"
        
        if processed_files_path.exists():
            with open(processed_files_path, 'r') as f:
                return set(json.load(f))
        
        return set()
    
    def _update_processed_files(self, new_files: List[Path]):
        """Update the list of processed files."""
        for file_path in new_files:
            self.processed_files.add(file_path.name)
        
        processed_files_path = self.graphrag_root / "processed_files.json"
        with open(processed_files_path, 'w') as f:
            json.dump(list(self.processed_files), f) 