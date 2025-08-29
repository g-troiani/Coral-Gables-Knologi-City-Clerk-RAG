"""
Incremental extraction support for the preprocessing pipeline.
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

from .extraction_integration import ExtractionPipelineIntegration
from ..common.processing_registry import ProcessingRegistry

log = logging.getLogger(__name__)


class IncrementalExtractionPipeline(ExtractionPipelineIntegration):
    """
    Extended extraction pipeline with incremental processing support.
    
    Inherits from ExtractionPipelineIntegration to reuse all existing logic,
    adding only incremental processing capabilities.
    """
    
    def __init__(self, output_dir: Path):
        super().__init__(output_dir)
        # Initialize processing registry
        registry_dir = output_dir.parent / "registry"
        self.registry = ProcessingRegistry(registry_dir)
        self.incremental_mode = False
        self.processed_files: List[Path] = []
        self.skipped_files: List[Path] = []
    
    async def run_extraction_pipeline(self, base_dir: Path, incremental: bool = False) -> List[Dict[str, Any]]:
        """
        Run extraction pipeline with optional incremental mode.
        
        Args:
            base_dir: Source directory containing PDFs
            incremental: If True, only process new/modified files
            
        Returns:
            List of extracted documents
        """
        self.incremental_mode = incremental
        self.processed_files = []
        self.skipped_files = []
        
        if incremental:
            log.info("🔄 Running in INCREMENTAL mode - processing only new/modified files")
            
            # Validate registry
            is_valid, issues = self.registry.validate_registry()
            if not is_valid:
                log.warning(f"Registry validation issues: {issues}")
                log.info("Falling back to full processing")
                incremental = False
                self.incremental_mode = False
        
        # Get list of files to process
        if incremental:
            files_to_process = self.registry.get_new_documents(base_dir)
            if not files_to_process:
                log.info("✅ No new documents to process")
                return []
            log.info(f"📋 Found {len(files_to_process)} new/modified documents")
        else:
            log.info("🔄 Running in FULL mode - processing all files")
        
        # Run the standard extraction pipeline
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        extracted_documents = await super().run_extraction_pipeline(base_dir)
        
        # Record processing results
        if incremental:
            stats = {
                "documents_processed": len(self.processed_files),
                "documents_skipped": len(self.skipped_files),
                "extraction_success": len(extracted_documents)
            }
            self.registry.mark_incremental_run(run_id, self.processed_files, stats)
        else:
            stats = {
                "total_documents": len(extracted_documents),
                "extraction_success": len(extracted_documents)
            }
            self.registry.mark_full_run(run_id, stats)
        
        return extracted_documents
    
    def _should_process_file(self, file_path: Path) -> bool:
        """Check if a file should be processed based on incremental mode."""
        if not self.incremental_mode:
            return True
        
        is_processed, _ = self.registry.is_document_processed(file_path)
        if is_processed:
            self.skipped_files.append(file_path)
            log.debug(f"Skipping already processed: {file_path.name}")
            return False
        
        return True
    
    def _mark_file_processed(self, file_path: Path, metadata: Dict):
        """Mark a file as processed in the registry."""
        if file_path not in self.processed_files:
            self.processed_files.append(file_path)
        
        # Add processing metadata
        metadata.update({
            "processor": "ExtractionPipelineIntegration",
            "version": "1.0"
        })
        
        self.registry.mark_document_processed(file_path, metadata)
    
    async def _process_agenda_file(self, agenda_file: Path, meeting_date: str) -> Optional[Dict[str, Any]]:
        """Process agenda file with incremental tracking."""
        # Check if should process
        if not self._should_process_file(agenda_file):
            return None
        
        # Process using parent method
        result = await super()._process_agenda_file(agenda_file, meeting_date)
        
        # Mark as processed if successful
        if result:
            metadata = {
                "document_type": "agenda",
                "meeting_date": meeting_date,
                "entities_extracted": len(result.get("entities", [])),
                "agenda_items": len(result.get("agenda_items", []))
            }
            self._mark_file_processed(agenda_file, metadata)
        
        return result
    
    def _find_agenda_files(self, base_dir: Path, meeting_date: str) -> List[Path]:
        """Find agenda files, filtering based on incremental mode."""
        all_files = super()._find_agenda_files(base_dir, meeting_date)
        
        if not self.incremental_mode:
            return all_files
        
        # Filter to only new/modified files
        filtered_files = []
        for file_path in all_files:
            if self._should_process_file(file_path):
                filtered_files.append(file_path)
        
        return filtered_files
    
    async def process_specific_folder(self, folder_path: Path) -> List[Dict[str, Any]]:
        """
        Process only files in a specific folder (for targeted incremental updates).
        
        Args:
            folder_path: Path to folder containing new meeting documents
            
        Returns:
            List of extracted documents
        """
        log.info(f"📁 Processing specific folder: {folder_path}")
        
        if not folder_path.exists():
            log.error(f"Folder does not exist: {folder_path}")
            return []
        
        # Always run in incremental mode for specific folders
        return await self.run_extraction_pipeline(folder_path, incremental=True)
    
    def get_processing_summary(self) -> Dict:
        """Get summary of the processing run."""
        stats = self.registry.get_processing_stats()
        
        return {
            "mode": "incremental" if self.incremental_mode else "full",
            "processed_files": len(self.processed_files),
            "skipped_files": len(self.skipped_files),
            "registry_stats": stats
        }
