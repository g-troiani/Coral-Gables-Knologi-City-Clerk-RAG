"""
Processing registry to track document processing state for incremental updates.
"""

import json
import hashlib
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
import logging
import fcntl
from contextlib import contextmanager

log = logging.getLogger(__name__)


class ProcessingRegistry:
    """
    Tracks which documents have been processed to enable incremental processing.
    
    Stores processing state including:
    - Document file hash
    - Processing timestamp
    - Extraction status
    - Entity counts
    - Version information
    """
    
    def __init__(self, registry_dir: Path):
        """Initialize the processing registry."""
        self.registry_dir = Path(registry_dir)
        self.registry_dir.mkdir(parents=True, exist_ok=True)
        self.registry_file = self.registry_dir / "processing_state.json"
        self.lock_file = self.registry_dir / ".processing_state.lock"
        self._ensure_registry_exists()
    
    def _ensure_registry_exists(self):
        """Create registry file if it doesn't exist."""
        if not self.registry_file.exists():
            self._save_registry({
                "version": "1.0",
                "created_at": datetime.now().isoformat(),
                "documents": {},
                "last_full_run": None,
                "incremental_runs": []
            })
    
    @contextmanager
    def _file_lock(self):
        """Acquire file lock for thread-safe operations."""
        lock_fd = None
        try:
            lock_fd = open(self.lock_file, 'w')
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            yield
        finally:
            if lock_fd:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                lock_fd.close()
    
    def _load_registry(self) -> Dict:
        """Load registry data with file locking."""
        with self._file_lock():
            try:
                with open(self.registry_file, 'r') as f:
                    return json.load(f)
            except (json.JSONDecodeError, FileNotFoundError):
                log.warning("Registry file corrupted or missing, creating new one")
                self._ensure_registry_exists()
                with open(self.registry_file, 'r') as f:
                    return json.load(f)
    
    def _save_registry(self, data: Dict):
        """Save registry data with file locking."""
        with self._file_lock():
            with open(self.registry_file, 'w') as f:
                json.dump(data, f, indent=2)
    
    def _calculate_file_hash(self, file_path: Path) -> str:
        """Calculate SHA-256 hash of a file."""
        sha256_hash = hashlib.sha256()
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    
    def is_document_processed(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Check if a document has been processed.
        
        Returns:
            Tuple of (is_processed, previous_hash)
        """
        registry = self._load_registry()
        doc_key = str(file_path.absolute())
        
        if doc_key not in registry["documents"]:
            return False, None
        
        doc_info = registry["documents"][doc_key]
        current_hash = self._calculate_file_hash(file_path)
        previous_hash = doc_info.get("file_hash")
        
        # Document is processed if hash matches
        return current_hash == previous_hash, previous_hash
    
    def mark_document_processed(self, file_path: Path, metadata: Dict):
        """Mark a document as processed with metadata."""
        registry = self._load_registry()
        doc_key = str(file_path.absolute())
        
        # Calculate file hash
        file_hash = self._calculate_file_hash(file_path)
        
        # Update document info
        registry["documents"][doc_key] = {
            "file_hash": file_hash,
            "processed_at": datetime.now().isoformat(),
            "file_name": file_path.name,
            "status": "completed",
            **metadata
        }
        
        self._save_registry(registry)
        log.info(f"Marked as processed: {file_path.name}")
    
    def get_new_documents(self, source_dir: Path, file_pattern: str = "*.pdf") -> List[Path]:
        """
        Get list of new or modified documents in a directory.
        
        Args:
            source_dir: Directory to scan
            file_pattern: File pattern to match (default: *.pdf)
            
        Returns:
            List of paths to new/modified documents
        """
        new_documents = []
        
        # Find all matching files
        if source_dir.is_file():
            files = [source_dir]
        else:
            files = list(source_dir.rglob(file_pattern))
        
        for file_path in files:
            is_processed, _ = self.is_document_processed(file_path)
            if not is_processed:
                new_documents.append(file_path)
                log.info(f"New/modified document found: {file_path.name}")
        
        return new_documents
    
    def mark_incremental_run(self, run_id: str, processed_files: List[Path], stats: Dict):
        """Record an incremental processing run."""
        registry = self._load_registry()
        
        registry["incremental_runs"].append({
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "processed_files": [str(f) for f in processed_files],
            "stats": stats
        })
        
        # Keep only last 100 runs
        if len(registry["incremental_runs"]) > 100:
            registry["incremental_runs"] = registry["incremental_runs"][-100:]
        
        self._save_registry(registry)
    
    def mark_full_run(self, run_id: str, stats: Dict):
        """Record a full processing run."""
        registry = self._load_registry()
        
        registry["last_full_run"] = {
            "run_id": run_id,
            "timestamp": datetime.now().isoformat(),
            "stats": stats
        }
        
        self._save_registry(registry)
    
    def get_processing_stats(self) -> Dict:
        """Get overall processing statistics."""
        registry = self._load_registry()
        
        total_docs = len(registry["documents"])
        completed_docs = sum(1 for d in registry["documents"].values() 
                           if d.get("status") == "completed")
        
        return {
            "total_documents": total_docs,
            "completed_documents": completed_docs,
            "last_full_run": registry.get("last_full_run"),
            "incremental_runs": len(registry.get("incremental_runs", [])),
            "registry_version": registry.get("version", "unknown")
        }
    
    def validate_registry(self) -> Tuple[bool, List[str]]:
        """
        Validate registry integrity.
        
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        try:
            registry = self._load_registry()
            
            # Check required fields
            required_fields = ["version", "documents", "created_at"]
            for field in required_fields:
                if field not in registry:
                    issues.append(f"Missing required field: {field}")
            
            # Validate document entries
            for doc_path, doc_info in registry.get("documents", {}).items():
                if "file_hash" not in doc_info:
                    issues.append(f"Missing hash for document: {doc_path}")
                
                # Check if file still exists
                if not Path(doc_path).exists():
                    issues.append(f"Tracked file no longer exists: {doc_path}")
            
        except Exception as e:
            issues.append(f"Registry validation error: {str(e)}")
        
        return len(issues) == 0, issues
    
    def rebuild_from_directory(self, source_dir: Path, file_pattern: str = "*.pdf"):
        """
        Rebuild registry by scanning a directory.
        Useful for recovery or initial setup.
        """
        log.info(f"Rebuilding registry from: {source_dir}")
        
        # Create new registry
        new_registry = {
            "version": "1.0",
            "created_at": datetime.now().isoformat(),
            "documents": {},
            "last_full_run": None,
            "incremental_runs": [],
            "rebuilt_at": datetime.now().isoformat()
        }
        
        # Scan all files
        files = list(source_dir.rglob(file_pattern))
        for file_path in files:
            try:
                file_hash = self._calculate_file_hash(file_path)
                doc_key = str(file_path.absolute())
                
                new_registry["documents"][doc_key] = {
                    "file_hash": file_hash,
                    "processed_at": datetime.now().isoformat(),
                    "file_name": file_path.name,
                    "status": "unknown",  # We don't know if it was actually processed
                    "rebuilt": True
                }
            except Exception as e:
                log.warning(f"Error processing {file_path}: {e}")
        
        self._save_registry(new_registry)
        log.info(f"Registry rebuilt with {len(new_registry['documents'])} documents")
