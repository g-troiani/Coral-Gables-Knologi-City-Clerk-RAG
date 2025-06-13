"""
Encapsulates the execution of the Microsoft GraphRAG indexing process.
"""

import subprocess
import logging
from pathlib import Path
from typing import Optional
import os

log = logging.getLogger(__name__)


class GraphRAGIndexer:
    """Handles the execution of Microsoft GraphRAG indexing."""
    
    def __init__(self):
        pass

    def run_indexing_process(self, graphrag_root: Path, verbose: bool = True, force: bool = False) -> None:
        """
        Executes the graphrag index command as a subprocess.
        
        Args:
            graphrag_root: Root directory for GraphRAG operations
            verbose: Whether to use verbose output
            force: Whether to force re-indexing by re-initializing
        """
        log.info(f"🔄 Starting GraphRAG indexing process at root: {graphrag_root}")
        
        # Ensure the directory exists
        graphrag_root.mkdir(parents=True, exist_ok=True)
        
        # Initialize GraphRAG if needed or if forcing
        if force or not self._is_graphrag_initialized(graphrag_root):
            log.info("📋 Initializing GraphRAG configuration...")
            self._initialize_graphrag(graphrag_root)
        
        # Create the index command
        cmd = [
            "graphrag",
            "index",
            "--root", str(graphrag_root),
        ]

        if verbose:
            cmd.append("--verbose")

        log.info(f"🚀 Executing command: {' '.join(cmd)}")
        
        # Change to the GraphRAG directory for execution
        original_cwd = os.getcwd()
        try:
            os.chdir(graphrag_root)
            
            # Run with live output streaming to the console
            process = subprocess.Popen(
                cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT, 
                text=True,
                cwd=str(graphrag_root)
            )
            
            # Stream output in real-time
            for line in iter(process.stdout.readline, ''):
                log.info(line.strip())
            
            process.wait()

            if process.returncode != 0:
                log.error(f"❌ GraphRAG indexing failed with exit code {process.returncode}")
                raise RuntimeError(f"GraphRAG indexing failed with exit code {process.returncode}")
            
            log.info("✅ GraphRAG indexing completed successfully")
            
        finally:
            os.chdir(original_cwd)

    def _is_graphrag_initialized(self, graphrag_root: Path) -> bool:
        """Check if GraphRAG has been initialized in the given directory."""
        settings_file = graphrag_root / "settings.yaml"
        return settings_file.exists()

    def _initialize_graphrag(self, graphrag_root: Path) -> None:
        """Initialize GraphRAG in the given directory."""
        init_cmd = [
            "graphrag",
            "init",
            "--root", str(graphrag_root)
        ]
        
        log.info(f"🔧 Initializing GraphRAG: {' '.join(init_cmd)}")
        
        try:
            result = subprocess.run(
                init_cmd,
                cwd=str(graphrag_root),
                check=True,
                capture_output=True,
                text=True
            )
            
            log.info("✅ GraphRAG initialization completed")
            
            # Log output if verbose
            if result.stdout:
                log.debug(f"Init stdout: {result.stdout}")
            if result.stderr:
                log.debug(f"Init stderr: {result.stderr}")
                
        except subprocess.CalledProcessError as e:
            log.error(f"❌ GraphRAG initialization failed: {e}")
            if e.stdout:
                log.error(f"Stdout: {e.stdout}")
            if e.stderr:
                log.error(f"Stderr: {e.stderr}")
            raise

    def check_status(self, graphrag_root: Path) -> dict:
        """
        Check the status of GraphRAG indexing for the given directory.
        
        Args:
            graphrag_root: Root directory for GraphRAG operations
            
        Returns:
            Dictionary with status information
        """
        status = {
            'initialized': self._is_graphrag_initialized(graphrag_root),
            'output_exists': False,
            'entities_count': 0,
            'relationships_count': 0,
            'communities_count': 0
        }
        
        # Check for output files
        output_dir = graphrag_root / "output"
        if output_dir.exists():
            status['output_exists'] = True
            
            # Check for specific output files and count entities
            entities_file = output_dir / "create_final_entities.parquet"
            if entities_file.exists():
                try:
                    import pandas as pd
                    entities_df = pd.read_parquet(entities_file)
                    status['entities_count'] = len(entities_df)
                except Exception as e:
                    log.warning(f"Could not read entities file: {e}")
            
            # Check relationships
            relationships_file = output_dir / "create_final_relationships.parquet"
            if relationships_file.exists():
                try:
                    import pandas as pd
                    relationships_df = pd.read_parquet(relationships_file)
                    status['relationships_count'] = len(relationships_df)
                except Exception as e:
                    log.warning(f"Could not read relationships file: {e}")
            
            # Check communities
            communities_file = output_dir / "create_final_communities.parquet"
            if communities_file.exists():
                try:
                    import pandas as pd
                    communities_df = pd.read_parquet(communities_file)
                    status['communities_count'] = len(communities_df)
                except Exception as e:
                    log.warning(f"Could not read communities file: {e}")
        
        return status

    def cleanup_cache(self, graphrag_root: Path) -> None:
        """
        Clean up GraphRAG cache to force fresh indexing.
        
        Args:
            graphrag_root: Root directory for GraphRAG operations
        """
        log.info("🧹 Cleaning up GraphRAG cache")
        
        cache_dir = graphrag_root / "cache"
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
            log.info("✅ Cache directory removed")
        else:
            log.info("ℹ️ No cache directory found")

    def get_indexing_logs(self, graphrag_root: Path, lines: int = 50) -> list:
        """
        Get the last N lines from GraphRAG indexing logs.
        
        Args:
            graphrag_root: Root directory for GraphRAG operations
            lines: Number of lines to retrieve
            
        Returns:
            List of log lines
        """
        logs_dir = graphrag_root / "output" / "indexing-engine"
        log_files = []
        
        if logs_dir.exists():
            log_files = list(logs_dir.glob("*.log"))
        
        if not log_files:
            return ["No log files found"]
        
        # Get the most recent log file
        latest_log = max(log_files, key=lambda f: f.stat().st_mtime)
        
        try:
            with open(latest_log, 'r') as f:
                all_lines = f.readlines()
                return all_lines[-lines:] if len(all_lines) > lines else all_lines
        except Exception as e:
            return [f"Error reading log file: {e}"] 