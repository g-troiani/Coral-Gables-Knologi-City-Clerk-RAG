import asyncio
import subprocess
from pathlib import Path
import pandas as pd
import json
from typing import Dict, List, Any

# Import other components
from .graphrag_initializer import GraphRAGInitializer
from .document_adapter import CityClerkDocumentAdapter
from .prompt_tuner import CityClerkPromptTuner
from .graphrag_output_processor import GraphRAGOutputProcessor

class CityClerkGraphRAGPipeline:
    """Main pipeline for processing city clerk documents with GraphRAG."""
    
    def __init__(self, project_root: Path):
        self.project_root = Path(project_root)
        self.graphrag_root = self.project_root / "graphrag_data"
        self.output_dir = self.graphrag_root / "output"
        
    async def run_full_pipeline(self, force_reindex: bool = False):
        """Run the complete GraphRAG indexing pipeline."""
        
        # Step 1: Initialize GraphRAG
        print("🔧 Initializing GraphRAG...")
        initializer = GraphRAGInitializer(self.project_root)
        initializer.setup_environment()
        
        # Step 2: Prepare documents
        print("📄 Preparing documents...")
        adapter = CityClerkDocumentAdapter(
            self.project_root / "city_clerk_documents/extracted_text"
        )
        df = adapter.prepare_documents_for_graphrag(self.graphrag_root)
        
        # Step 3: Run prompt tuning
        print("🎯 Tuning prompts for city clerk domain...")
        tuner = CityClerkPromptTuner(self.graphrag_root)
        tuner.run_auto_tuning()
        tuner.customize_prompts()
        
        # Step 4: Run GraphRAG indexing
        print("🏗️ Running GraphRAG indexing...")
        subprocess.run([
            "graphrag", "index",
            "--root", str(self.graphrag_root),
            "--verbose"
        ])
        
        # Step 5: Process outputs
        print("📊 Processing GraphRAG outputs...")
        processor = GraphRAGOutputProcessor(self.output_dir)
        graph_data = processor.load_graphrag_artifacts()
        
        return graph_data 