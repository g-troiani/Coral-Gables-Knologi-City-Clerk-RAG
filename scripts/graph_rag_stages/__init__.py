"""
Unified City Clerk Knowledge Graph Pipeline Package

This package contains the modular components for:
1. Data pre-processing and extraction
2. Graph building (custom Cosmos DB and local NetworkX graphs)
3. NER-based query and response processing

Usage:
    python -m scripts.graph_rag_stages.main_pipeline
    
    Or import specific components:
    from scripts.graph_rag_stages.phase1_preprocessing import run_extraction_pipeline
    from scripts.graph_rag_stages.phase3_querying.ner import UnifiedQueryEngine
"""

__version__ = "1.0.0"
__author__ = "City Clerk Knowledge Graph Team" 