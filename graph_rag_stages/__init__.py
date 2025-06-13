"""
Unified City Clerk GraphRAG Pipeline Package

This package contains the modular components for:
1. Data pre-processing and extraction
2. Graph building (both custom graph and GraphRAG indexing)
3. Query and response processing

Usage:
    python -m graph_rag_stages.main_pipeline
    
    Or import specific components:
    from graph_rag_stages.data_preprocessing import PDFExtractor
    from graph_rag_stages.query_and_response import QueryEngine
"""

__version__ = "1.0.0"
__author__ = "City Clerk Knowledge Graph Team" 