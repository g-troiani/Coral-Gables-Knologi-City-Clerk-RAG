"""
Microsoft GraphRAG integration for City Clerk document processing.

This package provides components for integrating Microsoft GraphRAG with
the existing city clerk document processing pipeline.
"""

from .graphrag_initializer import GraphRAGInitializer
from .document_adapter import CityClerkDocumentAdapter
from .prompt_tuner import CityClerkPromptTuner
from .graphrag_pipeline import CityClerkGraphRAGPipeline
from .cosmos_synchronizer import GraphRAGCosmosSync
from .query_engine import CityClerkGraphRAGQuery, QueryType, CityClerkQueryEngine, handle_user_query
from .query_router import SmartQueryRouter, QueryIntent, QueryFocus
from .incremental_processor import IncrementalGraphRAGProcessor
from .graphrag_output_processor import GraphRAGOutputProcessor
from .entity_deduplicator import AdvancedEntityDeduplicator
from .enhanced_entity_deduplicator import EnhancedEntityDeduplicator

__all__ = [
    'GraphRAGInitializer',
    'CityClerkDocumentAdapter',
    'CityClerkPromptTuner',
    'CityClerkGraphRAGPipeline',
    'GraphRAGCosmosSync',
    'CityClerkGraphRAGQuery',
    'CityClerkQueryEngine',
    'QueryType',
    'SmartQueryRouter',
    'QueryIntent',
    'QueryFocus',
    'handle_user_query',
    'IncrementalGraphRAGProcessor',
    'GraphRAGOutputProcessor',
    'AdvancedEntityDeduplicator',
    'EnhancedEntityDeduplicator'
] 