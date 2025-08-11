"""
Graph Building Module

This module handles graph building approaches:
1. Custom graph building in Cosmos DB (cloud-based)
2. Local graph building with NetworkX (no cloud dependencies)

Updated to read from JSON extraction output instead of markdown.

Components:
- Custom graph builder for Cosmos DB
- Local graph builder for NetworkX
- Entity deduplication for enhanced results
"""

from .custom_graph_builder import CustomGraphBuilder
from .entity_deduplicator import EntityDeduplicator

__all__ = [
    'CustomGraphBuilder',
    'EntityDeduplicator'
]