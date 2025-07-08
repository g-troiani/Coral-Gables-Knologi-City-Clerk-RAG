#!/usr/bin/env python3
"""
Script to explore the structure of the Cosmos database to understand
what vertices and labels exist.
"""

import asyncio
import logging
from collections import defaultdict, Counter
from typing import Dict, List
from scripts.graph_rag_stages.common.cosmos_client import CosmosGraphClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def explore_cosmos_structure():
    """Explore the structure of the Cosmos database."""
    
    cosmos_client = CosmosGraphClient()
    
    try:
        async with cosmos_client:
            # Get all vertex labels
            print("Getting vertex labels...")
            vertex_labels = await get_vertex_labels(cosmos_client)
            
            # Get edge labels
            print("Getting edge labels...")
            edge_labels = await get_edge_labels(cosmos_client)
            
            # Get sample vertices for each label
            print("Getting sample vertices...")
            sample_vertices = await get_sample_vertices(cosmos_client, vertex_labels)
            
            # Print results
            print_exploration_results(vertex_labels, edge_labels, sample_vertices)
            
    except Exception as e:
        log.error(f"Error exploring database: {e}")
        raise


async def get_vertex_labels(cosmos_client: CosmosGraphClient) -> Dict[str, int]:
    """Get all vertex labels and their counts."""
    try:
        # Get all vertex labels with counts
        query = "g.V().label().groupCount()"
        result = await cosmos_client._execute_query(query)
        
        if result:
            return result[0]
        else:
            return {}
            
    except Exception as e:
        log.error(f"Error getting vertex labels: {e}")
        return {}


async def get_edge_labels(cosmos_client: CosmosGraphClient) -> Dict[str, int]:
    """Get all edge labels and their counts."""
    try:
        # Get all edge labels with counts
        query = "g.E().label().groupCount()"
        result = await cosmos_client._execute_query(query)
        
        if result:
            return result[0]
        else:
            return {}
            
    except Exception as e:
        log.error(f"Error getting edge labels: {e}")
        return {}


async def get_sample_vertices(cosmos_client: CosmosGraphClient, vertex_labels: Dict[str, int]) -> Dict[str, List[Dict]]:
    """Get sample vertices for each label."""
    samples = {}
    
    for label in vertex_labels.keys():
        try:
            # Get up to 3 sample vertices of this label
            query = f"g.V().hasLabel('{label}').limit(3).valueMap(true)"
            result = await cosmos_client._execute_query(query)
            
            samples[label] = result or []
            
        except Exception as e:
            log.error(f"Error getting samples for label {label}: {e}")
            samples[label] = []
    
    return samples


def print_exploration_results(vertex_labels: Dict[str, int], edge_labels: Dict[str, int], sample_vertices: Dict[str, List[Dict]]) -> None:
    """Print the exploration results."""
    print("\n" + "="*60)
    print("COSMOS DATABASE STRUCTURE EXPLORATION")
    print("="*60)
    
    # Vertex labels
    print(f"\nVERTEX LABELS:")
    total_vertices = sum(vertex_labels.values())
    print(f"  Total vertices: {total_vertices}")
    
    for label, count in sorted(vertex_labels.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_vertices) * 100 if total_vertices > 0 else 0
        print(f"  - {label}: {count} ({percentage:.1f}%)")
    
    # Edge labels
    print(f"\nEDGE LABELS:")
    total_edges = sum(edge_labels.values())
    print(f"  Total edges: {total_edges}")
    
    for label, count in sorted(edge_labels.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_edges) * 100 if total_edges > 0 else 0
        print(f"  - {label}: {count} ({percentage:.1f}%)")
    
    # Sample vertices
    print(f"\nSAMPLE VERTICES:")
    
    for label, samples in sample_vertices.items():
        print(f"\n  {label.upper()} (showing up to 3 samples):")
        
        if not samples:
            print("    No samples found")
            continue
            
        for i, sample in enumerate(samples[:3], 1):
            print(f"    Sample {i}:")
            # Show key properties
            vertex_id = sample.get('id', 'unknown')
            print(f"      ID: {vertex_id}")
            
            # Show other interesting properties
            for prop, value in sample.items():
                if prop == 'id':
                    continue
                    
                # Handle list values
                if isinstance(value, list):
                    if len(value) == 1:
                        display_value = value[0]
                    else:
                        display_value = f"[{len(value)} items]"
                else:
                    display_value = value
                
                # Truncate long values
                if isinstance(display_value, str) and len(display_value) > 100:
                    display_value = display_value[:100] + "..."
                    
                print(f"      {prop}: {display_value}")
            
            print()  # Empty line between samples
    
    # Analysis for document types
    print(f"\nDOCUMENT TYPE ANALYSIS:")
    document_related_labels = [label for label in vertex_labels.keys() if 'document' in label.lower() or label.lower() in ['agenda', 'transcript', 'ordinance', 'resolution']]
    
    if document_related_labels:
        print("  Document-related labels found:")
        for label in document_related_labels:
            print(f"    - {label}: {vertex_labels[label]} vertices")
    else:
        print("  No obvious document-related labels found")
        
    # Check for properties that might indicate document types
    print(f"\nPROPERTIES ANALYSIS:")
    properties_with_doc_types = defaultdict(set)
    
    for label, samples in sample_vertices.items():
        for sample in samples:
            for prop, value in sample.items():
                if prop == 'id':
                    continue
                    
                if isinstance(value, list):
                    for v in value:
                        if isinstance(v, str) and any(doc_type in str(v).upper() for doc_type in ['AGENDA', 'TRANSCRIPT', 'ORDINANCE', 'RESOLUTION']):
                            properties_with_doc_types[prop].add(str(v))
                else:
                    if isinstance(value, str) and any(doc_type in str(value).upper() for doc_type in ['AGENDA', 'TRANSCRIPT', 'ORDINANCE', 'RESOLUTION']):
                        properties_with_doc_types[prop].add(str(value))
    
    if properties_with_doc_types:
        print("  Properties containing document type keywords:")
        for prop, values in properties_with_doc_types.items():
            print(f"    - {prop}: {list(values)[:5]}...")  # Show first 5 values
    else:
        print("  No properties with document type keywords found")


if __name__ == "__main__":
    asyncio.run(explore_cosmos_structure()) 