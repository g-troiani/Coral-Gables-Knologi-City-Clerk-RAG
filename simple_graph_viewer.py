#!/usr/bin/env python3
"""
Simple NetworkX Graph Viewer for Local City Clerk Graph
"""

import pickle
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path
import json

def load_and_visualize_graph():
    """Load and visualize the local NetworkX graph."""
    
    # Load the graph
    graph_path = Path("local_graph_data/city_clerk_graph.pkl")
    
    if not graph_path.exists():
        print("❌ Graph file not found. Please run the pipeline first.")
        return
    
    # Load graph
    with open(graph_path, 'rb') as f:
        graph = pickle.load(f)
    
    # Load and display stats
    stats_path = Path("local_graph_data/graph_stats.json")
    if stats_path.exists():
        with open(stats_path, 'r') as f:
            stats = json.load(f)
        
        print("📊 Graph Statistics:")
        print(f"   • Total Nodes: {stats['total_nodes']}")
        print(f"   • Total Edges: {stats['total_edges']}")
        print(f"   • Documents: {stats['documents']}")
        print(f"   • Node Types: {stats['node_types']}")
        print()
    
    # Print node information
    print("📄 Document Nodes:")
    for i, (node_id, node_data) in enumerate(graph.nodes(data=True), 1):
        title = node_data.get('title', 'Unknown')
        doc_type = node_data.get('document_type', 'Unknown')
        source = node_data.get('source_file', 'Unknown')
        print(f"   {i:2d}. {title[:50]}... ({doc_type}) - {source}")
    
    print()
    
    # Create visualization
    print("🎨 Creating visualization...")
    
    # Set up the plot
    plt.figure(figsize=(15, 10))
    plt.title("City Clerk Document Graph", fontsize=16, fontweight='bold')
    
    # Use a layout that spreads nodes well
    if graph.number_of_nodes() > 0:
        # For disconnected nodes, use a grid-like layout
        pos = {}
        nodes = list(graph.nodes())
        
        # Calculate grid dimensions
        import math
        cols = math.ceil(math.sqrt(len(nodes)))
        rows = math.ceil(len(nodes) / cols)
        
        for i, node in enumerate(nodes):
            row = i // cols
            col = i % cols
            pos[node] = (col, rows - row)  # Flip y to have origin at bottom-left
    
    # Draw nodes with different colors based on document type
    node_colors = []
    node_sizes = []
    
    for node_id, node_data in graph.nodes(data=True):
        doc_type = node_data.get('document_type', 'document')
        
        # Color by document type
        if 'agenda' in doc_type.lower():
            node_colors.append('#0EA5E9')  # Blue for agenda
            node_sizes.append(1000)
        elif 'ordinance' in doc_type.lower():
            node_colors.append('#EF4444')  # Red for ordinances
            node_sizes.append(800)
        elif 'resolution' in doc_type.lower():
            node_colors.append('#10B981')  # Green for resolutions
            node_sizes.append(800)
        elif 'transcript' in doc_type.lower():
            node_colors.append('#F59E0B')  # Orange for transcripts
            node_sizes.append(600)
        else:
            node_colors.append('#6B7280')  # Gray for other
            node_sizes.append(600)
    
    # Draw the graph
    nx.draw_networkx_nodes(graph, pos, node_color=node_colors, node_size=node_sizes, alpha=0.8)
    nx.draw_networkx_edges(graph, pos, alpha=0.5, edge_color='gray')
    
    # Add labels (shortened)
    labels = {}
    for node_id, node_data in graph.nodes(data=True):
        source_file = node_data.get('source_file', str(node_id))
        # Shorten filename for display
        if len(source_file) > 20:
            labels[node_id] = source_file[:17] + "..."
        else:
            labels[node_id] = source_file
    
    nx.draw_networkx_labels(graph, pos, labels, font_size=8)
    
    # Add legend
    legend_elements = [
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#0EA5E9', markersize=15, label='Agenda'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#EF4444', markersize=15, label='Ordinance'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#10B981', markersize=15, label='Resolution'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#F59E0B', markersize=15, label='Transcript'),
        plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='#6B7280', markersize=15, label='Other')
    ]
    plt.legend(handles=legend_elements, loc='upper right', bbox_to_anchor=(1.15, 1))
    
    plt.axis('off')
    plt.tight_layout()
    
    # Save the plot
    output_path = "local_graph_data/graph_visualization.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"💾 Visualization saved to: {output_path}")
    
    # Show the plot
    print("🖼️ Displaying graph visualization...")
    plt.show()

if __name__ == "__main__":
    print("🎨 Simple NetworkX Graph Viewer")
    print("=" * 40)
    load_and_visualize_graph() 