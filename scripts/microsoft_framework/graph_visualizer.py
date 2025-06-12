#!/usr/bin/env python3
"""
GraphRAG Output Visualizer
Visualizes the knowledge graph extracted by Microsoft GraphRAG
"""

import pandas as pd
import plotly.graph_objects as go
import networkx as nx
from pathlib import Path
from typing import Dict, List, Tuple
import json

class GraphRAGVisualizer:
    """Visualize GraphRAG knowledge graph output."""
    
    def __init__(self, graphrag_output_dir: Path):
        self.output_dir = Path(graphrag_output_dir)
        self.entities = None
        self.relationships = None
        self.graph = nx.Graph()
        
    def load_graph_data(self):
        """Load entities and relationships from GraphRAG output."""
        # Load entities
        entities_path = self.output_dir / "entities.parquet"
        if entities_path.exists():
            self.entities = pd.read_parquet(entities_path)
            print(f"✅ Loaded {len(self.entities)} entities")
        else:
            raise FileNotFoundError(f"Entities file not found: {entities_path}")
            
        # Load relationships
        relationships_path = self.output_dir / "relationships.parquet"
        if relationships_path.exists():
            self.relationships = pd.read_parquet(relationships_path)
            print(f"✅ Loaded {len(self.relationships)} relationships")
        else:
            print("⚠️ No relationships file found")
            self.relationships = pd.DataFrame()
    
    def build_networkx_graph(self, entity_types: List[str] = None, limit: int = None):
        """Build NetworkX graph from GraphRAG data."""
        self.graph.clear()
        
        # Filter entities by type if specified
        entities_to_add = self.entities
        if entity_types:
            entities_to_add = self.entities[self.entities['type'].isin(entity_types)]
        
        # Limit entities if specified
        if limit:
            entities_to_add = entities_to_add.head(limit)
        
        # Create a mapping from entity title to DataFrame index for the filtered entities
        title_to_index = {}
        
        # Add nodes using entity titles as node IDs
        for idx, entity in entities_to_add.iterrows():
            node_id = entity['title']  # Use title as node ID
            title_to_index[node_id] = idx
            
            self.graph.add_node(
                node_id,
                title=entity['title'],
                type=entity['type'],
                description=entity.get('description', '')[:200],
                original_index=idx
            )
        
        # Add edges using entity titles (which match relationship source/target)
        node_titles = set(self.graph.nodes())
        edges_added = 0
        
        for _, rel in self.relationships.iterrows():
            source_title = rel['source']
            target_title = rel['target']
            
            # Only add edge if both nodes exist in our filtered graph
            if source_title in node_titles and target_title in node_titles:
                self.graph.add_edge(
                    source_title,
                    target_title,
                    description=rel.get('description', ''),
                    weight=rel.get('weight', 1.0)
                )
                edges_added += 1
        
        print(f"📊 Graph built with {len(self.graph.nodes)} nodes and {len(self.graph.edges)} edges")
        print(f"   🔗 Successfully connected {edges_added} relationships")
    
    def create_plotly_figure(self) -> go.Figure:
        """Create interactive Plotly visualization."""
        if len(self.graph.nodes) == 0:
            print("⚠️ No nodes in graph to visualize")
            return go.Figure()
            
        # Get layout
        pos = nx.spring_layout(self.graph, k=2, iterations=50)
        
        # Create edge traces
        edge_traces = []
        for edge in self.graph.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_traces.append(go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode='lines',
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                showlegend=False
            ))
        
        # Create node trace
        node_x = []
        node_y = []
        node_text = []
        node_color = []
        node_hover_text = []
        
        # Color mapping for entity types
        type_colors = {
            'AGENDA_ITEM': 'red',
            'ORDINANCE': 'blue', 
            'RESOLUTION': 'green',
            'PERSON': 'orange',
            'ORGANIZATION': 'purple',
            'MEETING': 'brown',
            'MONEY': 'pink',
            'PROJECT': 'cyan',
            'DOCUMENT_NUMBER': 'yellow',
            'EVENT': 'magenta',
            'CROSS_REFERENCE': 'lightblue'
        }
        
        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            # Get node attributes
            attrs = self.graph.nodes[node]
            
            # Create hover text
            hover_text = f"<b>{attrs['title']}</b><br>"
            hover_text += f"Type: {attrs['type']}<br>"
            hover_text += f"Connections: {self.graph.degree(node)}<br>"
            if attrs['description']:
                hover_text += f"<br>{attrs['description']}"
            node_hover_text.append(hover_text)
            
            # Set color based on type
            node_color.append(type_colors.get(attrs['type'], 'gray'))
            
            # Add text labels (truncate long titles)
            title = attrs['title']
            if len(title) > 15:
                title = title[:12] + "..."
            node_text.append(title)
        
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode='markers+text',
            text=node_text,
            textposition="top center",
            textfont=dict(size=8),
            hovertext=node_hover_text,
            hoverinfo='text',
            marker=dict(
                size=15,
                color=node_color,
                line=dict(width=2, color='white')
            ),
            showlegend=False
        )
        
        # Create figure
        fig_data = edge_traces + [node_trace]
        fig = go.Figure(data=fig_data)
        
        fig.update_layout(
            title="GraphRAG Knowledge Graph Visualization",
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            height=800,
            plot_bgcolor='white'
        )
        
        return fig
    
    def save_visualization(self, output_path: Path = None):
        """Save visualization as HTML."""
        if not output_path:
            output_path = Path("graphrag_visualization.html")
            
        fig = self.create_plotly_figure()
        fig.write_html(str(output_path))
        print(f"💾 Visualization saved to: {output_path}")
    
    def get_entity_summary(self) -> Dict:
        """Get summary statistics about entities."""
        summary = {
            'total_entities': len(self.entities),
            'entity_types': self.entities['type'].value_counts().to_dict(),
            'total_relationships': len(self.relationships)
        }
        return summary

def visualize_graphrag_output(
    graphrag_root: Path = Path("graphrag_data"),
    entity_types: List[str] = None,
    limit: int = 100
):
    """Quick function to visualize GraphRAG output."""
    
    output_dir = graphrag_root / "output"
    
    # Initialize visualizer
    viz = GraphRAGVisualizer(output_dir)
    
    # Load data
    viz.load_graph_data()
    
    # Show summary
    summary = viz.get_entity_summary()
    print("\n📊 GraphRAG Summary:")
    print(f"   Total entities: {summary['total_entities']}")
    print(f"   Total relationships: {summary['total_relationships']}")
    print("\n   Entity types:")
    for entity_type, count in summary['entity_types'].items():
        print(f"     - {entity_type}: {count}")
    
    # Build graph
    print(f"\n🔨 Building graph (limit: {limit} entities)...")
    viz.build_networkx_graph(entity_types=entity_types, limit=limit)
    
    # Save visualization
    viz.save_visualization()
    
    return viz

if __name__ == "__main__":
    # Example: Visualize agenda items and ordinances
    viz = visualize_graphrag_output(
        entity_types=['AGENDA_ITEM', 'ORDINANCE', 'RESOLUTION'],
        limit=50
    ) 