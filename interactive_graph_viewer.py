#!/usr/bin/env python3
"""
City Clerk Document Graph Visualizer - Interactive Cytoscape Version
"""

import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from collections import Counter
import dash
from dash import dcc, html, Input, Output
import dash_cytoscape as cyto
import networkx as nx

cyto.load_extra_layouts()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class GraphRAGVisualizer:
    """Interactive visualizer for GraphRAG knowledge graphs."""
    
    def __init__(self, graph_path: str = "local_graph_data/city_clerk_graph.graphml"):
        self.graph_path = Path(graph_path)
        self.graph = None
        self.graph_data = {"nodes": [], "edges": []}
        
        self.load_graph_data()
    
    def load_graph_data(self):
        """Load NetworkX graph from GraphML file."""
        try:
            full_graph = nx.read_graphml(str(self.graph_path))
            
            # Filter to only show connected nodes
            connected_nodes = set()
            for src, dst in full_graph.edges():
                connected_nodes.add(src)
                connected_nodes.add(dst)
            
            self.graph = full_graph.subgraph(connected_nodes).copy()
            log.info(f"✅ Loaded graph: {self.graph.number_of_nodes()} nodes, {self.graph.number_of_edges()} edges")
            
            self.convert_to_graph_data()
            
        except Exception as e:
            log.error(f"❌ Failed to load graph: {e}")
            self.graph = nx.DiGraph()  # Empty graph as fallback
    
    def convert_to_graph_data(self):
        """Convert NetworkX graph to standard format."""
        # Calculate connection counts for each node
        connection_counts = {}
        for src, dst in self.graph.edges():
            connection_counts[src] = connection_counts.get(src, 0) + 1
            connection_counts[dst] = connection_counts.get(dst, 0) + 1
        
        # Convert nodes
        nodes = []
        for node_id, attrs in self.graph.nodes(data=True):
            connections = connection_counts.get(node_id, 0)
            display_name = self._get_display_name(node_id, attrs)
            
            node_data = {
                "id": node_id,
                "label": display_name,
                "type": attrs.get("type", "OTHER").upper(),
                "connections": connections,
                "title": attrs.get("title", ""),
                "description": attrs.get("description", "")[:200] + "..." if attrs.get("description", "") else "",
                **attrs
            }
            nodes.append(node_data)
        
        # Convert edges
        edges = []
        for i, (src, dst, attrs) in enumerate(self.graph.edges(data=True)):
            relationship_type = attrs.get("relationship", "RELATED")
            kind = attrs.get("kind", "OTHER")
            
            edges.append({
                "id": f"e{i}",
                "source": src,
                "target": dst,
                "label": relationship_type,  # Use actual relationship type for label
                "kind": kind,  # Keep kind for styling
                "relationship": relationship_type,
                "order": attrs.get("order", ""),
                "sequence": attrs.get("sequence", ""),
                "role": attrs.get("role", ""),
                "description": f"{relationship_type} ({kind})"
            })
        
        self.graph_data = {"nodes": nodes, "edges": edges}
        log.info(f"Converted to graph data: {len(nodes)} nodes, {len(edges)} edges")
    
    def _get_display_name(self, node_id: str, attrs: Dict) -> str:
        """Get display name for nodes based on type."""
        node_type = attrs.get("type", "").upper()
        title = attrs.get("title", "")
        
        if node_type == "MEETING":
            return f"📅 Meeting\n{title}"
        elif node_type == "AGENDA_ITEM":
            return f"📋 {title}"
        elif node_type == "ORDINANCE":
            return f"⚖️ {title}"
        elif node_type == "RESOLUTION":
            return f"📜 {title}"
        elif node_type == "PERSON":
            return f"👤 {title}"
        elif node_type == "ORGANIZATION":
            return f"🏢 {title[:30]}"
        elif node_type == "PROJECT":
            return f"🔧 {title[:30]}"
        elif node_type == "DOCUMENT_NUMBER":
            return f"📄 {title}"
        elif node_type == "CROSS_REFERENCE":
            return f"🔗 {title[:25]}"
        elif node_type == "MONEY":
            return f"💰 {title}"
        else:
            return f"{title[:30]}" if title else node_id[:20]
    
    def get_cytoscape_elements(self):
        """Convert to Cytoscape format with connection-based sizing."""
        elements = []
        
        # Calculate size scaling
        connections = [node["connections"] for node in self.graph_data["nodes"]]
        if connections:
            min_conn = min(connections)
            max_conn = max(connections)
        else:
            min_conn = max_conn = 0
        
        # Add nodes with connection-based sizing
        for node in self.graph_data["nodes"]:
            connections = node["connections"]
            
            # Calculate size ratio (0-1)
            if max_conn > min_conn:
                size_ratio = (connections - min_conn) / (max_conn - min_conn)
            else:
                size_ratio = 0.5
            
            # Enhanced node data
            enhanced_node = node.copy()
            enhanced_node['size_ratio'] = size_ratio
            
            elements.append({'data': enhanced_node})
        
        # Add edges
        for edge in self.graph_data["edges"]:
            elements.append({'data': edge})
        
        return elements
    
    def get_node_type_counts(self):
        """Get counts by node type for statistics."""
        type_counts = Counter()
        for node in self.graph_data["nodes"]:
            type_counts[node["type"]] += 1
        return dict(type_counts)


# Initialize visualizer
visualizer = GraphRAGVisualizer()

# Create app
app = dash.Dash(__name__)

# Create legend
legend = html.Div([
    html.H4("📊 Node Types", style={'marginBottom': '15px', 'color': '#1f2937'}),
    html.Div([
        # Meetings
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#0EA5E9', 'borderRadius': '4px',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Meeting", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Agenda Items
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#F59E0B', 'borderRadius': '50%',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Agenda Item", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Ordinances
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#DC143C', 'borderRadius': '3px',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Ordinance", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Resolutions
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#7C2D12', 'borderRadius': '3px',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Resolution", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # People
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#EF4444', 'transform': 'rotate(45deg)',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Person", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Organizations
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#10B981',
                'clipPath': 'polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%)',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Organization", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Projects
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#8B5CF6', 'borderRadius': '3px',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Project", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'}),
        
        # Other types
        html.Div([
            html.Div(style={
                'width': '20px', 'height': '20px', 
                'backgroundColor': '#6B7280', 'borderRadius': '50%',
                'display': 'inline-block', 'marginRight': '10px'
            }),
            html.Span("Other", style={'verticalAlign': 'top', 'fontWeight': '500'})
        ], style={'marginBottom': '8px'})
    ]),
    
    html.H4("🔗 Relationships", style={'marginTop': '20px', 'marginBottom': '15px', 'color': '#1f2937'}),
    html.Div([
        # HAS_SECTION
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '3px', 
                'backgroundColor': '#3B82F6',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("HAS_SECTION", style={'fontSize': '11px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
        
        # CONTAINS_ITEM
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '3px', 
                'backgroundColor': '#F59E0B',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("CONTAINS_ITEM", style={'fontSize': '11px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
        
        # FOLLOWS
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '3px', 
                'backgroundColor': '#10B981', 'borderStyle': 'dashed',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("FOLLOWS", style={'fontSize': '11px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
        
        # REFERENCES_DOCUMENT
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '3px', 
                'backgroundColor': '#DC2626',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("REFERENCES_DOCUMENT", style={'fontSize': '10px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
        
        # HAS_TRANSCRIPT
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '3px', 
                'backgroundColor': '#7C3AED',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("HAS_TRANSCRIPT", style={'fontSize': '11px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
        
        # ATTENDED
        html.Div([
            html.Div(style={
                'width': '25px', 'height': '2px', 
                'backgroundColor': '#8B5CF6',
                'display': 'inline-block', 'marginRight': '8px',
                'verticalAlign': 'middle'
            }),
            html.Span("ATTENDED", style={'fontSize': '11px', 'fontWeight': '500'})
        ], style={'marginBottom': '6px'}),
    ])
], style={
    'position': 'absolute', 'top': '90px', 'right': '20px',
    'backgroundColor': 'white', 'padding': '20px',
    'border': '1px solid #e5e7eb', 'borderRadius': '8px',
    'boxShadow': '0 4px 6px -1px rgba(0, 0, 0, 0.1)',
    'zIndex': 1000, 'maxWidth': '220px'
})

# Get statistics for display
node_counts = visualizer.get_node_type_counts()
total_nodes = sum(node_counts.values())
total_edges = len(visualizer.graph_data["edges"])

app.layout = html.Div([
    html.H1("🏛️ City Clerk Document Knowledge Graph", 
            style={'textAlign': 'center', 'marginBottom': '10px', 'color': '#1f2937'}),
    
    html.P(f"📊 {total_nodes} nodes • {total_edges} relationships", 
           style={'textAlign': 'center', 'color': '#6b7280', 'marginBottom': '20px'}),
    
    html.Div([
        dcc.Dropdown(
            id='layout-selector',
            options=[
                {'label': '🌊 Hierarchical Flow', 'value': 'breadthfirst'},
                {'label': '🎯 Concentric', 'value': 'concentric'},
                {'label': '⚡ Force-Directed', 'value': 'cose'},
                {'label': '🌀 Circular', 'value': 'circle'},
                {'label': '📐 Grid', 'value': 'grid'},
                {'label': '🕸️ Web (Dagre)', 'value': 'dagre'}
            ],
            value='breadthfirst',
            style={'width': '300px', 'display': 'inline-block', 'marginRight': '15px'}
        ),
        html.Button('🔄 Refresh Layout', id='refresh-btn', 
                   style={'backgroundColor': '#3b82f6', 'color': 'white', 
                          'border': 'none', 'padding': '8px 16px', 'borderRadius': '6px'})
    ], style={'padding': '20px', 'textAlign': 'center'}),
    
    html.Div([
        html.Div([
            cyto.Cytoscape(
                id='cytoscape',
                elements=visualizer.get_cytoscape_elements(),
                style={'width': '100%', 'height': '700px', 'border': '1px solid #e5e7eb', 'borderRadius': '8px'},
                layout={'name': 'breadthfirst', 'directed': True, 'spacingFactor': 2.0},
                stylesheet=[
                    # Meeting nodes - Large blue rectangles
                    {
                        'selector': 'node[type="MEETING"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 120, 200)',
                            'height': 'mapData(size_ratio, 0, 1, 80, 130)',
                            'background-color': '#0EA5E9',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '12px',
                            'font-weight': 'bold',
                            'text-wrap': 'wrap',
                            'text-max-width': 'mapData(size_ratio, 0, 1, 100, 180)',
                            'shape': 'round-rectangle',
                            'border-width': '3px',
                            'border-color': '#0284C7'
                        }
                    },
                    # Agenda items - Amber circles
                    {
                        'selector': 'node[type="AGENDA_ITEM"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 80, 140)',
                            'height': 'mapData(size_ratio, 0, 1, 80, 140)',
                            'background-color': '#F59E0B',
                            'color': '#000000',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '10px',
                            'font-weight': 'bold',
                            'text-wrap': 'wrap',
                            'text-max-width': 'mapData(size_ratio, 0, 1, 70, 120)',
                            'shape': 'ellipse',
                            'border-width': '2px',
                            'border-color': '#D97706'
                        }
                    },
                    # Ordinances - Red rectangles
                    {
                        'selector': 'node[type="ORDINANCE"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 100, 160)',
                            'height': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'background-color': '#DC143C',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '10px',
                            'font-weight': 'bold',
                            'text-wrap': 'wrap',
                            'shape': 'round-rectangle',
                            'border-width': '2px',
                            'border-color': '#B91C1C'
                        }
                    },
                    # Resolutions - Dark red rectangles
                    {
                        'selector': 'node[type="RESOLUTION"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 100, 160)',
                            'height': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'background-color': '#7C2D12',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '10px',
                            'font-weight': 'bold',
                            'text-wrap': 'wrap',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Person nodes - Red diamonds
                    {
                        'selector': 'node[type="PERSON"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 70, 120)',
                            'height': 'mapData(size_ratio, 0, 1, 70, 120)',
                            'background-color': '#EF4444',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '9px',
                            'text-wrap': 'wrap',
                            'shape': 'diamond'
                        }
                    },
                    # Organization nodes - Green hexagons
                    {
                        'selector': 'node[type="ORGANIZATION"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 80, 130)',
                            'height': 'mapData(size_ratio, 0, 1, 80, 130)',
                            'background-color': '#10B981',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '9px',
                            'text-wrap': 'wrap',
                            'shape': 'hexagon'
                        }
                    },
                    # Project nodes - Purple rectangles
                    {
                        'selector': 'node[type="PROJECT"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 70, 120)',
                            'height': 'mapData(size_ratio, 0, 1, 50, 80)',
                            'background-color': '#8B5CF6',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '9px',
                            'text-wrap': 'wrap',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Default/Other nodes - Gray circles
                    {
                        'selector': 'node',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'height': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'background-color': '#6B7280',
                            'color': '#FFFFFF',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'font-size': '8px',
                            'text-wrap': 'wrap',
                            'shape': 'ellipse'
                        }
                    },
                    # Specific relationship styles
                    {
                        'selector': 'edge[relationship="HAS_SECTION"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 4,
                            'line-color': '#3B82F6',
                            'target-arrow-color': '#3B82F6',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'font-size': '10px',
                            'font-weight': 'bold',
                            'color': '#1E40AF',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -10
                        }
                    },
                    {
                        'selector': 'edge[relationship="CONTAINS_ITEM"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 3,
                            'line-color': '#F59E0B',
                            'target-arrow-color': '#F59E0B',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'font-size': '9px',
                            'font-weight': 'bold',
                            'color': '#D97706',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -8
                        }
                    },
                    {
                        'selector': 'edge[relationship="FOLLOWS"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#10B981',
                            'target-arrow-color': '#10B981',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'line-style': 'dashed',
                            'font-size': '8px',
                            'color': '#059669',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    },
                    {
                        'selector': 'edge[relationship="REFERENCES_DOCUMENT"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 3,
                            'line-color': '#DC2626',
                            'target-arrow-color': '#DC2626',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'font-size': '9px',
                            'color': '#B91C1C',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -8
                        }
                    },
                    {
                        'selector': 'edge[relationship="HAS_TRANSCRIPT"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#7C3AED',
                            'target-arrow-color': '#7C3AED',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'font-size': '8px',
                            'color': '#6D28D9',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    },
                    {
                        'selector': 'edge[relationship="SPONSORS"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#EF4444',
                            'target-arrow-color': '#EF4444',
                            'target-arrow-shape': 'vee',
                            'curve-style': 'bezier',
                            'line-style': 'dotted',
                            'font-size': '8px',
                            'color': '#DC2626',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    },
                    {
                        'selector': 'edge[relationship="ATTENDED"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#8B5CF6',
                            'target-arrow-color': '#8B5CF6',
                            'target-arrow-shape': 'vee',
                            'curve-style': 'bezier',
                            'font-size': '8px',
                            'color': '#7C3AED',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    },
                    # Edge styles by kind (fallback)
                    {
                        'selector': 'edge[kind="STRUCTURAL"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 3,
                            'line-color': '#3B82F6',
                            'target-arrow-color': '#3B82F6',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'font-size': '9px',
                            'color': '#1E40AF',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -8
                        }
                    },
                    {
                        'selector': 'edge[kind="ENTITY"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#F97316',
                            'target-arrow-color': '#F97316',
                            'target-arrow-shape': 'vee',
                            'curve-style': 'bezier',
                            'line-style': 'dotted',
                            'font-size': '8px',
                            'color': '#EA580C',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    },
                    # Default edge style
                    {
                        'selector': 'edge',
                        'style': {
                            'content': 'data(label)',
                            'width': 2,
                            'line-color': '#6B7280',
                            'target-arrow-color': '#6B7280',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'opacity': 0.7,
                            'font-size': '8px',
                            'color': '#374151',
                            'text-rotation': 'autorotate',
                            'text-margin-y': -6
                        }
                    }
                ],
                wheelSensitivity=0.1
            ),
            legend
        ], style={'position': 'relative'}),
    ]),
    
    # Node details panel
    html.Div(id='node-info', style={
        'padding': '25px',
        'backgroundColor': '#f9fafb',
        'marginTop': '25px',
        'borderRadius': '8px',
        'border': '1px solid #e5e7eb'
    })
])

@app.callback(
    Output('cytoscape', 'layout'),
    [Input('layout-selector', 'value'),
     Input('refresh-btn', 'n_clicks')]
)
def update_layout(layout_name, n_clicks):
    layouts = {
        'breadthfirst': {'name': 'breadthfirst', 'directed': True, 'spacingFactor': 2.0},
        'concentric': {'name': 'concentric', 'minNodeSpacing': 100},
        'cose': {'name': 'cose', 'animate': True, 'idealEdgeLength': 150},
        'circle': {'name': 'circle', 'radius': 300},
        'grid': {'name': 'grid', 'rows': 8},
        'dagre': {'name': 'dagre', 'rankDir': 'TB', 'rankSep': 100}
    }
    return layouts.get(layout_name, layouts['breadthfirst'])

@app.callback(
    Output('node-info', 'children'),
    [Input('cytoscape', 'tapNodeData'),
     Input('cytoscape', 'tapEdgeData')]
)
def show_element_details(node_data, edge_data):
    # Prioritize edge data if both are present
    if edge_data:
        return html.Div([
            html.H3("🔗 Relationship Details", style={'color': '#1f2937', 'marginBottom': '15px'}),
            
            # Relationship type badge
            html.Div([
                html.Span(edge_data.get('relationship', 'UNKNOWN'), style={
                    'backgroundColor': '#3B82F6',
                    'color': 'white',
                    'padding': '4px 12px',
                    'borderRadius': '20px',
                    'fontSize': '12px',
                    'fontWeight': 'bold',
                    'marginRight': '10px'
                }),
                html.Span(f"({edge_data.get('kind', 'unknown')})", style={
                    'color': '#6b7280',
                    'fontSize': '14px'
                })
            ], style={'marginBottom': '15px'}),
            
            # Connection info
            html.Div([
                html.Strong("Source: ", style={'color': '#374151'}),
                html.Span(edge_data.get('source', 'Unknown'), style={'color': '#6b7280', 'fontFamily': 'monospace'})
            ], style={'marginBottom': '8px'}),
            
            html.Div([
                html.Strong("Target: ", style={'color': '#374151'}),
                html.Span(edge_data.get('target', 'Unknown'), style={'color': '#6b7280', 'fontFamily': 'monospace'})
            ], style={'marginBottom': '8px'}),
            
            # Additional edge properties
            html.Hr(style={'margin': '15px 0', 'borderColor': '#e5e7eb'}),
            html.H5("Properties", style={'color': '#1f2937', 'marginBottom': '10px'}),
            
            # Show order if present
            *([html.Div([
                html.Strong("Order: ", style={'color': '#374151'}),
                html.Span(str(edge_data.get('order')), style={'color': '#6b7280'})
            ], style={'marginBottom': '8px'})] if edge_data.get('order', '') != '' else []),
            
            # Show sequence if present
            *([html.Div([
                html.Strong("Sequence: ", style={'color': '#374151'}),
                html.Span(str(edge_data.get('sequence')), style={'color': '#6b7280'})
            ], style={'marginBottom': '8px'})] if edge_data.get('sequence', '') != '' else []),
            
            # Show role if present
            *([html.Div([
                html.Strong("Role: ", style={'color': '#374151'}),
                html.Span(edge_data.get('role'), style={'color': '#6b7280'})
            ], style={'marginBottom': '8px'})] if edge_data.get('role') else []),
            
            html.P(edge_data.get('description', 'No description available'), 
                   style={'color': '#4b5563', 'marginTop': '10px', 'fontStyle': 'italic'})
        ])
    
    elif node_data:
        node_type = node_data.get('type', 'Unknown')
        connections = node_data.get('connections', 0)
        title = node_data.get('title', 'Unknown')
        description = node_data.get('description', '')
        
        details = [
            html.H3([
                f"📋 {node_type.replace('_', ' ').title()} Details"
            ], style={'color': '#1f2937'}),
            
            html.H4(title, style={'color': '#3b82f6', 'marginBottom': '15px'}),
            
            html.Div([
                html.Span("🔗 Connections: ", style={'fontWeight': 'bold'}),
                html.Span(f"{connections}", style={'color': '#059669', 'fontWeight': 'bold'})
            ], style={'backgroundColor': '#ecfdf5', 'padding': '10px', 'borderRadius': '6px', 'marginBottom': '15px'}),
        ]
        
        # Add description if available
        if description:
            details.append(html.Div([
                html.H5("Description:", style={'marginBottom': '8px', 'color': '#374151'}),
                html.P(description, style={'color': '#6b7280', 'lineHeight': '1.6'})
            ], style={'marginBottom': '15px'}))
        
        # Add other properties
        skip_props = {'id', 'label', 'type', 'connections', 'size_ratio', 'title', 'description'}
        for key, value in node_data.items():
            if key not in skip_props and value and str(value).strip():
                display_value = str(value)
                if len(display_value) > 200:
                    display_value = display_value[:200] + "..."
                
                details.append(html.Div([
                    html.Strong(f"{key.replace('_', ' ').title()}: ", style={'color': '#374151'}),
                    html.Span(display_value, style={'color': '#6b7280'})
                ], style={'marginBottom': '8px'}))
        
        return html.Div(details)
    
    else:
        return html.Div([
            html.H3("📋 Element Details", style={'color': '#1f2937'}),
            html.P("Click on a node or edge to see its details", 
                   style={'color': '#6b7280', 'fontStyle': 'italic'})
        ])


if __name__ == '__main__':
    print("\n🎨 Interactive Graph Visualizer Features:")
    print("  - 🌈 Color-coded node types with unique shapes")
    print("  - 📏 Dynamic node sizing based on connections")
    print("  - 🎛️ Multiple layout algorithms")
    print("  - 🔍 Interactive node details")
    print("  - 📊 Real-time statistics")
    print("\n🚀 Starting server...")
    
    app.run(debug=True, port=8053)
