#!/usr/bin/env python3
"""
FIXED City Clerk Document Graph Visualizer - Interactive Cytoscape Version
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

class FixedGraphRAGVisualizer:
    """Fixed interactive visualizer for GraphRAG knowledge graphs."""
    
    def __init__(self, graph_path: str = "local_graph_data/city_clerk_graph.graphml"):
        self.graph_path = Path(graph_path)
        self.graph = None
        self.graph_data = {"nodes": [], "edges": []}
        self.error_message = None
        
        # Load graph data with better error handling
        self._safe_load_graph_data()
    
    def _safe_load_graph_data(self):
        """Load NetworkX graph from GraphML file with comprehensive error handling."""
        try:
            if not self.graph_path.exists():
                self.error_message = f"Graph file not found: {self.graph_path}"
                log.error(f"❌ {self.error_message}")
                return
            
            log.info(f"📂 Loading graph from: {self.graph_path}")
            log.info(f"📏 File size: {self.graph_path.stat().st_size} bytes")
            
            # Load the full graph
            full_graph = nx.read_graphml(str(self.graph_path))
            log.info(f"📊 Raw graph loaded: {full_graph.number_of_nodes()} nodes, {full_graph.number_of_edges()} edges")
            
            # Option 1: Use ALL nodes (not just connected ones) 
            # This might be why nodes are missing!
            self.graph = full_graph.copy()
            
            # Option 2: If you want only connected nodes, use this instead:
            # connected_nodes = set()
            # for src, dst in full_graph.edges():
            #     connected_nodes.add(src)
            #     connected_nodes.add(dst)
            # self.graph = full_graph.subgraph(connected_nodes).copy()
            
            log.info(f"✅ Final graph: {self.graph.number_of_nodes()} nodes, {self.graph.number_of_edges()} edges")
            
            # Convert to display format
            self._convert_to_graph_data()
            
        except Exception as e:
            self.error_message = f"Failed to load graph: {str(e)}"
            log.error(f"❌ {self.error_message}")
            import traceback
            traceback.print_exc()
            
            # Create empty fallback graph
            self.graph = nx.DiGraph()
            self.graph_data = {"nodes": [], "edges": []}
    
    def _convert_to_graph_data(self):
        """Convert NetworkX graph to display format."""
        if not self.graph:
            log.warning("⚠️  No graph to convert")
            return
            
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
            
            # Create a copy of attrs without the original 'type' to avoid override
            attrs_copy = {k: v for k, v in attrs.items() if k != 'type'}
            
            node_data = {
                "id": node_id,
                "label": display_name,
                "type": attrs.get("type", "OTHER").upper(),
                "connections": connections,
                "title": attrs.get("title", ""),
                "description": attrs.get("description", "")[:200] + "..." if attrs.get("description", "") else "",
                **attrs_copy
            }
            nodes.append(node_data)
        
        # Convert edges
        edges = []
        for i, (src, dst, attrs) in enumerate(self.graph.edges(data=True)):
            relationship_type = attrs.get("relationship", "RELATED")
            
            edges.append({
                "id": f"e{i}",
                "source": src,
                "target": dst,
                "label": relationship_type,
                "relationship": relationship_type,
            })
        
        self.graph_data = {"nodes": nodes, "edges": edges}
        log.info(f"✅ Converted to display format: {len(nodes)} nodes, {len(edges)} edges")
    
    def _get_display_name(self, node_id: str, attrs: Dict) -> str:
        """Get display name for nodes based on type."""
        node_type = attrs.get("type", "").upper()
        title = attrs.get("title", "")
        
        if node_type == "MEETING":
            return f"📅 {title}"
        elif node_type == "AGENDA_ITEM":
            return f"📋 {title[:30]}"
        elif node_type == "PERSON":
            return f"👤 {title}"
        elif node_type == "ORGANIZATION":
            return f"🏢 {title[:25]}"
        elif node_type == "SECTION":
            return f"📂 {title[:25]}"
        elif node_type == "DOCUMENT":
            return f"📄 {title[:25]}"
        elif node_type == "VERBATIM_TRANSCRIPT":
            return f"🎤 {title[:25]}"
        elif node_type == "LEGAL_DOCUMENT":
            return f"⚖️ {title[:25]}"
        elif node_type == "RESOLUTION":
            return f"📜 {title[:25]}"
        elif node_type == "ORDINANCE":
            return f"📋 {title[:25]}"
        else:
            return f"{title[:25]}" if title else node_id[:20]
    
    def get_cytoscape_elements(self):
        """Convert to Cytoscape format."""
        if self.error_message:
            # Return error indicator elements
            return [
                {'data': {'id': 'error', 'label': 'Error Loading Graph'}},
                {'data': {'id': 'error_detail', 'label': self.error_message[:50]}}
            ]
        
        elements = []
        
        # Add nodes
        for node in self.graph_data["nodes"]:
            elements.append({'data': node})
        
        # Add edges
        for edge in self.graph_data["edges"]:
            elements.append({'data': edge})
        
        log.info(f"🎨 Created {len(elements)} Cytoscape elements")
        return elements
    
    def get_node_type_counts(self):
        """Get counts by node type for statistics."""
        if self.error_message:
            return {"ERROR": 1}
            
        type_counts = Counter()
        for node in self.graph_data["nodes"]:
            type_counts[node["type"]] += 1
        return dict(type_counts)

# Initialize visualizer
log.info("🚀 Initializing Fixed Graph Visualizer...")
visualizer = FixedGraphRAGVisualizer()

# Check if initialization was successful
if visualizer.error_message:
    log.error(f"❌ Visualizer initialization failed: {visualizer.error_message}")
else:
    log.info(f"✅ Visualizer initialized successfully with {len(visualizer.graph_data['nodes'])} nodes")

# Create app
app = dash.Dash(__name__)

# Get statistics for display
node_counts = visualizer.get_node_type_counts()
total_nodes = sum(node_counts.values()) if not visualizer.error_message else 0
total_edges = len(visualizer.graph_data["edges"]) if not visualizer.error_message else 0

app.layout = html.Div([
    html.H1("🔧 City Clerk Document Knowledge Graph", 
            style={'textAlign': 'center', 'marginBottom': '10px', 'color': '#1f2937'}),
    
    # Status indicator
    html.Div([
        html.P(f"📊 Status: {'ERROR' if visualizer.error_message else 'SUCCESS'}", 
               style={'textAlign': 'center', 'color': 'red' if visualizer.error_message else 'green', 'fontWeight': 'bold'}),
        html.P(f"📈 {total_nodes} nodes • {total_edges} relationships", 
               style={'textAlign': 'center', 'color': '#6b7280', 'marginBottom': '20px'}),
        html.P(f"🗂️ Node types: {', '.join(node_counts.keys())}", 
               style={'textAlign': 'center', 'color': '#6b7280', 'fontSize': '12px'})
    ]),
    
    # Error message if any
    html.Div([
        html.H3("❌ Error Details:", style={'color': 'red'}),
        html.P(visualizer.error_message, style={'color': 'red', 'fontFamily': 'monospace'})
    ] if visualizer.error_message else []),
    
    # Layout controls
    html.Div([
        dcc.Dropdown(
            id='layout-selector',
            options=[
                {'label': '🌊 Hierarchical Flow', 'value': 'breadthfirst'},
                {'label': '🎯 Concentric', 'value': 'concentric'},
                {'label': '⚡ Force-Directed', 'value': 'cose'},
                {'label': '🌀 Circular', 'value': 'circle'},
                {'label': '📐 Grid', 'value': 'grid'},
            ],
            value='breadthfirst',
            style={'width': '300px', 'display': 'inline-block', 'marginRight': '15px'}
        ),
        html.Button('🔄 Refresh Layout', id='refresh-btn', 
                   style={'backgroundColor': '#3b82f6', 'color': 'white', 
                          'border': 'none', 'padding': '8px 16px', 'borderRadius': '6px'})
    ], style={'padding': '20px', 'textAlign': 'center'}),
    
    # Main content area with graph and details side by side
    html.Div([
        # Graph display
        html.Div([
            cyto.Cytoscape(
                id='cytoscape',
                elements=visualizer.get_cytoscape_elements(),
                style={
                    'width': '100%', 
                    'height': '700px', 
                    'border': '2px solid #000' if visualizer.error_message else '1px solid #e5e7eb', 
                    'borderRadius': '8px',
                    'backgroundColor': '#ffe6e6' if visualizer.error_message else '#ffffff'
                },
                layout={'name': 'breadthfirst', 'directed': True, 'spacingFactor': 1.5},
                stylesheet=[
                    # Simple universal node style
                    {
                        'selector': 'node',
                        'style': {
                            'content': 'data(label)',
                            'background-color': '#3B82F6',
                            'color': 'white',
                            'text-valign': 'center',
                            'text-halign': 'center',
                            'width': '60px',
                            'height': '60px',
                            'font-size': '8px',
                            'text-wrap': 'wrap',
                            'text-max-width': '55px',
                            'border-width': '1px',
                            'border-color': '#1E40AF'
                        }
                    },
                    # Meeting nodes - Larger and blue
                    {
                        'selector': 'node[type="MEETING"]',
                        'style': {
                            'background-color': '#0EA5E9',
                            'width': '100px',
                            'height': '60px',
                            'shape': 'round-rectangle',
                            'font-size': '10px'
                        }
                    },
                    # Person nodes - Red diamonds
                    {
                        'selector': 'node[type="PERSON"]',
                        'style': {
                            'background-color': '#EF4444',
                            'shape': 'diamond'
                        }
                    },
                    # Organization nodes - Green
                    {
                        'selector': 'node[type="ORGANIZATION"]',
                        'style': {
                            'background-color': '#10B981',
                            'shape': 'hexagon'
                        }
                    },
                    # Agenda items - Yellow circles  
                    {
                        'selector': 'node[type="AGENDA_ITEM"]',
                        'style': {
                            'background-color': '#F59E0B',
                            'shape': 'ellipse'
                        }
                    },
                    # Sections - Purple rectangles
                    {
                        'selector': 'node[type="SECTION"]',
                        'style': {
                            'background-color': '#8B5CF6',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Documents - Gray rectangles
                    {
                        'selector': 'node[type="DOCUMENT"]',
                        'style': {
                            'background-color': '#6B7280',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Verbatim Transcripts - Pink/red diamonds
                    {
                        'selector': 'node[type="VERBATIM_TRANSCRIPT"]',
                        'style': {
                            'background-color': '#EC4899',
                            'shape': 'diamond',
                            'width': '70px',
                            'height': '70px'
                        }
                    },
                    # Legal Documents - Purple octagons
                    {
                        'selector': 'node[type="LEGAL_DOCUMENT"]',
                        'style': {
                            'background-color': '#9333EA',
                            'shape': 'octagon',
                            'width': '75px',
                            'height': '75px'
                        }
                    },
                    # Resolution nodes - Teal hexagons
                    {
                        'selector': 'node[type="RESOLUTION"]',
                        'style': {
                            'background-color': '#0891B2',
                            'shape': 'hexagon',
                            'width': '80px',
                            'height': '80px'
                        }
                    },
                    # Ordinance nodes - Indigo octagons
                    {
                        'selector': 'node[type="ORDINANCE"]',
                        'style': {
                            'background-color': '#4F46E5',
                            'shape': 'octagon',
                            'width': '85px',
                            'height': '85px'
                        }
                    },
                    # Simple edge style
                    {
                        'selector': 'edge',
                        'style': {
                            'width': 2,
                            'line-color': '#666',
                            'target-arrow-color': '#666',
                            'target-arrow-shape': 'triangle',
                            'curve-style': 'bezier',
                            'content': 'data(label)',
                            'font-size': '8px',
                            'color': '#333'
                        }
                    }
                ]
            )
        ], style={'width': '70%', 'display': 'inline-block', 'verticalAlign': 'top'}),
        
        # Node details panel
        html.Div([
            html.H3("🔍 Node Details", style={'marginBottom': '15px', 'color': '#1f2937'}),
            html.Div(id='node-info', children=[
                html.P("Click on a node to see its properties", 
                       style={'color': '#6b7280', 'fontStyle': 'italic'})
            ], style={
                'padding': '15px',
                'border': '1px solid #e5e7eb',
                'borderRadius': '8px',
                'backgroundColor': '#f9fafb',
                'maxHeight': '650px',
                'overflowY': 'auto'
            })
        ], style={
            'width': '28%', 
            'display': 'inline-block', 
            'verticalAlign': 'top', 
            'marginLeft': '2%'
        })
    ], style={'margin': '20px'})
])

@app.callback(
    Output('cytoscape', 'layout'),
    [Input('layout-selector', 'value'),
     Input('refresh-btn', 'n_clicks')]
)
def update_layout(layout_name, n_clicks):
    return {'name': layout_name, 'directed': True, 'spacingFactor': 1.5}

@app.callback(
    Output('node-info', 'children'),
    [Input('cytoscape', 'tapNodeData')]
)
def show_node_details(node_data):
    if not node_data:
        return [html.P("Click on a node to see its properties", 
                      style={'color': '#6b7280', 'fontStyle': 'italic'})]
    
    # Get node type for styling
    node_type = node_data.get('type', 'UNKNOWN')
    
    # Special handling for legal documents (ordinances and resolutions)
    if node_type in ['LEGAL_DOCUMENT', 'RESOLUTION', 'ORDINANCE']:
        return show_legal_document_details(node_data)
    
    # Type-specific emoji and color for other node types
    type_info = {
        'MEETING': {'emoji': '📅', 'color': '#0EA5E9'},
        'PERSON': {'emoji': '👤', 'color': '#EF4444'},
        'ORGANIZATION': {'emoji': '🏢', 'color': '#10B981'},
        'AGENDA_ITEM': {'emoji': '📋', 'color': '#F59E0B'},
        'SECTION': {'emoji': '📂', 'color': '#8B5CF6'},
        'DOCUMENT': {'emoji': '📄', 'color': '#6B7280'},
        'VERBATIM_TRANSCRIPT': {'emoji': '🎤', 'color': '#EC4899'},
        'LEGAL_DOCUMENT': {'emoji': '⚖️', 'color': '#9333EA'},
        'RESOLUTION': {'emoji': '📜', 'color': '#0891B2'},
        'ORDINANCE': {'emoji': '📋', 'color': '#4F46E5'},
    }
    
    info = type_info.get(node_type, {'emoji': '❓', 'color': '#6B7280'})
    
    # Build the details display for non-legal documents
    details = []
    
    # Header with type and title
    details.append(
        html.Div([
            html.H4(f"{info['emoji']} {node_type}", 
                    style={'color': info['color'], 'marginBottom': '5px'}),
            html.H5(node_data.get('title', node_data.get('id', 'No title')), 
                    style={'color': '#1f2937', 'marginTop': '0px', 'fontWeight': 'normal'})
        ])
    )
    
    # Key properties section
    key_props = ['id', 'connections', 'description']
    for prop in key_props:
        if prop in node_data and node_data[prop]:
            value = node_data[prop]
            # Format the property name nicely
            display_name = prop.replace('_', ' ').title()
            
            details.append(
                html.Div([
                    html.Strong(f"{display_name}: ", style={'color': '#374151'}),
                    html.Span(str(value), style={'color': '#6b7280'})
                ], style={'marginBottom': '8px'})
            )
    
    # All properties section
    details.append(html.Hr(style={'margin': '15px 0'}))
    details.append(html.H5("All Properties:", style={'color': '#1f2937', 'marginBottom': '10px'}))
    
    # Filter out already shown properties and internal ones
    excluded_props = {'id', 'label', 'connections', 'description', 'title', 'type'}
    
    for key, value in sorted(node_data.items()):
        if key not in excluded_props and value is not None:
            # Format the key nicely
            display_key = key.replace('_', ' ').title()
            
            # Truncate very long values
            if isinstance(value, str) and len(value) > 100:
                display_value = value[:100] + "..."
            else:
                display_value = str(value)
            
            details.append(
                html.Div([
                    html.Strong(f"{display_key}: ", 
                               style={'color': '#374151', 'fontSize': '13px'}),
                    html.Span(display_value, 
                             style={'color': '#6b7280', 'fontSize': '13px'})
                ], style={'marginBottom': '6px', 'paddingLeft': '10px'})
            )
    
    return details

def show_legal_document_details(node_data):
    """Show legal document details in the specific format requested."""
    import json
    
    # Get document type for header
    node_type = node_data.get('type', 'LEGAL_DOCUMENT').upper()
    if node_type == 'RESOLUTION':
        doc_type = 'Resolution'
        doc_type_title = 'Resolution'
    elif node_type == 'ORDINANCE':
        doc_type = 'Ordinance' 
        doc_type_title = 'Ordinance'
    else:
        # Fallback to document_type field for legacy nodes
        doc_type = node_data.get('document_type', 'Legal Document')
        doc_type_title = doc_type.capitalize()
    
    # Calculate network analysis
    connections = node_data.get('connections', 0)
    # Size ratio calculation (simple metric based on connections)
    size_ratio = round(connections / 100, 2) if connections > 0 else 0.01
    
    # Get title (truncated for display)
    full_title = node_data.get('title', 'No title')
    truncated_title = full_title[:35] + '...' if len(full_title) > 35 else full_title
    
    # Format meeting date from "01.09.2024" to "01-09-2024"
    meeting_date = node_data.get('meeting_date', '')
    formatted_meeting_date = meeting_date.replace('.', '-') if meeting_date else 'Unknown'
    
    # Parse legal metadata for vote details
    legal_metadata_str = node_data.get('legal_metadata', '{}')
    try:
        if isinstance(legal_metadata_str, str):
            legal_metadata = json.loads(legal_metadata_str)
        else:
            legal_metadata = legal_metadata_str
    except:
        legal_metadata = {}
    
    vote_details = legal_metadata.get('vote_details', {})
    
    # Get timestamp from metadata
    metadata_str = node_data.get('metadata', '{}')
    try:
        if isinstance(metadata_str, str):
            metadata = json.loads(metadata_str)
        else:
            metadata = metadata_str
    except:
        metadata = {}
    
    extraction_timestamp = metadata.get('extraction_timestamp', '')
    # Convert ISO timestamp to epoch milliseconds for display
    if extraction_timestamp:
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(extraction_timestamp.replace('Z', '+00:00'))
            timestamp_ms = int(dt.timestamp() * 1000)
        except:
            timestamp_ms = '1750786217509'  # fallback
    else:
        timestamp_ms = '1750786217509'  # fallback
    
    # Build the legal document details in the exact format
    details = []
    
    # Header
    details.append(
        html.H4(f"{doc_type_title} Details", 
                style={'color': '#1f2937', 'marginBottom': '15px', 'fontWeight': 'bold'})
    )
    
    # Truncated title
    details.append(
        html.Div(truncated_title, 
                style={'color': '#1f2937', 'marginBottom': '15px', 'fontWeight': 'bold'})
    )
    
    # Network Analysis
    details.append(
        html.Div([
            html.Strong("Network Analysis: "),
            html.Span(f"{connections} connections • Size ratio: {size_ratio}")
        ], style={'marginBottom': '8px'})
    )
    
    # Type
    details.append(
        html.Div([
            html.Strong("Type: "),
            html.Span(doc_type_title)
        ], style={'marginBottom': '8px'})
    )
    
    # Nodetype
    details.append(
        html.Div([
            html.Strong("Nodetype: "),
            html.Span(doc_type_title)
        ], style={'marginBottom': '8px'})
    )
    
    # Document_Number
    details.append(
        html.Div([
            html.Strong("Document_Number: "),
            html.Span(node_data.get('document_number', 'Unknown'))
        ], style={'marginBottom': '8px'})
    )
    
    # Full_Title
    details.append(
        html.Div([
            html.Strong("Full_Title: "),
            html.Span(full_title)
        ], style={'marginBottom': '8px'})
    )
    
    # Title
    details.append(
        html.Div([
            html.Strong("Title: "),
            html.Span(full_title)
        ], style={'marginBottom': '8px'})
    )
    
    # Document_Type
    details.append(
        html.Div([
            html.Strong("Document_Type: "),
            html.Span(doc_type_title)
        ], style={'marginBottom': '8px'})
    )
    
    # Meeting_Date
    details.append(
        html.Div([
            html.Strong("Meeting_Date: "),
            html.Span(formatted_meeting_date)
        ], style={'marginBottom': '8px'})
    )
    
    # Vote_Details
    details.append(
        html.Div([
            html.Strong("Vote_Details: "),
            html.Span(json.dumps(vote_details) if vote_details else "{}")
        ], style={'marginBottom': '8px'})
    )
    
    # Timestamp
    details.append(
        html.Div([
            html.Strong("Timestamp: "),
            html.Span(str(timestamp_ms))
        ], style={'marginBottom': '8px'})
    )
    
    return details

if __name__ == '__main__':
    print("🚀 Starting FIXED interactive graph viewer...")
    print(f"📊 Status: {'ERROR' if visualizer.error_message else 'SUCCESS'}")
    print(f"📈 Loaded: {total_nodes} nodes, {total_edges} edges")
    print(f"🗂️ Node types: {list(node_counts.keys())}")
    if visualizer.error_message:
        print(f"❌ Error: {visualizer.error_message}")
    print("🌐 Visit: http://127.0.0.1:8050")
    print("💡 Click on any node to see all its properties!")
    
    app.run(debug=True, port=8051) 