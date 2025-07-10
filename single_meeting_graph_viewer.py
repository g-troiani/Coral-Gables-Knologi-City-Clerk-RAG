#!/usr/bin/env python3
"""
Single Meeting Day Graph Visualizer - Interactive Cytoscape Version
Focus on visualizing the graph for just one meeting day
"""

import json
import logging
from typing import Dict, List, Optional, Any
from pathlib import Path
from collections import Counter
import dash
from dash import dcc, html, Input, Output, State
import dash_cytoscape as cyto
import networkx as nx
from datetime import datetime

cyto.load_extra_layouts()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class SingleMeetingGraphVisualizer:
    """Interactive visualizer for a single meeting day from GraphRAG knowledge graphs."""
    
    def __init__(self, graph_path: str = "local_graph_data/city_clerk_graph.graphml"):
        self.graph_path = Path(graph_path)
        self.full_graph = None
        self.filtered_graph = None
        self.graph_data = {"nodes": [], "edges": []}
        self.error_message = None
        self.available_meetings = []
        self.selected_meeting = None
        
        # Load graph data with better error handling
        self._safe_load_graph_data()
        self._extract_available_meetings()
    
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
            self.full_graph = nx.read_graphml(str(self.graph_path))
            log.info(f"📊 Full graph loaded: {self.full_graph.number_of_nodes()} nodes, {self.full_graph.number_of_edges()} edges")
            
        except Exception as e:
            self.error_message = f"Failed to load graph: {str(e)}"
            log.error(f"❌ {self.error_message}")
            import traceback
            traceback.print_exc()
            
            # Create empty fallback graph
            self.full_graph = nx.DiGraph()
    
    def _extract_available_meetings(self):
        """Extract available meetings from the graph."""
        if not self.full_graph:
            return
        
        meetings = []
        for node_id, attrs in self.full_graph.nodes(data=True):
            # Check if this is a meeting node
            node_label = attrs.get("label", "")
            node_type = self._get_node_type(attrs)
            
            if node_type == "MEETING":
                title = attrs.get("title", attrs.get("name", ""))
                meeting_date = attrs.get("meeting_date", "")
                
                # Try to parse date for sorting
                try:
                    if meeting_date:
                        # Handle various date formats
                        if '.' in meeting_date:
                            date_obj = datetime.strptime(meeting_date, "%m.%d.%Y")
                        elif '/' in meeting_date:
                            date_obj = datetime.strptime(meeting_date, "%m/%d/%Y")
                        elif '-' in meeting_date:
                            date_obj = datetime.strptime(meeting_date, "%m-%d-%Y")
                        else:
                            date_obj = datetime.now()
                    else:
                        date_obj = datetime.now()
                except:
                    date_obj = datetime.now()
                
                meetings.append({
                    'id': node_id,
                    'title': title,
                    'date': meeting_date,
                    'date_obj': date_obj,
                    'display_name': f"{meeting_date} - {title}" if meeting_date else title
                })
        
        # Sort by date (most recent first)
        meetings.sort(key=lambda x: x['date_obj'], reverse=True)
        self.available_meetings = meetings
        log.info(f"📅 Found {len(meetings)} meetings")
    
    def _get_node_type(self, attrs: Dict) -> str:
        """Get standardized node type from attributes."""
        node_label = attrs.get("label", "other")
        node_type_map = {
            "meeting": "MEETING",
            "agendaItem": "AGENDA_ITEM", 
            "section": "SECTION",
            "document": "DOCUMENT",
            "person": "PERSON",
            "organization": "ORGANIZATION",
            "department": "DEPARTMENT",
            "location": "LOCATION"
        }
        return node_type_map.get(node_label, node_label.upper())
    
    def filter_graph_by_meeting(self, meeting_id: str):
        """Filter graph to show complete ontology/taxonomy for a meeting date."""
        if not self.full_graph or not meeting_id:
            self.filtered_graph = nx.DiGraph()
            self.graph_data = {"nodes": [], "edges": []}
            return
        
        # Get the meeting date from the meeting node
        meeting_attrs = self.full_graph.nodes.get(meeting_id, {})
        meeting_date = meeting_attrs.get('meeting_date', '')
        
        if not meeting_date:
            log.warning(f"No meeting_date found for meeting: {meeting_id}")
            return
        
        log.info(f"🔍 Filtering graph for complete ontology of date: {meeting_date}")
        
        # Find ALL nodes that have this meeting date (complete taxonomy)
        date_related_nodes = set()
        date_related_nodes.add(meeting_id)  # Include the meeting itself
        
        # Find all nodes with matching meeting_date
        for node_id, attrs in self.full_graph.nodes(data=True):
            node_meeting_date = attrs.get('meeting_date', '')
            
            # Check various date formats and fields
            if node_meeting_date == meeting_date:
                date_related_nodes.add(node_id)
                continue
            
            # Also check other date-related fields
            other_date_fields = ['date', 'effective_date', 'scheduled_date']
            for field in other_date_fields:
                if attrs.get(field, '') == meeting_date:
                    date_related_nodes.add(node_id)
                    break
            
            # Check if the meeting date appears in source_file, title, or description
            source_file = str(attrs.get('source_file', '')).lower()
            title = str(attrs.get('title', '')).lower()
            description = str(attrs.get('description', '')).lower()
            
            # Convert meeting_date to various formats to check
            date_formats = [meeting_date]
            if '.' in meeting_date:
                # Convert 01.19.2024 to other formats
                parts = meeting_date.split('.')
                if len(parts) == 3:
                    date_formats.extend([
                        f"{parts[0]}/{parts[1]}/{parts[2]}",  # 01/19/2024
                        f"{parts[2]}-{parts[0]}-{parts[1]}",  # 2024-01-19
                        f"{parts[0]}-{parts[1]}-{parts[2]}",  # 01-19-2024
                    ])
            
            # Check if any date format appears in the node's text fields
            for date_format in date_formats:
                if (date_format.lower() in source_file or 
                    date_format.lower() in title or 
                    date_format.lower() in description):
                    date_related_nodes.add(node_id)
                    break
        
        log.info(f"✅ Found {len(date_related_nodes)} nodes with date {meeting_date}")
        
        # Now find all nodes connected to these date-related nodes (expand the taxonomy)
        expanded_nodes = set(date_related_nodes)
        
        # Add all directly connected nodes (1 hop from any date-related node)
        for node_id in date_related_nodes:
            # Add neighbors (outgoing edges)
            for neighbor in self.full_graph.neighbors(node_id):
                expanded_nodes.add(neighbor)
            
            # Add predecessors (incoming edges)
            for predecessor in self.full_graph.predecessors(node_id):
                expanded_nodes.add(predecessor)
        
        # Add 2-hop connections from date-related nodes to capture full taxonomy
        for node_id in list(date_related_nodes):
            # Get neighbors of neighbors
            for neighbor in self.full_graph.neighbors(node_id):
                for second_neighbor in self.full_graph.neighbors(neighbor):
                    expanded_nodes.add(second_neighbor)
                for second_predecessor in self.full_graph.predecessors(neighbor):
                    expanded_nodes.add(second_predecessor)
            
            # Get predecessors of predecessors
            for predecessor in self.full_graph.predecessors(node_id):
                for second_neighbor in self.full_graph.neighbors(predecessor):
                    expanded_nodes.add(second_neighbor)
                for second_predecessor in self.full_graph.predecessors(predecessor):
                    expanded_nodes.add(second_predecessor)
        
        # Create subgraph with the expanded node set
        self.filtered_graph = self.full_graph.subgraph(expanded_nodes).copy()
        self.selected_meeting = meeting_id
        
        log.info(f"✅ Complete ontology graph: {self.filtered_graph.number_of_nodes()} nodes, {self.filtered_graph.number_of_edges()} edges")
        
        # Log the breakdown by node type
        node_types = {}
        for node_id in expanded_nodes:
            node_attrs = self.full_graph.nodes[node_id]
            node_type = self._get_node_type(node_attrs)
            node_types[node_type] = node_types.get(node_type, 0) + 1
        
        log.info(f"📊 Node type breakdown: {dict(node_types)}")
        
        # Convert to display format
        self._convert_to_graph_data()
    
    def _convert_to_graph_data(self):
        """Convert filtered NetworkX graph to display format."""
        if not self.filtered_graph:
            log.warning("⚠️  No filtered graph to convert")
            self.graph_data = {"nodes": [], "edges": []}
            return
        
        # Calculate connection counts for each node
        connection_counts = {}
        for src, dst in self.filtered_graph.edges():
            connection_counts[src] = connection_counts.get(src, 0) + 1
            connection_counts[dst] = connection_counts.get(dst, 0) + 1
        
        # Convert nodes
        nodes = []
        for node_id, attrs in self.filtered_graph.nodes(data=True):
            connections = connection_counts.get(node_id, 0)
            display_name = self._get_display_name(node_id, attrs)
            node_type = self._get_node_type(attrs)
            
            node_data = {
                "id": node_id,
                "label": display_name,
                "type": node_type,
                "connections": connections,
                "title": attrs.get("title", attrs.get("name", "")),
                "description": attrs.get("description", "")[:200] + "..." if attrs.get("description", "") else "",
                **attrs
            }
            nodes.append(node_data)
        
        # Convert edges
        edges = []
        for i, (src, dst, attrs) in enumerate(self.filtered_graph.edges(data=True)):
            relationship_type = attrs.get("label", attrs.get("relationship", "RELATED"))
            
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
        node_type = self._get_node_type(attrs)
        title = attrs.get("title", attrs.get("name", ""))
        
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
        elif node_type == "DEPARTMENT":
            return f"🏛️ {title[:25]}"
        elif node_type == "LOCATION":
            return f"📍 {title[:25]}"
        else:
            return f"{title[:25]}" if title else node_id[:20]
    
    def get_cytoscape_elements(self):
        """Convert to Cytoscape format."""
        if self.error_message:
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
    
    def get_meeting_options(self):
        """Get meeting options for dropdown."""
        if not self.available_meetings:
            return []
        
        return [
            {'label': meeting['display_name'], 'value': meeting['id']}
            for meeting in self.available_meetings
        ]

# Initialize visualizer
log.info("🚀 Initializing Single Meeting Graph Visualizer...")
visualizer = SingleMeetingGraphVisualizer()

# Check if initialization was successful
if visualizer.error_message:
    log.error(f"❌ Visualizer initialization failed: {visualizer.error_message}")
else:
    log.info(f"✅ Visualizer initialized successfully with {len(visualizer.available_meetings)} meetings")

# Create app
app = dash.Dash(__name__)

# Initial state - no meeting selected
initial_stats = {"No meeting selected": 0}

app.layout = html.Div([
    html.H1("📅 Meeting Date Ontology Visualizer", 
            style={'textAlign': 'center', 'marginBottom': '10px', 'color': '#1f2937'}),
    
    # Meeting selection
    html.Div([
        html.Label("Select Meeting Date:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
        dcc.Dropdown(
            id='meeting-selector',
            options=visualizer.get_meeting_options(),
            value=None,
            placeholder="Choose a meeting date to see complete ontology...",
            style={'width': '600px', 'display': 'inline-block'}
        ),
        html.Div([
            html.P("Shows complete taxonomy/ontology for the selected date:", 
                   style={'fontSize': '14px', 'color': '#6b7280', 'margin': '5px 0 0 0'}),
            html.P("• All documents, agenda items, ordinances, resolutions from that date", 
                   style={'fontSize': '12px', 'color': '#6b7280', 'margin': '0'}),
            html.P("• All people, organizations, locations mentioned", 
                   style={'fontSize': '12px', 'color': '#6b7280', 'margin': '0'}),
            html.P("• All relationships between these entities", 
                   style={'fontSize': '12px', 'color': '#6b7280', 'margin': '0'})
        ], style={'textAlign': 'center', 'marginTop': '10px'})
    ], style={'textAlign': 'center', 'marginBottom': '20px'}),
    
    # Status indicator
    html.Div(id='status-info', children=[
        html.P(f"📊 Status: {'ERROR' if visualizer.error_message else 'Ready'}", 
               style={'textAlign': 'center', 'color': 'red' if visualizer.error_message else 'blue', 'fontWeight': 'bold'}),
        html.P("Select a meeting date to view its complete ontology", 
               style={'textAlign': 'center', 'color': '#6b7280', 'marginBottom': '20px'}),
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
                {'label': '📊 Hierarchy (Dagre)', 'value': 'dagre'},
                {'label': '⚡ Force-Directed', 'value': 'cose'},
                {'label': '🎯 Concentric', 'value': 'concentric'},
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
                elements=[],  # Start with empty elements
                style={
                    'width': '100%', 
                    'height': '700px', 
                    'border': '2px solid #000' if visualizer.error_message else '1px solid #e5e7eb', 
                    'borderRadius': '8px',
                    'backgroundColor': '#ffe6e6' if visualizer.error_message else '#f8f9fa'
                },
                layout={'name': 'breadthfirst', 'directed': True},
                stylesheet=[
                    # Simple universal node style
                    {
                        'selector': 'node',
                        'style': {
                            'content': 'data(label)',
                            'backgroundColor': '#3B82F6',
                            'color': 'white',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'width': '60px',
                            'height': '60px',
                            'fontSize': '8px',
                            'textWrap': 'wrap',
                            'textMaxWidth': '55px',
                            'borderWidth': '1px',
                            'borderColor': '#1E40AF'
                        }
                    },
                    # Meeting nodes - Larger and blue
                    {
                        'selector': 'node[type="MEETING"]',
                        'style': {
                            'backgroundColor': '#0EA5E9',
                            'width': '100px',
                            'height': '60px',
                            'shape': 'round-rectangle',
                            'fontSize': '10px'
                        }
                    },
                    # Person nodes - Red diamonds
                    {
                        'selector': 'node[type="PERSON"]',
                        'style': {
                            'backgroundColor': '#EF4444',
                            'shape': 'diamond'
                        }
                    },
                    # Organization nodes - Green
                    {
                        'selector': 'node[type="ORGANIZATION"]',
                        'style': {
                            'backgroundColor': '#10B981',
                            'shape': 'hexagon'
                        }
                    },
                    # Agenda items - Yellow circles  
                    {
                        'selector': 'node[type="AGENDA_ITEM"]',
                        'style': {
                            'backgroundColor': '#F59E0B',
                            'shape': 'ellipse'
                        }
                    },
                    # Sections - Purple rectangles
                    {
                        'selector': 'node[type="SECTION"]',
                        'style': {
                            'backgroundColor': '#8B5CF6',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Documents - Gray rectangles
                    {
                        'selector': 'node[type="DOCUMENT"]',
                        'style': {
                            'backgroundColor': '#6B7280',
                            'shape': 'round-rectangle'
                        }
                    },
                    # Verbatim Transcripts - Pink/red diamonds
                    {
                        'selector': 'node[type="VERBATIM_TRANSCRIPT"]',
                        'style': {
                            'backgroundColor': '#EC4899',
                            'shape': 'diamond',
                            'width': '70px',
                            'height': '70px'
                        }
                    },
                    # Legal Documents - Purple octagons
                    {
                        'selector': 'node[type="LEGAL_DOCUMENT"]',
                        'style': {
                            'backgroundColor': '#9333EA',
                            'shape': 'octagon',
                            'width': '75px',
                            'height': '75px'
                        }
                    },
                    # Resolution nodes - Teal hexagons
                    {
                        'selector': 'node[type="RESOLUTION"]',
                        'style': {
                            'backgroundColor': '#0891B2',
                            'shape': 'hexagon',
                            'width': '80px',
                            'height': '80px'
                        }
                    },
                    # Ordinance nodes - Indigo octagons
                    {
                        'selector': 'node[type="ORDINANCE"]',
                        'style': {
                            'backgroundColor': '#4F46E5',
                            'shape': 'octagon',
                            'width': '85px',
                            'height': '85px'
                        }
                    },
                    # Department nodes - Brown squares
                    {
                        'selector': 'node[type="DEPARTMENT"]',
                        'style': {
                            'backgroundColor': '#A16207',
                            'shape': 'square',
                            'width': '70px',
                            'height': '70px'
                        }
                    },
                    # Location nodes - Orange triangles
                    {
                        'selector': 'node[type="LOCATION"]',
                        'style': {
                            'backgroundColor': '#EA580C',
                            'shape': 'triangle',
                            'width': '65px',
                            'height': '65px'
                        }
                    },
                    # Simple edge style
                    {
                        'selector': 'edge',
                        'style': {
                            'width': 2,
                            'lineColor': '#666',
                            'targetArrowColor': '#666',
                            'targetArrowShape': 'triangle',
                            'curveStyle': 'bezier',
                            'content': 'data(label)',
                            'fontSize': '8px',
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
                html.P("Select a meeting date and click on a node to see its properties", 
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
    [Output('cytoscape', 'elements'),
     Output('cytoscape', 'layout'),
     Output('status-info', 'children')],
    [Input('meeting-selector', 'value'),
     Input('layout-selector', 'value'),
     Input('refresh-btn', 'n_clicks')]
)
def update_graph(selected_meeting, layout_name, n_clicks):
    """Update graph based on selected meeting and layout."""
    if not selected_meeting:
        empty_status = [
            html.P("📊 Status: Ready", 
                   style={'textAlign': 'center', 'color': 'blue', 'fontWeight': 'bold'}),
            html.P("Select a meeting date to view its complete ontology", 
                   style={'textAlign': 'center', 'color': '#6b7280', 'marginBottom': '20px'}),
        ]
        return [], {'name': layout_name, 'directed': True}, empty_status
    
    # Filter graph by selected meeting
    visualizer.filter_graph_by_meeting(selected_meeting)
    
    # Get elements
    elements = visualizer.get_cytoscape_elements()
    
    # Get statistics
    node_counts = visualizer.get_node_type_counts()
    total_nodes = len(visualizer.graph_data["nodes"])
    total_edges = len(visualizer.graph_data["edges"])
    
    # Build layout
    if layout_name == 'breadthfirst':
        layout = {
            'name': 'breadthfirst',
            'directed': True,
            'roots': [selected_meeting],
            'spacingFactor': 2.5,
            'nodeDimensionsIncludeLabels': True,
            'avoidOverlap': True,
            'maximal': False,
            'circle': False,
            'padding': 30
        }
    elif layout_name == 'dagre':
        layout = {
            'name': 'dagre',
            'directed': True,
            'spacingFactor': 1.5,
            'rankDir': 'TB',
            'nodeDimensionsIncludeLabels': True,
            'avoidOverlap': True
        }
    elif layout_name == 'cose':
        layout = {
            'name': 'cose',
            'directed': True,
            'nodeRepulsion': 8000,
            'idealEdgeLength': 100,
            'edgeElasticity': 200,
            'nestingFactor': 5,
            'gravity': 1,
            'numIter': 1000,
            'initialTemp': 200,
            'coolingFactor': 0.95,
            'minTemp': 1.0
        }
    else:
        layout = {'name': layout_name, 'directed': True, 'spacingFactor': 1.5}
    
    # Update status
    status_info = [
        html.P(f"📊 Status: SUCCESS", 
               style={'textAlign': 'center', 'color': 'green', 'fontWeight': 'bold'}),
        html.P(f"📈 {total_nodes} nodes • {total_edges} relationships", 
               style={'textAlign': 'center', 'color': '#6b7280', 'marginBottom': '20px'}),
        html.P(f"🗂️ Node types: {', '.join(node_counts.keys())}", 
               style={'textAlign': 'center', 'color': '#6b7280', 'fontSize': '12px'})
    ]
    
    return elements, layout, status_info

@app.callback(
    Output('node-info', 'children'),
    [Input('cytoscape', 'tapNodeData')]
)
def show_node_details(node_data):
    """Show detailed information about the selected node."""
    if not node_data:
        return [html.P("Select a meeting date and click on a node to see its properties", 
                      style={'color': '#6b7280', 'fontStyle': 'italic'})]
    
    # Get node type for styling
    node_type = node_data.get('type', 'UNKNOWN')
    
    # Type-specific emoji and color
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
        'DEPARTMENT': {'emoji': '🏛️', 'color': '#A16207'},
        'LOCATION': {'emoji': '📍', 'color': '#EA580C'},
    }
    
    info = type_info.get(node_type, {'emoji': '❓', 'color': '#6B7280'})
    
    # Build the details display
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

if __name__ == '__main__':
    import socket
    
    def is_port_open(port):
        """Check if a port is available"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(('127.0.0.1', port))
                return True
            except socket.error:
                return False
    
    # Find available port
    ports_to_try = [8055, 8056, 8057, 8058, 8059]
    available_port = None
    
    for port in ports_to_try:
        if is_port_open(port):
            available_port = port
            break
    
    if not available_port:
        print("❌ ERROR: No available ports found. Please close other applications using ports 8055-8059")
        exit(1)
    
    print("🚀 Starting Single Meeting Graph Viewer...")
    print(f"📊 Status: {'ERROR' if visualizer.error_message else 'SUCCESS'}")
    print(f"📈 Available meetings: {len(visualizer.available_meetings)}")
    if visualizer.error_message:
        print(f"❌ Error: {visualizer.error_message}")
    
    print("=" * 50)
    print(f"🌐 VISIT: http://127.0.0.1:{available_port}")
    print(f"🌐 VISIT: http://localhost:{available_port}")
    print("=" * 50)
    print("💡 Select a meeting from the dropdown to view its graph!")
    print("🔄 Press Ctrl+C to stop the server")
    
    try:
        app.run(
            debug=False,
            host='127.0.0.1',
            port=available_port,
            use_reloader=False
        )
    except Exception as e:
        print(f"❌ ERROR starting server: {e}")
        print("💡 Try running: pip install dash==2.17.1 dash-cytoscape==0.3.0")
        exit(1) 