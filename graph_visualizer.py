#!/usr/bin/env python3
"""
City Clerk Agenda Graph Visualizer - ENHANCED VISUAL DIFFERENTIATION
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
import dash
from dash import dcc, html, Input, Output
import dash_cytoscape as cyto
from gremlin_python.driver import client, serializer
from datetime import datetime

from config import (
    COSMOS_ENDPOINT, COSMOS_KEY, DATABASE, CONTAINER,
    PARTITION_KEY, PARTITION_VALUE
)

cyto.load_extra_layouts()

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

class AgendaGraphVisualizer:
    """Visualizer with enhanced visual differentiation."""
    
    def __init__(self):
        self.gremlin_client = None
        self.graph_data = {"nodes": [], "edges": []}
        self.connection_status = "disconnected"
        
        self.connect_to_database()
        self.load_initial_data()
    
    def connect_to_database(self):
        """Connect to Cosmos DB."""
        try:
            self.gremlin_client = client.Client(
                f"{COSMOS_ENDPOINT}/gremlin", "g",
                username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
                password=COSMOS_KEY,
                message_serializer=serializer.GraphSONSerializersV2d0()
            )
            test_result = self.gremlin_client.submit("g.V().count()").all().result()
            vertex_count = test_result[0] if test_result else 0
            log.info(f"✅ Connected! Found {vertex_count} vertices")
            self.connection_status = "connected"
        except Exception as e:
            log.error(f"❌ Connection failed: {e}")
    
    def load_initial_data(self):
        """Load data on initialization."""
        self.query_agenda_graph()
        log.info(f"Loaded {len(self.graph_data['nodes'])} nodes")
    
    def query_agenda_graph(self, meeting_date: Optional[str] = None):
        """Query agenda data."""
        print(f"\nDEBUG: GraphVisualizer loading data")
        print(f"  Endpoint: {COSMOS_ENDPOINT}")
        print(f"  Database: {DATABASE}")
        print(f"  Container: {CONTAINER}")
        
        if self.connection_status != "connected":
            print("  WARNING: Not connected to database!")
            return
        
        try:
            if meeting_date:
                vertices_query = f"""
                    g.V().has('Meeting', 'date', '{meeting_date}')
                    .union(__.identity(), __.out(), __.out().out())
                    .dedup().valueMap(true)
                """
            else:
                vertices_query = "g.V().valueMap(true)"
            
            print(f"  Query: {vertices_query}")
            
            vertices = self.gremlin_client.submit(vertices_query).all().result()
            print(f"  Query result count: {len(vertices) if vertices else 0}")
            
            if not vertices:
                print("  WARNING: No vertices found!")
                # Try a simpler query
                count_query = "g.V().count()"
                count_result = self.gremlin_client.submit(count_query).all().result()
                print(f"  Total vertex count: {count_result[0] if count_result else 0}")
                
            log.info(f"Got {len(vertices)} vertices")
            
            nodes = []
            node_ids = set()
            
            for vertex in vertices:
                vertex_id = str(vertex.get('id', ''))
                vertex_label = str(vertex.get('label', ''))
                
                if not vertex_id or vertex_id in node_ids:
                    continue
                    
                node_ids.add(vertex_id)
                
                # Extract properties
                props = {}
                for key, value in vertex.items():
                    if key not in ['id', 'label']:
                        if isinstance(value, list) and value:
                            props[key] = value[0]
                        else:
                            props[key] = value
                
                # Get display name
                display_name = self._get_display_name(vertex, vertex_label, props)
                
                node_data = {
                    "id": vertex_id,
                    "label": display_name,
                    "type": vertex_label,
                    **props
                }
                nodes.append(node_data)
            
            # Get edges
            edges = []
            edge_results = self.gremlin_client.submit("""
                g.E().project('source','target','label')
                .by(outV().id())
                .by(inV().id())
                .by(label())
            """).all().result()
            
            for e in edge_results:
                source = str(e['source'])
                target = str(e['target'])
                if source in node_ids and target in node_ids:
                    edges.append({
                        "source": source,
                        "target": target,
                        "label": e['label']
                    })
            
            self.graph_data = {"nodes": nodes, "edges": edges}
            
        except Exception as e:
            print(f"  ERROR loading graph: {e}")
            log.error(f"Query failed: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _get_display_name(self, vertex, label, props):
        """Get display name for nodes."""
        # Extract vertex_id from the vertex object
        vertex_id = str(vertex.get('id', ''))
        
        if label == 'Meeting':
            return f"📅 Meeting\n{props.get('date', 'Unknown')}"
        elif label == 'AgendaSection':
            title = props.get('title', 'Section')
            return f"📋 {title[:30]}..."
        elif label == 'AgendaItem':
            code = props.get('code', '')
            doc_type = props.get('type', '')
            title = props.get('title', '')[:25]
            
            # Add emoji based on document type
            emoji = {
                'Resolution': '📜',
                'Ordinance': '⚖️',
                'Contract': '📄',
                'Proclamation': '📢',
                'Discussion': '💬',
                'Presentation': '🎯',
                'Report': '📊'
            }.get(doc_type, '📌')
            
            if code:
                return f"{emoji} {code}\n{doc_type}"
            else:
                return f"{emoji} {doc_type}\n{title}..."
        elif label == 'Person':
            name = props.get('name', 'Unknown')
            role = props.get('roles', props.get('role', ''))
            return f"👤 {name}" + (f"\n{role}" if role else "")
        elif label == 'Organization':
            return f"🏢 {props.get('name', 'Unknown Org')[:30]}"
        elif label == 'Location':
            return f"📍 {props.get('name', 'Unknown Location')[:30]}"
        elif label == 'FinancialItem':
            amount = props.get('amount', '')
            return f"💰 {amount}"
        else:
            return props.get('name', props.get('title', vertex_id))[:40]
    
    def get_cytoscape_elements(self):
        """Convert to Cytoscape format with connection-based node sizing."""
        elements = []
        
        # Calculate connection counts for each node
        connection_counts = {}
        for edge in self.graph_data["edges"]:
            source = edge['source']
            target = edge['target']
            connection_counts[source] = connection_counts.get(source, 0) + 1
            connection_counts[target] = connection_counts.get(target, 0) + 1
        
        # Calculate size scaling parameters
        if connection_counts:
            min_connections = min(connection_counts.values())
            max_connections = max(connection_counts.values())
        else:
            min_connections = max_connections = 0
        
        # Add nodes with connection-based sizing
        for node in self.graph_data["nodes"]:
            node_id = node['id']
            connections = connection_counts.get(node_id, 0)
            
            # Calculate relative size (0-1 scale)
            if max_connections > min_connections:
                size_ratio = (connections - min_connections) / (max_connections - min_connections)
            else:
                size_ratio = 0.5  # Default middle size if all nodes have same connections
            
            # Enhanced node data with connection info
            enhanced_node = node.copy()
            enhanced_node['connections'] = connections
            enhanced_node['size_ratio'] = size_ratio
            
            elements.append({'data': enhanced_node})
        
        # Add edges (unchanged)
        for i, edge in enumerate(self.graph_data["edges"]):
            elements.append({
                'data': {
                    'id': f'e{i}',
                    'source': edge['source'],
                    'target': edge['target'],
                    'label': edge['label']
                }
            })
        
        return elements
    
    def get_available_meetings(self):
        """Get list of available meetings."""
        if self.connection_status != "connected":
            return [{'label': 'All Meetings', 'value': ''}]
        
        try:
            dates = self.gremlin_client.submit("g.V().hasLabel('Meeting').values('date')").all().result()
            options = [{'label': 'All Meetings', 'value': ''}]
            for date in dates:
                options.append({'label': f'Meeting - {date}', 'value': date})
            return options
        except:
            return [{'label': 'All Meetings', 'value': ''}]


# Initialize visualizer
visualizer = AgendaGraphVisualizer()

# Create app
app = dash.Dash(__name__)

# Create a legend component
legend = html.Div([
    html.H4("Node Types", style={'marginBottom': '10px'}),
    html.Div([
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#0EA5E9',
                'display': 'inline-block',
                'marginRight': '10px',
                'borderRadius': '3px'
            }),
            html.Span("Meeting", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#8B5CF6',
                'display': 'inline-block',
                'marginRight': '10px',
                'borderRadius': '10px'
            }),
            html.Span("Agenda Section", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#F59E0B',
                'display': 'inline-block',
                'marginRight': '10px',
                'borderRadius': '50%'
            }),
            html.Span("Agenda Item", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#EF4444',
                'display': 'inline-block',
                'marginRight': '10px',
                'transform': 'rotate(45deg)',
                'marginBottom': '-5px'
            }),
            html.Span("Person", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#10B981',
                'display': 'inline-block',
                'marginRight': '10px',
                'clipPath': 'polygon(50% 0%, 100% 25%, 100% 75%, 50% 100%, 0% 75%, 0% 25%)'
            }),
            html.Span("Organization", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#EC4899',
                'display': 'inline-block',
                'marginRight': '10px',
                'clipPath': 'polygon(50% 0%, 60% 40%, 100% 50%, 60% 60%, 50% 100%, 40% 60%, 0% 50%, 40% 40%)'
            }),
            html.Span("Location", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'}),
        
        html.Div([
            html.Div(style={
                'width': '20px', 
                'height': '20px', 
                'backgroundColor': '#14B8A6',
                'display': 'inline-block',
                'marginRight': '10px',
                'clipPath': 'polygon(30% 0%, 70% 0%, 100% 30%, 100% 70%, 70% 100%, 30% 100%, 0% 70%, 0% 30%)'
            }),
            html.Span("Financial Item", style={'verticalAlign': 'top'})
        ], style={'marginBottom': '5px'})
    ])
], style={
    'position': 'absolute',
    'top': '80px',
    'right': '20px',
    'backgroundColor': 'white',
    'padding': '15px',
    'border': '1px solid #ccc',
    'borderRadius': '5px',
    'zIndex': 1000
})

app.layout = html.Div([
    html.H1("🏛️ City Clerk Agenda Graph", style={'textAlign': 'center'}),
    
    html.Div([
        dcc.Dropdown(
            id='meeting-selector',
            options=visualizer.get_available_meetings(),
            value='',
            style={'width': '300px', 'display': 'inline-block', 'marginRight': '10px'}
        ),
        dcc.Dropdown(
            id='layout-selector',
            options=[
                {'label': 'Hierarchical', 'value': 'breadthfirst'},
                {'label': 'Circle', 'value': 'circle'},
                {'label': 'Grid', 'value': 'grid'},
                {'label': 'Force-Directed', 'value': 'cose'},
                {'label': 'Concentric', 'value': 'concentric'}
            ],
            value='breadthfirst',
            style={'width': '200px', 'display': 'inline-block', 'marginRight': '10px'}
        ),
        html.Button('Refresh', id='refresh-btn', style={'marginLeft': '10px'})
    ], style={'padding': '20px', 'textAlign': 'center'}),
    
    html.Div([
        # Main graph container
        html.Div([
            cyto.Cytoscape(
                id='cytoscape',
                elements=visualizer.get_cytoscape_elements(),
                style={'width': '100%', 'height': '600px', 'border': '1px solid #ccc'},
                layout={'name': 'breadthfirst', 'directed': True, 'spacingFactor': 1.5},
                stylesheet=[
                    # Meeting nodes - Large blue rectangles (size based on connections)
                    {
                        'selector': 'node[type="Meeting"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 120, 180)',  # Dynamic width based on connections
                            'height': 'mapData(size_ratio, 0, 1, 80, 120)',  # Dynamic height based on connections
                            'backgroundColor': '#0EA5E9',  # Sky blue
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '14px',
                            'fontWeight': 'bold',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 100, 160)',
                            'shape': 'round-rectangle',
                            'borderWidth': '3px',
                            'borderColor': '#0284C7'
                        }
                    },
                    # Section nodes - Purple rounded rectangles (size based on connections)
                    {
                        'selector': 'node[type="AgendaSection"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 100, 150)',  # Dynamic width
                            'height': 'mapData(size_ratio, 0, 1, 60, 100)',  # Dynamic height
                            'backgroundColor': '#8B5CF6',  # Purple
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '12px',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 80, 130)',
                            'shape': 'round-rectangle',
                            'borderWidth': '2px',
                            'borderColor': '#7C3AED'
                        }
                    },
                    # Agenda items - Amber circles (size based on connections)
                    {
                        'selector': 'node[type="AgendaItem"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 80, 130)',  # Dynamic size
                            'height': 'mapData(size_ratio, 0, 1, 80, 130)',  # Dynamic size
                            'backgroundColor': '#F59E0B',  # Amber
                            'color': '#000000',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '11px',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 70, 110)',
                            'shape': 'ellipse',
                            'borderWidth': '2px',
                            'borderColor': '#D97706'
                        }
                    },
                    # Person nodes - Red diamonds (size based on connections)
                    {
                        'selector': 'node[type="Person"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'height': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'backgroundColor': '#EF4444',  # Red
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '11px',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'shape': 'diamond'
                        }
                    },
                    # Organization nodes - Green hexagons (size based on connections)
                    {
                        'selector': 'node[type="Organization"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 80, 130)',  # Dynamic size
                            'height': 'mapData(size_ratio, 0, 1, 80, 130)',  # Dynamic size
                            'backgroundColor': '#10B981',  # Emerald
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '11px',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 70, 110)',
                            'shape': 'hexagon'
                        }
                    },
                    # Location nodes - Pink stars (size based on connections)
                    {
                        'selector': 'node[type="Location"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'height': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'backgroundColor': '#EC4899',  # Pink
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '10px',
                            'textWrap': 'wrap',
                            'textMaxWidth': 'mapData(size_ratio, 0, 1, 60, 100)',
                            'shape': 'star'
                        }
                    },
                    # Financial nodes - Teal octagons (size based on connections)
                    {
                        'selector': 'node[type="FinancialItem"]',
                        'style': {
                            'content': 'data(label)',
                            'width': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'height': 'mapData(size_ratio, 0, 1, 70, 120)',  # Dynamic size
                            'backgroundColor': '#14B8A6',  # Teal
                            'color': '#FFFFFF',
                            'textValign': 'center',
                            'textHalign': 'center',
                            'fontSize': '11px',
                            'textWrap': 'wrap',
                            'shape': 'octagon'
                        }
                    },
                    # Edge styles by type
                    {
                        'selector': 'edge[label="HAS_SECTION"]',
                        'style': {
                            'width': 3,
                            'lineColor': '#3B82F6',
                            'targetArrowColor': '#3B82F6',
                            'targetArrowShape': 'triangle',
                            'curveStyle': 'bezier'
                        }
                    },
                    {
                        'selector': 'edge[label="CONTAINS_ITEM"]',
                        'style': {
                            'width': 2,
                            'lineColor': '#F97316',
                            'targetArrowColor': '#F97316',
                            'targetArrowShape': 'triangle',
                            'curveStyle': 'bezier'
                        }
                    },
                    {
                        'selector': 'edge[label="ATTENDED"]',
                        'style': {
                            'width': 2,
                            'lineColor': '#10B981',
                            'targetArrowColor': '#10B981',
                            'targetArrowShape': 'circle',
                            'lineStyle': 'dashed'
                        }
                    },
                    {
                        'selector': 'edge[label="SPONSORS"]',
                        'style': {
                            'width': 2,
                            'lineColor': '#EF4444',
                            'targetArrowColor': '#EF4444',
                            'targetArrowShape': 'vee',
                            'curveStyle': 'bezier'
                        }
                    },
                    # Default edge style
                    {
                        'selector': 'edge',
                        'style': {
                            'width': 1,
                            'lineColor': '#9CA3AF',
                            'targetArrowColor': '#9CA3AF',
                            'targetArrowShape': 'triangle',
                            'curveStyle': 'bezier',
                            'label': 'data(label)',
                            'fontSize': '8px',
                            'textRotation': 'autorotate'
                        }
                    }
                ],
                wheelSensitivity=0.1
            ),
            legend  # Add the legend
        ], style={'position': 'relative'}),
    ]),
    
    # Node details panel
    html.Div(id='node-info', style={
        'padding': '20px',
        'backgroundColor': '#f5f5f5',
        'marginTop': '20px',
        'borderRadius': '5px'
    })
])

@app.callback(
    Output('cytoscape', 'elements'),
    [Input('meeting-selector', 'value'),
     Input('refresh-btn', 'n_clicks')],
    prevent_initial_call=True
)
def update_graph(meeting, n_clicks):
    visualizer.query_agenda_graph(meeting)
    return visualizer.get_cytoscape_elements()

@app.callback(
    Output('cytoscape', 'layout'),
    Input('layout-selector', 'value')
)
def update_layout(layout_name):
    layouts = {
        'breadthfirst': {'name': 'breadthfirst', 'directed': True, 'spacingFactor': 1.5},
        'circle': {'name': 'circle'},
        'grid': {'name': 'grid', 'rows': 5},
        'cose': {'name': 'cose', 'animate': True},
        'concentric': {'name': 'concentric', 'minNodeSpacing': 50}
    }
    return layouts.get(layout_name, layouts['breadthfirst'])

@app.callback(
    Output('node-info', 'children'),
    Input('cytoscape', 'tapNodeData')
)
def show_node_details(data):
    if not data:
        return "Click a node to see details"
    
    # Parse URLs if they exist
    urls_json = data.get('urls')
    urls = []
    if urls_json:
        try:
            urls = json.loads(urls_json)
        except:
            urls = []
    
    # Build property rows
    property_rows = []
    
    # Standard properties
    skip_props = {'id', 'label', 'partitionKey', 'urls', 'url_count', 
                  'primary_url', 'primary_url_text', 'connections', 'size_ratio'}
    # Also exclude parent_ props since they're shown in the dedicated section above
    skip_props.update({k for k in data.keys() if k.startswith('parent_')})
    
    details = [
        html.H3(f"{data.get('type', 'Unknown')} Details"),
        html.P(html.Strong(data.get('label', 'Unknown')))
    ]
    
    # Add connection information prominently
    connections = data.get('connections', 0)
    size_ratio = data.get('size_ratio', 0)
    details.append(html.P([
        html.Strong("Network Analysis: "), 
        f"{connections} connections • Size ratio: {size_ratio:.2f}"
    ], style={'backgroundColor': '#e3f2fd', 'padding': '8px', 'borderRadius': '4px'}))
    
    # Parent Relationships section (NEW: prominently display parent_ attributes)
    parent_props = {k: v for k, v in data.items() if k.startswith('parent_') and v is not None}
    if parent_props:
        details.append(html.H5("🔗 Parent Relationships:", style={'color': '#059669', 'marginTop': '15px', 'fontWeight': 'bold'}))
        
        for key, value in sorted(parent_props.items()):
            # Format the key nicely - remove 'parent_' prefix and format
            display_key = key.replace('parent_', '').replace('_', ' ').title()
            
            details.append(html.P([
                html.Strong(f"Parent {display_key}: ", style={'color': '#047857'}),
                html.Span(str(value), style={'color': '#065f46', 'fontWeight': 'bold'})
            ], style={'backgroundColor': '#ecfdf5', 'padding': '8px', 'borderRadius': '4px', 'border': '1px solid #a7f3d0', 'marginBottom': '8px'}))
    
    # Show relevant properties based on node type
    if data.get('type') == 'AgendaItem':
        for key in ['code', 'type', 'title', 'document_reference', 'sponsor', 'department']:
            if key in data:
                details.append(html.P([html.Strong(f"{key.title()}: "), str(data[key])]))
    else:
        for key, value in data.items():
            if key not in skip_props and value:
                # Handle JSON strings
                display_value = value
                if isinstance(value, str) and value.startswith('{'):
                    try:
                        parsed = json.loads(value)
                        display_value = json.dumps(parsed, indent=2)
                    except:
                        pass
                
                details.append(html.P([html.Strong(f"{key.title()}: "), str(display_value)]))
    
    # Add URLs section if available
    if urls:
        url_section = html.Div([
            html.H5("Associated URLs", className="mt-3 mb-2"),
            html.Div([
                html.Div([
                    html.A(
                        url.get('text', url.get('url', 'Link'))[:100] + '...' 
                        if len(url.get('text', url.get('url', ''))) > 100 
                        else url.get('text', url.get('url', 'Link')),
                        href=url.get('url', '#'),
                        target="_blank",
                        className="d-block mb-1"
                    ),
                    html.Small(f"Page {url.get('page', 'N/A')}", 
                             className="text-muted")
                ], className="mb-2")
                for url in urls
            ])
        ])
        details.append(url_section)
    
    return html.Div(details)


if __name__ == '__main__':
    print("\n🎨 Visual enhancements:")
    print("  - Different colors for each node type")
    print("  - Different shapes (rectangle, circle, diamond, hexagon, star, octagon)")
    print("  - Emojis in labels for quick identification")
    print("  - Color-coded edges")
    print("  - Visual legend")
    print("\nTry different layouts for different perspectives!")
    
    app.run(debug=True, port=8052) 