#!/usr/bin/env python3
"""
Graph Database Visualizer for Azure Cosmos DB Gremlin API
Displays the city clerk knowledge graph in an interactive web interface
"""

import os
import json
import time
import traceback
from typing import Dict, List, Tuple, Any
import networkx as nx
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash
from dash import dcc, html, Input, Output, callback, dash_table
import pandas as pd

from gremlin_python.driver import client, serializer
from gremlin_python.driver.protocol import GremlinServerError

# Import configuration
from config import (
    COSMOS_ENDPOINT, COSMOS_KEY, DATABASE, CONTAINER, 
    PARTITION_KEY, PARTITION_VALUE, validate_config
)

class GraphVisualizer:
    def __init__(self):
        self.gremlin_client = None
        self.graph_data = {"nodes": [], "edges": []}
        self.nx_graph = nx.Graph()
        
        # Validate configuration before connecting
        if not validate_config():
            raise Exception("Configuration validation failed. Please check your .env file.")
        
        self.connect_to_database()
        
    def connect_to_database(self):
        """Connect to Azure Cosmos DB Gremlin API"""
        try:
            print("🔗 Connecting to Cosmos DB Gremlin API...")
            self.gremlin_client = client.Client(
                f"{COSMOS_ENDPOINT}/gremlin", "g",
                username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
                password=COSMOS_KEY,
                message_serializer=serializer.GraphSONSerializersV2d0()
            )
            print("✅ Connected successfully!")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise

    def query_graph_data(self, limit: int = 100):
        """Query vertices and edges from the graph database"""
        if not self.gremlin_client:
            raise Exception("Not connected to database")
            
        try:
            print("📊 Querying graph data...")
            
            # Query vertices
            vertices_query = f"g.V().limit({limit})"
            vertices_result = self.gremlin_client.submit(vertices_query).all()
            
            # Query edges  
            edges_query = f"g.E().limit({limit})"
            edges_result = self.gremlin_client.submit(edges_query).all()
            
            # Process vertices
            nodes = []
            for vertex in vertices_result:
                try:
                    vertex_data = vertex.result()
                    node = {
                        'id': vertex_data.get('id', ''),
                        'label': vertex_data.get('label', 'Unknown'),
                        'properties': vertex_data.get('properties', {})
                    }
                    
                    # Extract name if available
                    props = node['properties']
                    if 'name' in props and isinstance(props['name'], list) and len(props['name']) > 0:
                        node['name'] = props['name'][0].get('value', node['id'])
                    else:
                        node['name'] = node['id']
                    
                    nodes.append(node)
                except Exception as e:
                    print(f"⚠️ Error processing vertex: {e}")
                    continue
            
            # Process edges
            edges = []
            for edge in edges_result:
                try:
                    edge_data = edge.result()
                    edge_info = {
                        'id': edge_data.get('id', ''),
                        'label': edge_data.get('label', 'RELATED'),
                        'source': edge_data.get('outV', ''),
                        'target': edge_data.get('inV', ''),
                        'properties': edge_data.get('properties', {})
                    }
                    edges.append(edge_info)
                except Exception as e:
                    print(f"⚠️ Error processing edge: {e}")
                    continue
            
            self.graph_data = {"nodes": nodes, "edges": edges}
            print(f"✅ Retrieved {len(nodes)} nodes and {len(edges)} edges")
            
            # Build NetworkX graph for layout calculation
            self.build_networkx_graph()
            
            return self.graph_data
            
        except Exception as e:
            print(f"❌ Query failed: {e}")
            raise

    def build_networkx_graph(self):
        """Build NetworkX graph for layout calculations"""
        self.nx_graph = nx.Graph()
        
        # Add nodes
        for node in self.graph_data["nodes"]:
            self.nx_graph.add_node(node['id'], **node)
        
        # Add edges
        for edge in self.graph_data["edges"]:
            if edge['source'] in self.nx_graph and edge['target'] in self.nx_graph:
                self.nx_graph.add_edge(edge['source'], edge['target'], **edge)

    def create_plotly_visualization(self, layout_type="spring"):
        """Create interactive Plotly visualization"""
        if not self.graph_data["nodes"]:
            return go.Figure()
        
        # Calculate layout
        if layout_type == "spring":
            pos = nx.spring_layout(self.nx_graph, k=1, iterations=50)
        elif layout_type == "circular":
            pos = nx.circular_layout(self.nx_graph)
        elif layout_type == "random":
            pos = nx.random_layout(self.nx_graph)
        else:
            pos = nx.spring_layout(self.nx_graph)
        
        # Create edge traces
        edge_x = []
        edge_y = []
        edge_info = []
        
        for edge in self.graph_data["edges"]:
            source_id = edge['source']
            target_id = edge['target']
            
            if source_id in pos and target_id in pos:
                x0, y0 = pos[source_id]
                x1, y1 = pos[target_id]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
                edge_info.append(f"{source_id} -> {target_id} ({edge['label']})")
        
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=0.5, color='#888'),
            hoverinfo='none',
            mode='lines'
        )
        
        # Create node traces by label type
        node_traces = {}
        color_map = px.colors.qualitative.Set3
        
        for i, node in enumerate(self.graph_data["nodes"]):
            if node['id'] not in pos:
                continue
                
            label = node['label']
            if label not in node_traces:
                color_idx = len(node_traces) % len(color_map)
                node_traces[label] = {
                    'x': [], 'y': [], 'text': [], 'ids': [],
                    'color': color_map[color_idx]
                }
            
            x, y = pos[node['id']]
            node_traces[label]['x'].append(x)
            node_traces[label]['y'].append(y)
            node_traces[label]['text'].append(f"{node['name']}<br>Type: {label}<br>ID: {node['id']}")
            node_traces[label]['ids'].append(node['id'])
        
        # Create figure
        fig = go.Figure()
        
        # Add edge trace
        fig.add_trace(edge_trace)
        
        # Add node traces
        for label, data in node_traces.items():
            fig.add_trace(go.Scatter(
                x=data['x'], y=data['y'],
                mode='markers+text',
                marker=dict(
                    size=10,
                    color=data['color'],
                    line=dict(width=2, color='white')
                ),
                text=[name.split('<br>')[0] for name in data['text']],
                textposition="middle center",
                textfont=dict(size=8),
                hovertext=data['text'],
                hoverinfo='text',
                name=label
            ))
        
        fig.update_layout(
            title="Knowledge Graph Visualization",
            titlefont_size=16,
            showlegend=True,
            hovermode='closest',
            margin=dict(b=20,l=5,r=5,t=40),
            annotations=[ dict(
                text="Interactive graph visualization of your knowledge base",
                showarrow=False,
                xref="paper", yref="paper",
                x=0.005, y=-0.002,
                xanchor="left", yanchor="bottom",
                font=dict(color="#888", size=12)
            )],
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='white'
        )
        
        return fig

    def get_graph_statistics(self):
        """Get statistics about the graph"""
        if not self.nx_graph:
            return {}
        
        stats = {
            "total_nodes": self.nx_graph.number_of_nodes(),
            "total_edges": self.nx_graph.number_of_edges(),
            "node_types": {},
            "edge_types": {},
            "connected_components": nx.number_connected_components(self.nx_graph),
            "average_degree": sum(dict(self.nx_graph.degree()).values()) / self.nx_graph.number_of_nodes() if self.nx_graph.number_of_nodes() > 0 else 0
        }
        
        # Count node types
        for node in self.graph_data["nodes"]:
            label = node['label']
            stats["node_types"][label] = stats["node_types"].get(label, 0) + 1
        
        # Count edge types
        for edge in self.graph_data["edges"]:
            label = edge['label']
            stats["edge_types"][label] = stats["edge_types"].get(label, 0) + 1
        
        return stats

# Initialize the visualizer
print("🚀 Initializing Graph Visualizer...")
try:
    visualizer = GraphVisualizer()
    
    # Query initial data
    print("📊 Loading graph data...")
    graph_data = visualizer.query_graph_data(limit=500)  # Adjust limit as needed
    
    # Get statistics
    stats = visualizer.get_graph_statistics()
    
except Exception as e:
    print(f"❌ Initialization failed: {e}")
    print("⚠️ Make sure to create a .env file with your credentials")
    visualizer = None
    graph_data = {"nodes": [], "edges": []}
    stats = {}

# Create Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("🏛️ City Clerk Knowledge Graph Visualizer", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': '30px'}),
    
    # Statistics panel
    html.Div([
        html.H3("📊 Graph Statistics", style={'color': '#34495e'}),
        html.Div(id='stats-content')
    ], style={'backgroundColor': '#ecf0f1', 'padding': '20px', 'borderRadius': '10px', 'marginBottom': '20px'}),
    
    # Controls
    html.Div([
        html.Label("Layout Algorithm:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
        dcc.Dropdown(
            id='layout-dropdown',
            options=[
                {'label': 'Spring Layout', 'value': 'spring'},
                {'label': 'Circular Layout', 'value': 'circular'},
                {'label': 'Random Layout', 'value': 'random'}
            ],
            value='spring',
            style={'width': '200px', 'display': 'inline-block'}
        ),
        html.Button('🔄 Refresh Data', id='refresh-button', 
                   style={'marginLeft': '20px', 'padding': '10px 20px', 'backgroundColor': '#3498db', 'color': 'white', 'border': 'none', 'borderRadius': '5px'})
    ], style={'marginBottom': '20px'}),
    
    # Graph visualization
    dcc.Graph(id='graph-plot', style={'height': '700px'}),
    
    # Data tables
    html.Div([
        html.H3("📋 Node Details", style={'color': '#34495e'}),
        dash_table.DataTable(
            id='nodes-table',
            columns=[
                {'name': 'ID', 'id': 'id'},
                {'name': 'Name', 'id': 'name'},
                {'name': 'Type', 'id': 'label'}
            ],
            data=[],
            page_size=10,
            style_cell={'textAlign': 'left'},
            style_header={'backgroundColor': '#3498db', 'color': 'white', 'fontWeight': 'bold'}
        )
    ], style={'marginTop': '30px'})
])

@app.callback(
    [Output('graph-plot', 'figure'),
     Output('stats-content', 'children'),
     Output('nodes-table', 'data')],
    [Input('layout-dropdown', 'value'),
     Input('refresh-button', 'n_clicks')]
)
def update_visualization(layout_type, n_clicks):
    global visualizer, graph_data, stats
    
    # Refresh data if button clicked
    if n_clicks and visualizer:
        try:
            graph_data = visualizer.query_graph_data(limit=500)
            stats = visualizer.get_graph_statistics()
        except Exception as e:
            print(f"❌ Refresh failed: {e}")
    
    # Create visualization
    if visualizer and graph_data["nodes"]:
        fig = visualizer.create_plotly_visualization(layout_type)
    else:
        fig = go.Figure()
        fig.add_annotation(
            text="No graph data available or connection failed<br>Check your database connection and .env file",
            xref="paper", yref="paper",
            x=0.5, y=0.5, xanchor='center', yanchor='middle',
            font=dict(size=16, color="red")
        )
    
    # Create statistics content
    if stats:
        stats_content = [
            html.P(f"🔵 Total Nodes: {stats.get('total_nodes', 0)}"),
            html.P(f"🔗 Total Edges: {stats.get('total_edges', 0)}"),
            html.P(f"🌐 Connected Components: {stats.get('connected_components', 0)}"),
            html.P(f"📊 Average Degree: {stats.get('average_degree', 0):.2f}"),
            html.H4("Node Types:", style={'marginTop': '15px'}),
            html.Ul([html.Li(f"{node_type}: {count}") for node_type, count in stats.get('node_types', {}).items()]),
            html.H4("Edge Types:", style={'marginTop': '15px'}),
            html.Ul([html.Li(f"{edge_type}: {count}") for edge_type, count in stats.get('edge_types', {}).items()])
        ]
    else:
        stats_content = [html.P("No statistics available")]
    
    # Create table data
    table_data = []
    for node in graph_data.get("nodes", []):
        table_data.append({
            'id': node['id'],
            'name': node.get('name', node['id']),
            'label': node['label']
        })
    
    return fig, stats_content, table_data

if __name__ == '__main__':
    print("🌐 Starting web server...")
    print("📱 Open your browser to: http://localhost:8050")
    if not COSMOS_KEY:
        print("⚠️ Warning: COSMOS_KEY not found in environment variables")
        print("   Please create a .env file with your Azure Cosmos DB credentials")
    app.run_server(debug=True, host='localhost', port=8050) 