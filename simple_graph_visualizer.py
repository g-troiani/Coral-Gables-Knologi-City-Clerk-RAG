#!/usr/bin/env python3
"""
Simple Graph Database Visualizer for Azure Cosmos DB Gremlin API
Uses Flask + basic HTML/JavaScript for visualization
"""

import json
import time
import traceback
from typing import Dict, List
import networkx as nx
from flask import Flask, render_template_string, jsonify, request
from gremlin_python.driver import client, serializer
from config import (
    COSMOS_ENDPOINT, COSMOS_KEY, DATABASE, CONTAINER, 
    PARTITION_KEY, PARTITION_VALUE, validate_config
)

class SimpleGraphVisualizer:
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
            vertices_result = self.gremlin_client.submit(vertices_query).all().result()
            
            # Query edges  
            edges_query = f"g.E().limit({limit})"
            edges_result = self.gremlin_client.submit(edges_query).all().result()
            
            # Process vertices
            nodes = []
            for vertex in vertices_result:
                try:
                    vertex_data = vertex
                    node = {
                        'id': str(vertex_data.get('id', '')),
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
                    edge_data = edge
                    edge_info = {
                        'id': str(edge_data.get('id', '')),
                        'label': edge_data.get('label', 'RELATED'),
                        'source': str(edge_data.get('outV', '')),
                        'target': str(edge_data.get('inV', '')),
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

    def get_layout_positions(self, layout_type="spring"):
        """Calculate node positions using NetworkX"""
        if not self.nx_graph.nodes():
            return {}
            
        try:
            if layout_type == "spring":
                pos = nx.spring_layout(self.nx_graph, k=1, iterations=50)
            elif layout_type == "circular":
                pos = nx.circular_layout(self.nx_graph)
            elif layout_type == "random":
                pos = nx.random_layout(self.nx_graph)
            else:
                pos = nx.spring_layout(self.nx_graph)
            
            # Convert to serializable format and scale for display
            scaled_pos = {}
            for node_id, (x, y) in pos.items():
                scaled_pos[str(node_id)] = {
                    'x': float(x) * 400 + 400,  # Scale and center
                    'y': float(y) * 400 + 300
                }
            return scaled_pos
        except Exception as e:
            print(f"Error calculating layout: {e}")
            return {}

# Initialize the visualizer
print("🚀 Initializing Simple Graph Visualizer...")
try:
    visualizer = SimpleGraphVisualizer()
    
    # Query initial data
    print("📊 Loading graph data...")
    graph_data = visualizer.query_graph_data(limit=200)  # Smaller limit for faster loading
    
    # Get statistics
    stats = visualizer.get_graph_statistics()
    print(f"📈 Graph loaded: {stats.get('total_nodes', 0)} nodes, {stats.get('total_edges', 0)} edges")
    
except Exception as e:
    print(f"❌ Initialization failed: {e}")
    print("⚠️ Make sure your .env file has the correct credentials")
    visualizer = None
    graph_data = {"nodes": [], "edges": []}
    stats = {}

# Create Flask app
app = Flask(__name__)

# HTML template with embedded JavaScript visualization
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🏛️ City Clerk Knowledge Graph</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .header {
            text-align: center;
            color: #2c3e50;
            margin-bottom: 30px;
        }
        .controls {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .stats {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .graph-container {
            background: white;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            overflow: hidden;
        }
        #graph {
            width: 100%;
            height: 600px;
            border: 1px solid #ddd;
            cursor: grab;
        }
        #graph:active {
            cursor: grabbing;
        }
        .btn {
            background-color: #3498db;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 5px;
            cursor: pointer;
            margin-right: 10px;
        }
        .btn:hover {
            background-color: #2980b9;
        }
        select {
            padding: 8px;
            border-radius: 4px;
            border: 1px solid #ddd;
            margin-right: 10px;
        }
        .node {
            cursor: pointer;
            transition: all 0.2s ease;
        }
        .node:hover {
            stroke-width: 3;
            filter: drop-shadow(2px 2px 4px rgba(0,0,0,0.3));
        }
        .node.selected {
            stroke: #ff6b35 !important;
            stroke-width: 4 !important;
            filter: drop-shadow(0 0 8px #ff6b35);
        }
        .node.connected {
            stroke: #27ae60 !important;
            stroke-width: 3 !important;
        }
        .edge {
            stroke: #999;
            stroke-width: 1;
            transition: all 0.2s ease;
        }
        .edge:hover {
            stroke: #3498db;
            stroke-width: 2;
        }
        .edge.highlighted {
            stroke: #e74c3c;
            stroke-width: 3;
        }
        .edge-label {
            font-size: 10px;
            font-family: Arial;
            fill: #666;
            text-anchor: middle;
            pointer-events: none;
            opacity: 0.8;
        }
        .tooltip {
            position: absolute;
            background: rgba(0,0,0,0.8);
            color: white;
            padding: 8px;
            border-radius: 4px;
            pointer-events: none;
            font-size: 12px;
            z-index: 1000;
            max-width: 300px;
        }
        .node-info {
            position: fixed;
            top: 20px;
            right: 20px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            max-width: 300px;
            display: none;
            z-index: 1001;
        }
        .node-info h4 {
            margin: 0 0 10px 0;
            color: #2c3e50;
        }
        .connections {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 5px;
            margin-top: 10px;
        }
        .connection-item {
            margin: 5px 0;
            padding: 3px 6px;
            background: #e9ecef;
            border-radius: 3px;
            font-size: 11px;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🏛️ City Clerk Knowledge Graph Visualizer</h1>
        <p>Interactive visualization - Drag nodes, zoom with mouse wheel, pan by dragging empty space!</p>
    </div>
    
    <div class="stats">
        <h3>📊 Graph Statistics</h3>
        <div id="stats-content">
            <p>🔵 Total Nodes: {{ stats.total_nodes }}</p>
            <p>🔗 Total Edges: {{ stats.total_edges }}</p>
            <p>🌐 Connected Components: {{ stats.connected_components }}</p>
            <p>📊 Average Degree: {{ "%.2f"|format(stats.average_degree) }}</p>
            
            {% if stats.node_types %}
            <h4>Node Types:</h4>
            <ul>
                {% for node_type, count in stats.node_types.items() %}
                <li>{{ node_type }}: {{ count }}</li>
                {% endfor %}
            </ul>
            {% endif %}
            
            {% if stats.edge_types %}
            <h4>Edge Types:</h4>
            <ul>
                {% for edge_type, count in stats.edge_types.items() %}
                <li>{{ edge_type }}: {{ count }}</li>
                {% endfor %}
            </ul>
            {% endif %}
        </div>
    </div>
    
    <div class="controls">
        <label>Layout: </label>
        <select id="layout-select">
            <option value="spring">Spring Layout</option>
            <option value="circular">Circular Layout</option>
            <option value="random">Random Layout</option>
        </select>
        <button class="btn" onclick="refreshData()">🔄 Refresh Data</button>
        <button class="btn" onclick="updateLayout()">🎨 Update Layout</button>
        <button class="btn" onclick="showEdgeLabels = !showEdgeLabels; updateVisualization()">👁️ Toggle Edge Labels</button>
        <button class="btn" onclick="resetSelection()">🔄 Reset Selection</button>
        <button class="btn" onclick="fitToView()">🔍 Fit to View</button>
        <button class="btn" onclick="zoomIn()">🔍+ Zoom In</button>
        <button class="btn" onclick="zoomOut()">🔍- Zoom Out</button>
        <button class="btn" onclick="resetView()">🏠 Reset View</button>
    </div>
    
    <div class="graph-container">
        <svg id="graph"></svg>
    </div>
    
    <div class="tooltip" id="tooltip" style="display: none;"></div>
    <div class="node-info" id="node-info">
        <div id="node-details"></div>
    </div>

    <script>
        // Graph data from server
        let graphData = {{ graph_data | tojson }};
        let currentLayout = 'spring';
        let showEdgeLabels = false;
        let selectedNode = null;
        let draggedNode = null;
        let isDragging = false;
        let nodePositions = {};
        
        // Zoom and pan variables
        let zoomScale = 1;
        let panX = 0;
        let panY = 0;
        let isPanning = false;
        let lastPanX = 0;
        let lastPanY = 0;
        
        // Interactive functionality
        function updateVisualization() {
            const svg = document.getElementById('graph');
            const rect = svg.getBoundingClientRect();
            svg.setAttribute('width', rect.width);
            svg.setAttribute('height', '600');
            
            // Clear previous content
            svg.innerHTML = '';
            
            // Get layout positions
            fetch('/api/layout?type=' + currentLayout)
                .then(response => response.json())
                .then(positions => {
                    nodePositions = positions;
                    drawGraph(svg, graphData, positions);
                    // Auto-fit to view after loading
                    setTimeout(() => fitToView(), 100);
                })
                .catch(error => {
                    console.error('Error getting layout:', error);
                    drawGraph(svg, graphData, {});
                });
        }
        
        function drawGraph(svg, data, positions) {
            const tooltip = document.getElementById('tooltip');
            
            // Clear previous content
            svg.innerHTML = '';
            
            // Create main transform group for zoom and pan
            const mainGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            mainGroup.setAttribute('id', 'main-group');
            updateTransform(mainGroup);
            svg.appendChild(mainGroup);
            
            // Create edges group
            const edgesGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            edgesGroup.setAttribute('id', 'edges');
            mainGroup.appendChild(edgesGroup);
            
            // Create nodes group
            const nodesGroup = document.createElementNS('http://www.w3.org/2000/svg', 'g');
            nodesGroup.setAttribute('id', 'nodes');
            mainGroup.appendChild(nodesGroup);
            
            // Draw edges first (so they appear behind nodes)
            data.edges.forEach((edge, index) => {
                const sourcePos = positions[edge.source];
                const targetPos = positions[edge.target];
                
                if (sourcePos && targetPos) {
                    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
                    line.setAttribute('x1', sourcePos.x);
                    line.setAttribute('y1', sourcePos.y);
                    line.setAttribute('x2', targetPos.x);
                    line.setAttribute('y2', targetPos.y);
                    line.setAttribute('class', 'edge');
                    line.setAttribute('data-source', edge.source);
                    line.setAttribute('data-target', edge.target);
                    line.setAttribute('data-label', edge.label);
                    
                    // Add click event to edge
                    line.addEventListener('click', (e) => {
                        e.stopPropagation();
                        showEdgeInfo(edge);
                    });
                    
                    // Add hover event to edge
                    line.addEventListener('mouseenter', (e) => {
                        tooltip.style.display = 'block';
                        tooltip.style.left = (e.pageX + 10) + 'px';
                        tooltip.style.top = (e.pageY - 10) + 'px';
                        tooltip.innerHTML = `
                            <strong>Relationship: ${edge.label}</strong><br>
                            From: ${getNodeName(edge.source)}<br>
                            To: ${getNodeName(edge.target)}
                        `;
                        line.classList.add('highlighted');
                    });
                    
                    line.addEventListener('mouseleave', () => {
                        tooltip.style.display = 'none';
                        line.classList.remove('highlighted');
                    });
                    
                    edgesGroup.appendChild(line);
                    
                    // Add edge label if enabled
                    if (showEdgeLabels) {
                        const midX = (sourcePos.x + targetPos.x) / 2;
                        const midY = (sourcePos.y + targetPos.y) / 2;
                        
                        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                        text.setAttribute('x', midX);
                        text.setAttribute('y', midY);
                        text.setAttribute('class', 'edge-label');
                        text.textContent = edge.label;
                        edgesGroup.appendChild(text);
                    }
                }
            });
            
            // Color map for node types
            const colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', '#1abc9c', '#34495e', '#f1c40f'];
            const nodeTypes = [...new Set(data.nodes.map(n => n.label))];
            const colorMap = {};
            nodeTypes.forEach((type, i) => {
                colorMap[type] = colors[i % colors.length];
            });
            
            // Draw nodes
            data.nodes.forEach(node => {
                const pos = positions[node.id];
                if (!pos) return;
                
                const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
                circle.setAttribute('cx', pos.x);
                circle.setAttribute('cy', pos.y);
                circle.setAttribute('r', 10);
                circle.setAttribute('fill', colorMap[node.label] || '#95a5a6');
                circle.setAttribute('stroke', 'white');
                circle.setAttribute('stroke-width', '2');
                circle.setAttribute('class', 'node');
                circle.setAttribute('data-id', node.id);
                
                // Make nodes draggable and clickable
                circle.addEventListener('mousedown', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    if (e.button === 0) { // Left mouse button only
                        isDragging = true;
                        draggedNode = node.id;
                        circle.style.cursor = 'grabbing';
                        console.log('Started dragging node:', node.id);
                    }
                });
                
                circle.addEventListener('click', (e) => {
                    e.stopPropagation();
                    if (!isDragging) {
                        selectNode(node);
                    }
                });
                
                // Add hover effects
                circle.addEventListener('mouseenter', (e) => {
                    if (!isDragging) {
                        tooltip.style.display = 'block';
                        tooltip.style.left = (e.pageX + 10) + 'px';
                        tooltip.style.top = (e.pageY - 10) + 'px';
                        tooltip.innerHTML = `
                            <strong>${node.name}</strong><br>
                            Type: ${node.label}<br>
                            ID: ${node.id}<br>
                            <em>LEFT-CLICK and drag to move</em>
                        `;
                    }
                });
                
                circle.addEventListener('mouseleave', () => {
                    if (!isDragging) {
                        tooltip.style.display = 'none';
                    }
                });
                
                nodesGroup.appendChild(circle);
                
                // Add text label
                if (node.name && node.name.length < 25) {
                    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
                    text.setAttribute('x', pos.x);
                    text.setAttribute('y', pos.y + 25);
                    text.setAttribute('text-anchor', 'middle');
                    text.setAttribute('font-size', '10px');
                    text.setAttribute('font-family', 'Arial');
                    text.setAttribute('pointer-events', 'none');
                    text.textContent = node.name.substring(0, 20);
                    nodesGroup.appendChild(text);
                }
            });
            
            // Mouse event handlers
            function handleMouseMove(e) {
                if (isDragging && draggedNode) {
                    e.preventDefault();
                    // Convert screen coordinates to SVG coordinates considering zoom and pan
                    const rect = svg.getBoundingClientRect();
                    const screenX = e.clientX - rect.left;
                    const screenY = e.clientY - rect.top;
                    const newX = (screenX - panX) / zoomScale;
                    const newY = (screenY - panY) / zoomScale;
                    
                    // Update node position
                    nodePositions[draggedNode] = { x: newX, y: newY };
                    
                    // Update the visual position immediately
                    const draggedElement = document.querySelector(`[data-id="${draggedNode}"]`);
                    if (draggedElement) {
                        draggedElement.setAttribute('cx', newX);
                        draggedElement.setAttribute('cy', newY);
                        
                        // Update text label position
                        const textElement = draggedElement.parentNode.querySelector(`text[x="${nodePositions[draggedNode].x}"]`);
                        if (textElement) {
                            textElement.setAttribute('x', newX);
                            textElement.setAttribute('y', newY + 25);
                        }
                        
                        // Update connected edges
                        data.edges.forEach(edge => {
                            if (edge.source === draggedNode || edge.target === draggedNode) {
                                const edgeElement = document.querySelector(`[data-source="${edge.source}"][data-target="${edge.target}"]`);
                                if (edgeElement) {
                                    const sourcePos = nodePositions[edge.source];
                                    const targetPos = nodePositions[edge.target];
                                    if (sourcePos && targetPos) {
                                        edgeElement.setAttribute('x1', sourcePos.x);
                                        edgeElement.setAttribute('y1', sourcePos.y);
                                        edgeElement.setAttribute('x2', targetPos.x);
                                        edgeElement.setAttribute('y2', targetPos.y);
                                    }
                                }
                            }
                        });
                    }
                } else if (isPanning) {
                    e.preventDefault();
                    const rect = svg.getBoundingClientRect();
                    const currentX = e.clientX - rect.left;
                    const currentY = e.clientY - rect.top;
                    
                    panX += currentX - lastPanX;
                    panY += currentY - lastPanY;
                    
                    lastPanX = currentX;
                    lastPanY = currentY;
                    
                    updateTransform(mainGroup);
                }
            }
            
            function handleMouseUp(e) {
                if (isDragging) {
                    isDragging = false;
                    draggedNode = null;
                    document.querySelectorAll('.node').forEach(n => n.style.cursor = 'pointer');
                    console.log('Stopped dragging');
                } else if (isPanning) {
                    isPanning = false;
                    svg.style.cursor = 'grab';
                }
            }
            
            function handleMouseDown(e) {
                if (e.target === svg || e.target === mainGroup || e.target.tagName === 'line') {
                    e.preventDefault();
                    isPanning = true;
                    const rect = svg.getBoundingClientRect();
                    lastPanX = e.clientX - rect.left;
                    lastPanY = e.clientY - rect.top;
                    svg.style.cursor = 'grabbing';
                }
            }
            
            // Remove existing event listeners to avoid duplicates
            svg.removeEventListener('mousemove', handleMouseMove);
            svg.removeEventListener('mouseup', handleMouseUp);
            svg.removeEventListener('mousedown', handleMouseDown);
            document.removeEventListener('mousemove', handleMouseMove);
            document.removeEventListener('mouseup', handleMouseUp);
            
            // Add event listeners
            svg.addEventListener('mousemove', handleMouseMove);
            svg.addEventListener('mouseup', handleMouseUp);
            svg.addEventListener('mousedown', handleMouseDown);
            document.addEventListener('mousemove', handleMouseMove);
            document.addEventListener('mouseup', handleMouseUp);
            
            // Zoom functionality
            svg.addEventListener('wheel', (e) => {
                e.preventDefault();
                const rect = svg.getBoundingClientRect();
                const mouseX = e.clientX - rect.left;
                const mouseY = e.clientY - rect.top;
                
                const zoomFactor = e.deltaY > 0 ? 0.9 : 1.1;
                const newScale = Math.max(0.1, Math.min(5, zoomScale * zoomFactor));
                
                // Zoom towards mouse position
                panX = mouseX - (mouseX - panX) * (newScale / zoomScale);
                panY = mouseY - (mouseY - panY) * (newScale / zoomScale);
                
                zoomScale = newScale;
                updateTransform(mainGroup);
            });
            
            // Click on empty space to deselect
            svg.addEventListener('click', (e) => {
                if (e.target === svg || e.target === mainGroup) {
                    resetSelection();
                }
            });
        }
        
        function updateTransform(group) {
            group.setAttribute('transform', `translate(${panX}, ${panY}) scale(${zoomScale})`);
        }
        
        function getNodeName(nodeId) {
            const node = graphData.nodes.find(n => n.id === nodeId);
            return node ? node.name : nodeId;
        }
        
        function selectNode(node) {
            selectedNode = node;
            
            // Update visual selection
            document.querySelectorAll('.node').forEach(n => {
                n.classList.remove('selected', 'connected');
            });
            document.querySelectorAll('.edge').forEach(e => {
                e.classList.remove('highlighted');
            });
            
            // Highlight selected node
            const selectedElement = document.querySelector(`[data-id="${node.id}"]`);
            if (selectedElement) {
                selectedElement.classList.add('selected');
            }
            
            // Find and highlight connected nodes and edges
            const connections = [];
            graphData.edges.forEach(edge => {
                if (edge.source === node.id || edge.target === node.id) {
                    // Highlight edge
                    const edgeElement = document.querySelector(`[data-source="${edge.source}"][data-target="${edge.target}"]`);
                    if (edgeElement) {
                        edgeElement.classList.add('highlighted');
                    }
                    
                    // Highlight connected node
                    const connectedNodeId = edge.source === node.id ? edge.target : edge.source;
                    const connectedElement = document.querySelector(`[data-id="${connectedNodeId}"]`);
                    if (connectedElement) {
                        connectedElement.classList.add('connected');
                    }
                    
                    // Add to connections list
                    connections.push({
                        nodeId: connectedNodeId,
                        nodeName: getNodeName(connectedNodeId),
                        relationship: edge.label,
                        direction: edge.source === node.id ? 'outgoing' : 'incoming'
                    });
                }
            });
            
            // Show node info panel
            showNodeInfo(node, connections);
        }
        
        function showNodeInfo(node, connections) {
            const nodeInfo = document.getElementById('node-info');
            const nodeDetails = document.getElementById('node-details');
            
            let connectionsHtml = '';
            if (connections.length > 0) {
                connectionsHtml = `
                    <div class="connections">
                        <strong>Connections (${connections.length}):</strong>
                        ${connections.map(conn => `
                            <div class="connection-item">
                                ${conn.direction === 'outgoing' ? '→' : '←'} 
                                <strong>${conn.relationship}</strong> 
                                ${conn.nodeName}
                            </div>
                        `).join('')}
                    </div>
                `;
            }
            
            nodeDetails.innerHTML = `
                <h4>${node.name}</h4>
                <p><strong>Type:</strong> ${node.label}</p>
                <p><strong>ID:</strong> ${node.id}</p>
                ${connectionsHtml}
                <button onclick="resetSelection()" style="margin-top: 10px; padding: 5px 10px; background: #e74c3c; color: white; border: none; border-radius: 3px; cursor: pointer;">Close</button>
            `;
            
            nodeInfo.style.display = 'block';
        }
        
        function showEdgeInfo(edge) {
            const tooltip = document.getElementById('tooltip');
            tooltip.style.display = 'block';
            tooltip.style.left = '50%';
            tooltip.style.top = '50%';
            tooltip.style.transform = 'translate(-50%, -50%)';
            tooltip.innerHTML = `
                <strong>Edge Details</strong><br>
                <strong>Relationship:</strong> ${edge.label}<br>
                <strong>From:</strong> ${getNodeName(edge.source)}<br>
                <strong>To:</strong> ${getNodeName(edge.target)}<br>
                <button onclick="document.getElementById('tooltip').style.display='none'" style="margin-top: 5px; padding: 3px 8px; background: #3498db; color: white; border: none; border-radius: 3px; cursor: pointer;">Close</button>
            `;
        }
        
        function resetSelection() {
            selectedNode = null;
            document.getElementById('node-info').style.display = 'none';
            document.getElementById('tooltip').style.display = 'none';
            
            document.querySelectorAll('.node').forEach(n => {
                n.classList.remove('selected', 'connected');
            });
            document.querySelectorAll('.edge').forEach(e => {
                e.classList.remove('highlighted');
            });
        }
        
        // Zoom and navigation functions
        function zoomIn() {
            const svg = document.getElementById('graph');
            const rect = svg.getBoundingClientRect();
            const centerX = rect.width / 2;
            const centerY = rect.height / 2;
            
            const zoomFactor = 1.2;
            const newScale = Math.min(5, zoomScale * zoomFactor);
            
            panX = centerX - (centerX - panX) * (newScale / zoomScale);
            panY = centerY - (centerY - panY) * (newScale / zoomScale);
            
            zoomScale = newScale;
            const mainGroup = document.getElementById('main-group');
            if (mainGroup) updateTransform(mainGroup);
        }
        
        function zoomOut() {
            const svg = document.getElementById('graph');
            const rect = svg.getBoundingClientRect();
            const centerX = rect.width / 2;
            const centerY = rect.height / 2;
            
            const zoomFactor = 0.8;
            const newScale = Math.max(0.1, zoomScale * zoomFactor);
            
            panX = centerX - (centerX - panX) * (newScale / zoomScale);
            panY = centerY - (centerY - panY) * (newScale / zoomScale);
            
            zoomScale = newScale;
            const mainGroup = document.getElementById('main-group');
            if (mainGroup) updateTransform(mainGroup);
        }
        
        function resetView() {
            zoomScale = 1;
            panX = 0;
            panY = 0;
            const mainGroup = document.getElementById('main-group');
            if (mainGroup) updateTransform(mainGroup);
        }
        
        function fitToView() {
            if (!graphData.nodes || graphData.nodes.length === 0) return;
            
            // Find bounds of all nodes
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            
            Object.values(nodePositions).forEach(pos => {
                minX = Math.min(minX, pos.x);
                minY = Math.min(minY, pos.y);
                maxX = Math.max(maxX, pos.x);
                maxY = Math.max(maxY, pos.y);
            });
            
            if (minX === Infinity) return; // No valid positions
            
            const svg = document.getElementById('graph');
            const rect = svg.getBoundingClientRect();
            const padding = 50; // Padding around the graph
            
            const graphWidth = maxX - minX;
            const graphHeight = maxY - minY;
            const viewWidth = rect.width - 2 * padding;
            const viewHeight = rect.height - 2 * padding;
            
            // Calculate scale to fit
            const scaleX = viewWidth / graphWidth;
            const scaleY = viewHeight / graphHeight;
            zoomScale = Math.min(scaleX, scaleY, 2); // Max zoom of 2x
            
            // Center the graph
            const graphCenterX = (minX + maxX) / 2;
            const graphCenterY = (minY + maxY) / 2;
            const viewCenterX = rect.width / 2;
            const viewCenterY = rect.height / 2;
            
            panX = viewCenterX - graphCenterX * zoomScale;
            panY = viewCenterY - graphCenterY * zoomScale;
            
            const mainGroup = document.getElementById('main-group');
            if (mainGroup) updateTransform(mainGroup);
        }
        
        function updateLayout() {
            currentLayout = document.getElementById('layout-select').value;
            updateVisualization();
        }
        
        function refreshData() {
            fetch('/api/refresh')
                .then(response => response.json())
                .then(data => {
                    graphData = data;
                    resetSelection();
                    updateVisualization();
                    location.reload(); // Refresh stats
                })
                .catch(error => {
                    console.error('Error refreshing data:', error);
                });
        }
        
        // Initialize visualization
        updateVisualization();
        
        // Handle window resize
        window.addEventListener('resize', updateVisualization);
        
        // Prevent text selection during drag
        document.addEventListener('selectstart', (e) => {
            if (isDragging) e.preventDefault();
        });
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    """Main page with graph visualization"""
    return render_template_string(HTML_TEMPLATE, 
                                graph_data=graph_data, 
                                stats=stats)

@app.route('/api/layout')
def api_layout():
    """API endpoint to get node layout positions"""
    layout_type = request.args.get('type', 'spring')
    if visualizer:
        positions = visualizer.get_layout_positions(layout_type)
        return jsonify(positions)
    return jsonify({})

@app.route('/api/refresh')
def api_refresh():
    """API endpoint to refresh graph data"""
    global graph_data, stats
    if visualizer:
        try:
            graph_data = visualizer.query_graph_data(limit=200)
            stats = visualizer.get_graph_statistics()
            return jsonify(graph_data)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"error": "Visualizer not initialized"}), 500

if __name__ == '__main__':
    print("🌐 Starting Simple Graph Visualizer...")
    print("📱 Open your browser to: http://localhost:8050")
    if not COSMOS_KEY:
        print("⚠️ Warning: COSMOS_KEY not found in environment variables")
        print("   Please check your .env file")
    
    app.run(debug=True, host='localhost', port=8050) 