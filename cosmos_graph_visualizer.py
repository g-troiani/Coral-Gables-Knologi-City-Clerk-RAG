#!/usr/bin/env python3
"""
Cosmos DB Graph Visualizer
Connects to Cosmos DB and creates an interactive visualization of the entire graph.
Click on nodes to see their attributes.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any
from gremlin_python.driver import client, serializer

import webbrowser
from dotenv import load_dotenv
import html

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


class CosmosGraphVisualizer:
    """Visualizes the complete graph from Cosmos DB."""
    
    def __init__(self):
        """Initialize with Cosmos DB connection."""
        print("🔧 INITIALIZING CosmosGraphVisualizer...")
        log.info("🔧 INITIALIZING CosmosGraphVisualizer...")
        
        # Load environment variables from .env file
        load_dotenv()
        print("✅ Environment variables loaded")
        
        # Load Cosmos DB configuration from environment
        self.endpoint = os.getenv("COSMOS_ENDPOINT", "").rstrip('/')
        self.key = os.getenv("COSMOS_KEY", "").strip(' "')
        self.database = os.getenv("DATABASE", "cgGraph").strip(' "')
        self.container = os.getenv("CONTAINER", "cityClerk").strip(' "')
        
        print(f"🔍 COSMOS_ENDPOINT: '{self.endpoint}'")
        print(f"🔍 COSMOS_KEY: {'*' * 10 + self.key[-4:] if self.key else 'MISSING'}")
        print(f"🔍 DATABASE: '{self.database}'")
        print(f"🔍 CONTAINER: '{self.container}'")
        
        if not self.endpoint or not self.key:
            raise ValueError("COSMOS_ENDPOINT and COSMOS_KEY must be set in .env file")
        
        # Initialize Gremlin client
        print("🔗 Connecting to Cosmos DB...")
        self.client = client.Client(
            f"{self.endpoint}/gremlin",
            'g',
            username=f"/dbs/{self.database}/colls/{self.container}",
            password=self.key,
            message_serializer=serializer.GraphSONSerializersV2d0()
        )
        print("✅ Connected to Cosmos DB")
        
        # Entity type colors
        self.entity_colors = {
            'Person': '#FF6B6B',
            'Organization': '#4ECDC4',
            'Document': '#45B7D1',
            'Policy': '#F7B731',
            'Event': '#5F27CD',
            'Action': '#00D2D3',
            'Asset': '#FF9F43',
            'Project': '#54A0FF',
            'Location': '#48DBFB',
            'Role': '#FF6348',
            'Topic': '#B983FF',
            'AgendaItem': '#FD79A8',
            'agendaitem': '#FD79A8',  # Lowercase version from Cosmos
            'Agendaitem': '#FD79A8',  # Handle title case version
            'Event': '#5F27CD',
            'event': '#5F27CD',      # Lowercase version from Cosmos
            'Contract': '#A29BFE',
            'Technology': '#6C5CE7',
            'VoteOutcome': '#FDCB6E',
            'Voteoutcome': '#FDCB6E',  # Handle title case version
            'Meeting': '#00B894',
            'Section': '#E17055',
            'topic': '#B983FF',      # Lowercase version from Cosmos
            'action': '#00D2D3',     # Lowercase version from Cosmos
            'asset': '#FF9F43',      # Lowercase version from Cosmos
            'document': '#74B9FF',   # Lowercase version from Cosmos
            'organization': '#81ECEC', # Lowercase version from Cosmos
            'person': '#55A3FF',     # Lowercase version from Cosmos
            'policy': '#F7B731',     # Lowercase version from Cosmos
            'project': '#54A0FF',    # Lowercase version from Cosmos
            'location': '#48DBFB',   # Lowercase version from Cosmos
            'role': '#FF6348'        # Lowercase version from Cosmos
        }
    
    def fetch_all_vertices(self) -> List[Dict[str, Any]]:
        """Fetch all vertices from Cosmos DB."""
        print("📊 Fetching vertices from Cosmos DB...")
        log.info("Fetching all vertices from Cosmos DB...")
        
        # First, get the total count of all vertices
        count_query = "g.V().count()"
        try:
            total_count_result = self.client.submit(count_query).all().result()
            total_count = total_count_result[0] if total_count_result else 0
            print(f"🔢 Total vertices in Cosmos DB: {total_count}")
        except Exception as e:
            print(f"⚠️ Could not get total count: {e}")
            total_count = "Unknown"
        
        query = "g.V().valueMap(true)"
        
        try:
            result = self.client.submit(query).all().result()
            vertices = []
            
            for item in result:
                # Handle both string and list formats for label (Cosmos DB inconsistency)
                label = item['label']
                if isinstance(label, list) and len(label) > 0:
                    label = label[0]
                elif not isinstance(label, str):
                    label = str(label)
                
                vertex = {
                    'id': item['id'],
                    'label': label,
                    'properties': {}
                }
                
                # Extract properties
                for key, value in item.items():
                    if key not in ['id', 'label']:
                        # Gremlin returns properties as lists, but Cosmos might return strings
                        if isinstance(value, list) and len(value) > 0:
                            vertex['properties'][key] = value[0]
                        else:
                            vertex['properties'][key] = value
                
                vertices.append(vertex)
            
            print(f"✅ Fetched {len(vertices)} vertices")
            if total_count != "Unknown" and len(vertices) < total_count:
                print(f"⚠️  WARNING: Only showing {len(vertices)} of {total_count} total vertices!")
            log.info(f"Fetched {len(vertices)} vertices")
            return vertices
            
        except Exception as e:
            print(f"❌ Error fetching vertices: {e}")
            log.error(f"Error fetching vertices: {e}")
            return []
    
    def fetch_all_edges(self) -> List[Dict[str, Any]]:
        """Fetch all edges from Cosmos DB."""
        print("📊 Fetching edges from Cosmos DB...")
        log.info("Fetching all edges from Cosmos DB...")
        
        # First, get the total count of all edges
        count_query = "g.E().count()"
        try:
            total_edge_count_result = self.client.submit(count_query).all().result()
            total_edge_count = total_edge_count_result[0] if total_edge_count_result else 0
            print(f"🔢 Total edges in Cosmos DB: {total_edge_count}")
        except Exception as e:
            print(f"⚠️ Could not get total edge count: {e}")
            total_edge_count = "Unknown"
        
        query = "g.E().project('id', 'label', 'inV', 'outV', 'properties').by(id).by(label).by(inV().id()).by(outV().id()).by(valueMap())"
        
        try:
            result = self.client.submit(query).all().result()
            edges = []
            
            for item in result:
                edge = {
                    'id': item['id'],
                    'label': item['label'],
                    'source': item['outV'],
                    'target': item['inV'],
                    'properties': item.get('properties', {})
                }
                edges.append(edge)
            
            print(f"✅ Fetched {len(edges)} edges")
            if total_edge_count != "Unknown" and len(edges) < total_edge_count:
                print(f"⚠️  WARNING: Only showing {len(edges)} of {total_edge_count} total edges!")
            log.info(f"Fetched {len(edges)} edges")
            return edges
            
        except Exception as e:
            print(f"❌ Error fetching edges: {e}")
            log.error(f"Error fetching edges: {e}")
            return []
    
    def create_visualization(self, output_file: str = "cosmos_graph_visualization.html") -> str:
        """Create D3.js interactive visualization of the graph."""
        print("🎨 Creating D3.js graph visualization...")
        log.info("Creating D3.js graph visualization...")
        
        # Fetch data
        vertices = self.fetch_all_vertices()
        edges = self.fetch_all_edges()
        
        if not vertices:
            print("⚠️ No vertices found in graph")
            log.warning("No vertices found in graph")
            return output_file
        
        # Store data as instance variables for generate_html
        self.nodes = {str(vertex['id']): vertex for vertex in vertices}
        self.edges = edges
        
        # Calculate node degrees (connection counts)
        node_degrees = {}
        for node_id in self.nodes:
            node_degrees[node_id] = 0
        
        for edge in edges:
            source = str(edge['source'])
            target = str(edge['target'])
            if source in node_degrees:
                node_degrees[source] += 1
            if target in node_degrees:
                node_degrees[target] += 1
        
        # Add degree to node data
        for node_id, degree in node_degrees.items():
            if node_id in self.nodes:
                self.nodes[node_id]['degree'] = degree
        
        # Generate HTML with D3.js
        return self.generate_html(output_file)
    
    def generate_html(self, output_file="cosmos_graph_visualization.html"):
        """Generate HTML file with D3.js visualization."""
        
        # Prepare nodes and edges data (same as before)
        nodes_data = []
        for node_id, vertex in self.nodes.items():
            node_info = {
                "id": node_id,
                "label": vertex.get("label", vertex.get("name", node_id)),
                "type": vertex.get("type", vertex.get("label", "unknown")),
                "group": self._get_group_number(vertex.get("type", vertex.get("label", "unknown"))),
                "degree": vertex.get("degree", 0)  # Add degree
            }
            # Add all other properties
            properties = vertex.get('properties', {})
            for key, value in properties.items():
                if key not in ["id", "label", "type"]:
                    node_info[key] = value
            nodes_data.append(node_info)
        
        edges_data = []
        for edge in self.edges:
            edges_data.append({
                "source": edge["source"],
                "target": edge["target"],
                "label": edge.get("label", ""),
                "id": edge.get("id", "")
            })
        
        # Get unique entity types for legend
        entity_types = list(set(vertex.get("type", vertex.get("label", "unknown")) for vertex in self.nodes.values()))
        entity_types.sort()
        
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Cosmos DB Graph Visualization</title>
    <meta charset="utf-8">
    <script src="https://d3js.org/d3.v7.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            overflow: hidden;
        }}
        
        #loading {{
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 24px;
            color: #333;
            z-index: 9999;
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.3);
        }}
        
        .hidden {{
            display: none !important;
        }}
        
        #info {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.9);
            padding: 15px;
            border-radius: 5px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            max-width: 350px;
        }}
        
        .node {{
            stroke: #fff;
            stroke-width: 1.5px;
            cursor: pointer;
        }}
        
        .link {{
            stroke: #999;
            stroke-opacity: 0.6;
            fill: none;
        }}
        
        .node:hover {{
            stroke: #000;
            stroke-width: 2px;
        }}
        
        /* Edge label styling */
        .edgeLabel {{
            font-size: 8px;
            fill: #666;
            text-anchor: middle;
            pointer-events: none;
            background: white;
        }}
        
        .edgeLabelBg {{
            fill: white;
            opacity: 0.8;
        }}
        
        text {{
            font: 10px sans-serif;
            pointer-events: none;
        }}
        
        #tooltip {{
            position: absolute;
            text-align: left;
            padding: 10px;
            font-size: 12px;
            background: rgba(0, 0, 0, 0.8);
            color: white;
            border-radius: 5px;
            pointer-events: none;
            opacity: 0;
            transition: opacity 0.3s;
            max-width: 300px;
            z-index: 1000;
        }}
        
        .legend {{
            position: absolute;
            bottom: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.9);
            padding: 10px;
            border-radius: 5px;
            max-height: 300px;
            overflow-y: auto;
        }}
        
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
            cursor: pointer;
        }}
        
        .legend-color {{
            width: 15px;
            height: 15px;
            margin-right: 5px;
            border-radius: 50%;
        }}
        
        /* Search panel styles */
        .search-panel {{
            position: fixed;
            top: 10px;
            right: 10px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            z-index: 2000;
            width: 350px;
            border: 2px solid #333;
        }}
        
        .search-input {{
            width: 65%;
            padding: 8px;
            border: 2px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
            margin-right: 5px;
        }}
        
        .search-btn {{
            padding: 8px 12px;
            border: none;
            border-radius: 4px;
            background: #4CAF50;
            color: white;
            cursor: pointer;
        }}
        
        .search-btn:hover {{
            background: #45a049;
        }}
        
        .search-result-item {{
            padding: 8px;
            margin: 4px 0;
            background: #f0f0f0;
            border-radius: 3px;
            cursor: pointer;
            border: 1px solid #ddd;
        }}
        
        .search-result-item:hover {{
            background: #e0e0e0;
        }}
        
        .details-panel {{
            position: fixed;
            bottom: 10px;
            right: 10px;
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            z-index: 2000;
            width: 400px;
            max-height: 40vh;
            overflow-y: auto;
            border: 2px solid #333;
        }}
        
        .close-btn {{
            float: right;
            cursor: pointer;
            font-size: 20px;
            color: #666;
        }}
    </style>
</head>
<body>
    <div id="loading">Loading graph...</div>
    
    <div id="info" class="hidden">
        <h2>Cosmos DB Graph Visualization</h2>
        <p><strong>Database:</strong> {self.database}</p>
        <p><strong>Container:</strong> {self.container}</p>
        <p><strong>Nodes:</strong> {len(self.nodes)}</p>
        <p><strong>Edges:</strong> {len(self.edges)}</p>
        <hr>
        <h3>Instructions:</h3>
        <ul>
                <li>Click and drag to pan</li>
                <li>Scroll to zoom</li>
            <li>Click a node to see full details</li>
                <li>Drag nodes to reposition</li>
            <li>Use search panel to find nodes</li>
            <li>Relationship types shown on edges</li>
            </ul>
    </div>
    
    <div id="tooltip"></div>
    
    <!-- Search Panel -->
    <div id="searchPanel" class="search-panel hidden">
        <h3>🔍 Node Search</h3>
        <div style="color: blue; font-size: 12px; margin-bottom: 5px;" id="searchStatus">Ready to search</div>
        <input type="text" 
               class="search-input" 
               id="nodeSearchInput" 
               placeholder="Enter ID, chunk, or any property">
        <button class="search-btn" id="searchBtn">Search</button>
        <button class="search-btn" id="clearBtn">Clear</button>
        <div id="searchResults" style="margin-top: 10px; max-height: 300px; overflow-y: auto;"></div>
    </div>
    
    <!-- Details Panel -->
    <div id="detailsPanel" class="details-panel hidden">
        <span class="close-btn" id="closeDetailsBtn">×</span>
        <h3>📋 Node Details</h3>
        <div id="nodeDetails" style="font-family: monospace; font-size: 12px;"></div>
    </div>
    
    <script>
        // Global variables
        let svg, simulation, link, node, tooltip, zoom, edgeLabels;
        let searchMatches = [];
        const width = window.innerWidth;
        const height = window.innerHeight;
        
        // Data
        const graphData = {{
            nodes: {json.dumps(nodes_data)},
            links: {json.dumps(edges_data)}
        }};
        
        // Entity types for legend
        const entityTypes = {json.dumps(entity_types)};
        
        // Color scale for node types
        const color = d3.scaleOrdinal()
            .domain(entityTypes)
            .range(d3.schemeCategory10);
        
        // Initialize after DOM is loaded
        document.addEventListener('DOMContentLoaded', function() {{
            console.log('DOM loaded, initializing graph...');
            initializeGraph();
        }});
        
        function initializeGraph() {{
            try {{
                // Create SVG
                svg = d3.select("body")
                    .append("svg")
                    .attr("width", width)
                    .attr("height", height)
                    .style("background-color", "#f0f0f0");
                
                // Create container for graph elements (single transform target)
                const container = svg.append("g");
                
                // Add zoom functionality that transforms only the container
                zoom = d3.zoom()
                    .scaleExtent([0.1, 10])
                    .on("zoom", (event) => {{
                        container.attr("transform", event.transform);
                    }});
                
                svg.call(zoom);
                
                // Create simulation
                simulation = d3.forceSimulation(graphData.nodes)
                    .force("link", d3.forceLink(graphData.links)
                        .id(d => d.id)
                        .distance(150))
                    .force("charge", d3.forceManyBody()
                        .strength(-500))
                    .force("center", d3.forceCenter(width / 2, height / 2))
                    .force("collision", d3.forceCollide().radius(d => {{
                        const minRadius = 5;
                        const maxRadius = 30;
                        const maxDegree = Math.max(...graphData.nodes.map(n => n.degree || 0));
                        if (maxDegree === 0) return minRadius + 5;
                        const scale = d3.scaleSqrt()
                            .domain([0, maxDegree])
                            .range([minRadius, maxRadius]);
                        return scale(d.degree || 0) + 5;
                    }}));
                
                // Add links first (so they appear behind nodes)
                link = container.append("g")
                    .attr("class", "links")
                    .selectAll("path")
                    .data(graphData.links)
                    .join("path")
                    .attr("class", "link")
                    .attr("stroke-width", d => Math.sqrt(d.value || 1))
                    .attr("id", (d, i) => `link-${{i}}`);
                
                // Add edge labels
                const edgeLabelGroup = container.append("g")
                    .attr("class", "edgeLabels");
                
                // Add background rectangles for edge labels
                const edgeLabelBg = edgeLabelGroup.selectAll("rect")
                    .data(graphData.links)
                    .join("rect")
                    .attr("class", "edgeLabelBg")
                    .attr("width", d => d.label ? d.label.length * 6 : 0)
                    .attr("height", 12)
                    .attr("x", d => d.label ? -d.label.length * 3 : 0)
                    .attr("y", -6);
                
                // Add edge label text
                edgeLabels = edgeLabelGroup.selectAll("text")
                    .data(graphData.links)
                    .join("text")
                    .attr("class", "edgeLabel")
                    .text(d => d.label || "");
                
                // Add nodes
                node = container.append("g")
                    .attr("class", "nodes")
                    .selectAll("circle")
                    .data(graphData.nodes)
                    .join("circle")
                    .attr("class", "node")
                    .attr("r", d => {{
                        const minRadius = 5;
                        const maxRadius = 30;
                        const maxDegree = Math.max(...graphData.nodes.map(n => n.degree || 0));
                        if (maxDegree === 0) return minRadius;
                        const scale = d3.scaleSqrt()
                            .domain([0, maxDegree])
                            .range([minRadius, maxRadius]);
                        return scale(d.degree || 0);
                    }})
                    .attr("fill", d => color(d.type))
                    .call(drag(simulation));
                
                // NO NODE LABELS - Removed the label group entirely
                
                // Add tooltip functionality
                tooltip = d3.select("#tooltip");
                
                node.on("mouseover", function(event, d) {{
                    tooltip.transition()
                        .duration(200)
                        .style("opacity", .9);
                    tooltip.html(`<strong>${{d.label || d.id}}</strong><br/>
                                 Type: ${{d.type}}<br/>
                                 ID: ${{d.id}}`)
                        .style("left", (event.pageX + 10) + "px")
                        .style("top", (event.pageY - 28) + "px");
                }})
                .on("mouseout", function(d) {{
                    tooltip.transition()
                        .duration(500)
                        .style("opacity", 0);
                }})
                .on("click", showNodeDetails);
                
                // Add title to nodes (for browser tooltip)
                node.append("title")
                    .text(d => d.label || d.id);
                
                // Update positions on simulation tick
                simulation.on("tick", () => {{
                    // Update link positions as curved paths
                    link.attr("d", d => {{
                        const dx = d.target.x - d.source.x;
                        const dy = d.target.y - d.source.y;
                        const dr = Math.sqrt(dx * dx + dy * dy);
                        return `M${{d.source.x}},${{d.source.y}}A${{dr}},${{dr}} 0 0,1 ${{d.target.x}},${{d.target.y}}`;
                    }});
                    
                    // Update edge label positions
                    edgeLabels
                        .attr("x", d => (d.source.x + d.target.x) / 2)
                        .attr("y", d => (d.source.y + d.target.y) / 2);
                    
                    edgeLabelBg
                        .attr("x", d => (d.source.x + d.target.x) / 2 - (d.label ? d.label.length * 3 : 0))
                        .attr("y", d => (d.source.y + d.target.y) / 2 - 6);
                    
                    // Update node positions
                    node
                        .attr("cx", d => d.x)
                        .attr("cy", d => d.y);
                }});
                
                // Create legend
                createLegend();
                
                // Hide loading and show UI
                document.getElementById('loading').classList.add('hidden');
                document.getElementById('info').classList.remove('hidden');
                document.getElementById('searchPanel').classList.remove('hidden');
                
                // Initialize search functionality
                initializeSearch();
                
                console.log('Graph initialized successfully');
                
            }} catch (error) {{
                console.error('Error initializing graph:', error);
                document.getElementById('loading').textContent = 'Error loading graph: ' + error.message;
            }}
        }}
        
        // Keep all the search functionality exactly as it was
        function initializeSearch() {{
            console.log('Initializing search functionality...');
            
            const searchBtn = document.getElementById('searchBtn');
            const clearBtn = document.getElementById('clearBtn');
            const searchInput = document.getElementById('nodeSearchInput');
            const closeDetailsBtn = document.getElementById('closeDetailsBtn');
            
            searchBtn.addEventListener('click', performSearch);
            clearBtn.addEventListener('click', clearSearch);
            closeDetailsBtn.addEventListener('click', () => {{
                document.getElementById('detailsPanel').classList.add('hidden');
            }});
            
            searchInput.addEventListener('keypress', (e) => {{
                if (e.key === 'Enter') performSearch();
            }});
            
            document.getElementById('searchStatus').textContent = `Ready to search {len(nodes_data)} nodes`;
        }}
        
        function performSearch() {{
            const searchTerm = document.getElementById('nodeSearchInput').value.toLowerCase().trim();
            if (!searchTerm) {{
                alert('Please enter a search term');
                return;
            }}
            
            console.log('Searching for:', searchTerm);
            document.getElementById('searchStatus').textContent = 'Searching...';
            
            searchMatches = [];
            
            graphData.nodes.forEach(nodeData => {{
                let found = false;
                
                for (const [key, value] of Object.entries(nodeData)) {{
                    if (value && String(value).toLowerCase().includes(searchTerm)) {{
                        found = true;
                        break;
                    }}
                }}
                
                if (found) {{
                    searchMatches.push(nodeData);
                }}
            }});
            
            console.log('Found', searchMatches.length, 'matches');
            displaySearchResults();
        }}
        
        function displaySearchResults() {{
            const resultsDiv = document.getElementById('searchResults');
            document.getElementById('searchStatus').textContent = `Found ${{searchMatches.length}} matches`;
            
            if (searchMatches.length === 0) {{
                resultsDiv.innerHTML = '<p style="color: red;">No nodes found</p>';
                return;
            }}
            
            let html = '';
            searchMatches.forEach((nodeData, i) => {{
                html += `
                    <div class="search-result-item" onclick="focusOnSearchResult(${{i}})">
                        <strong>${{nodeData.label || nodeData.id}}</strong><br>
                        <small>Type: ${{nodeData.type || 'Unknown'}}</small><br>
                        <small>ID: ${{nodeData.id}}</small>
                    </div>
                `;
            }});
            resultsDiv.innerHTML = html;
            
            if (searchMatches.length === 1) {{
                focusOnSearchResult(0);
            }}
        }}
        
        function focusOnSearchResult(index) {{
            const nodeData = searchMatches[index];
            if (!nodeData) return;
            
            const foundNode = d3.selectAll('.node')
                .filter(d => d.id === nodeData.id);
            
            if (!foundNode.empty()) {{
                d3.selectAll('.node')
                    .style('stroke', null)
                    .style('stroke-width', null);
                
                foundNode
                    .style('stroke', 'red')
                    .style('stroke-width', '3px');
                
                const d = foundNode.datum();
                // Center the node in view and zoom in
                const scale = 2;
                const transform = d3.zoomIdentity
                    .translate(width / 2, height / 2)
                    .scale(scale)
                    .translate(-d.x, -d.y);
                
                svg.transition()
                    .duration(750)
                    .call(zoom.transform, transform);
                
                showNodeDetails(null, d);
            }}
        }}
        
        function clearSearch() {{
            document.getElementById('nodeSearchInput').value = '';
            document.getElementById('searchResults').innerHTML = '';
            document.getElementById('searchStatus').textContent = `Ready to search ${{graphData.nodes.length}} nodes`;
            searchMatches = [];
            
            d3.selectAll('.node')
                .style('stroke', null)
                .style('stroke-width', null);
            
            document.getElementById('detailsPanel').classList.add('hidden');
        }}
        
        function drag(simulation) {{
            function dragstarted(event, d) {{
                if (!event.active) simulation.alphaTarget(0.3).restart();
                d.fx = d.x;
                d.fy = d.y;
            }}
            
            function dragged(event, d) {{
                d.fx = event.x;
                d.fy = event.y;
            }}
            
            function dragended(event, d) {{
                if (!event.active) simulation.alphaTarget(0);
                d.fx = null;
                d.fy = null;
            }}
            
            return d3.drag()
                .on("start", dragstarted)
                .on("drag", dragged)
                .on("end", dragended);
        }}
        
        function showNodeDetails(event, d) {{
            const detailsPanel = document.getElementById('detailsPanel');
            const detailsDiv = document.getElementById('nodeDetails');
            
            // Reorder properties for display
            const systemProps = ['group', 'degree', 'partitionKey', 'index', 'x', 'y', 'vx', 'vy', 'fx', 'fy'];
            const orderedData = {{}};
            
            // Add non-system properties first
            for (const [key, value] of Object.entries(d)) {{
                if (!systemProps.includes(key)) {{
                    orderedData[key] = value;
                }}
            }}
            
            // Add system properties last
            for (const prop of systemProps) {{
                if (prop in d) {{
                    orderedData[prop] = d[prop];
                }}
            }}
            
            let html = '<pre>' + JSON.stringify(orderedData, null, 2) + '</pre>';
            detailsDiv.innerHTML = html;
            detailsPanel.classList.remove('hidden');
        }}
        
        function createLegend() {{
            const legend = d3.select("body")
                .append("div")
                .attr("class", "legend");
            
            legend.append("h3").text("Entity Types:");
            
            entityTypes.forEach(type => {{
                const item = legend.append("div")
                    .attr("class", "legend-item");
                
                item.append("div")
                    .attr("class", "legend-color")
                    .style("background-color", color(type));
                
                item.append("span").text(type);
                
                // Click to highlight type
                item.on("click", function() {{
                    const isActive = d3.select(this).classed("active");
                    
                    d3.selectAll(".legend-item").classed("active", false);
                    
                    if (!isActive) {{
                        d3.select(this).classed("active", true);
                        
                        node.style("opacity", d => d.type === type ? 1 : 0.1);
                        link.style("opacity", 0.1);
                        edgeLabels.style("opacity", 0.1);
                    }} else {{
                        node.style("opacity", 1);
                        link.style("opacity", 0.6);
                        edgeLabels.style("opacity", 1);
                    }}
                }});
            }});
        }}
    </script>
</body>
</html>
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_template)
        
        print(f"💾 Visualization saved to {output_file}")
        log.info(f"Visualization saved to {output_file}")
        return output_file
    
    def _get_group_number(self, entity_type):
        """Get a numeric group number for the entity type."""
        entity_types = list(self.entity_colors.keys())
        
        # Try exact match first
        if entity_type in entity_types:
            return entity_types.index(entity_type)
        
        # Try lowercase match
        lowercase_type = entity_type.lower()
        if lowercase_type in entity_types:
            return entity_types.index(lowercase_type)
            
        # Try title case match
        title_type = entity_type.title()
        if title_type in entity_types:
            return entity_types.index(title_type)
            
        # Try proper case for known types
        proper_cases = {
            'agendaitem': 'AgendaItem',
            'voteoutcome': 'VoteOutcome',
            'event': 'Event',
            'person': 'Person',
            'organization': 'Organization',
            'document': 'Document',
            'policy': 'Policy',
            'action': 'Action',
            'asset': 'Asset',
            'project': 'Project',
            'location': 'Location',
            'role': 'Role',
            'topic': 'Topic'
        }
        
        proper_case = proper_cases.get(lowercase_type)
        if proper_case and proper_case in entity_types:
            return entity_types.index(proper_case)
        
        return len(entity_types)  # Default group for unknown types
    

    
    def close(self):
        """Close the Gremlin client connection."""
        if hasattr(self, 'client'):
            print("🔌 Closing Cosmos DB connection...")
            self.client.close()
            print("✅ Connection closed")


def main():
    """Main function to run the visualizer."""
    print("🚀 Starting Cosmos DB Graph Visualizer")
    print("=" * 50)
    
    try:
        # Create visualizer
        visualizer = CosmosGraphVisualizer()
        
        # Create visualization
        output_file = visualizer.create_visualization("cosmos_graph_visualization.html")
        
        # Close connection
        visualizer.close()
        
        # Open in browser
        abs_path = Path(output_file).absolute()
        webbrowser.open(f"file://{abs_path}")
        
        print("=" * 50)
        print(f"✅ Visualization complete! Opening {output_file} in browser...")
        
    except Exception as e:
        print(f"❌ Visualization failed: {e}")
        log.error(f"Visualization failed: {e}")
        raise


if __name__ == "__main__":
    main() 