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
from pyvis.network import Network
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
            'Agendaitem': '#FD79A8',  # Handle title case version
            'Contract': '#A29BFE',
            'Technology': '#6C5CE7',
            'VoteOutcome': '#FDCB6E',
            'Voteoutcome': '#FDCB6E',  # Handle title case version
            'Meeting': '#00B894',
            'Section': '#E17055'
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
        
        query = "g.V().valueMap(true).limit(500)"  # Limit for performance
        
        try:
            result = self.client.submit(query).all().result()
            vertices = []
            
            for item in result:
                vertex = {
                    'id': item['id'],
                    'label': item['label'],
                    'properties': {}
                }
                
                # Extract properties
                for key, value in item.items():
                    if key not in ['id', 'label']:
                        # Gremlin returns properties as lists
                        if isinstance(value, list) and len(value) > 0:
                            vertex['properties'][key] = value[0]
                        else:
                            vertex['properties'][key] = value
                
                vertices.append(vertex)
            
            print(f"✅ Fetched {len(vertices)} vertices (limited to 500)")
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
        
        query = "g.E().project('id', 'label', 'inV', 'outV', 'properties').by(id).by(label).by(inV().id()).by(outV().id()).by(valueMap()).limit(1000)"
        
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
            
            print(f"✅ Fetched {len(edges)} edges (limited to 1000)")
            if total_edge_count != "Unknown" and len(edges) < total_edge_count:
                print(f"⚠️  WARNING: Only showing {len(edges)} of {total_edge_count} total edges!")
            log.info(f"Fetched {len(edges)} edges")
            return edges
            
        except Exception as e:
            print(f"❌ Error fetching edges: {e}")
            log.error(f"Error fetching edges: {e}")
            return []
    
    def create_visualization(self, output_file: str = "cosmos_graph.html") -> str:
        """Create interactive visualization of the graph."""
        print("🎨 Creating graph visualization...")
        log.info("Creating graph visualization...")
        
        # Fetch data
        vertices = self.fetch_all_vertices()
        edges = self.fetch_all_edges()
        
        if not vertices:
            print("⚠️ No vertices found in graph")
            log.warning("No vertices found in graph")
            return output_file
        
        # Create network with tooltip enabled
        net = Network(height="900px", width="100%", bgcolor="#f0f0f0", font_color="black")
        net.barnes_hut(gravity=-5000, central_gravity=0.3, spring_length=200)
        
        # Store node data for custom click handler
        node_data = {}
        
        # Add vertices
        print("🔵 Adding nodes to visualization...")
        for vertex in vertices:
            node_id = str(vertex['id'])
            label = vertex['label']
            properties = vertex['properties']
            
            # Determine display name for storage (not display)
            display_name = properties.get('name', 
                          properties.get('title', 
                          properties.get('item_code', 
                          node_id[:20])))
            
            # Store node data for later reference
            node_data[node_id] = {
                'id': node_id,
                'label': label,
                'name': display_name,
                'properties': properties
            }
            
            # Create title with all properties for hover
            title_parts = [f"<strong>{label}</strong>", f"Name: {display_name}", f"ID: {node_id}"]
            
            for key, value in sorted(properties.items()):
                if value and key not in ['name', 'title', 'item_code']:
                    # Escape HTML and truncate long values
                    display_value = html.escape(str(value))
                    if len(display_value) > 100:
                        display_value = display_value[:100] + "..."
                    title_parts.append(f"{key}: {display_value}")
            
            title = "<br>".join(title_parts)
            
            # Add node WITHOUT label
            net.add_node(
                node_id, 
                label="",  # Empty label - no text displayed on node
                title=title,
                color=self.entity_colors.get(label.title(), '#A0A0A0'),
                size=20,  # Slightly smaller since no text
                font={'size': 0}  # Hide any font
            )
        
        # Add edges
        print("🔗 Adding edges to visualization...")
        for edge in edges:
            source_id = str(edge['source'])
            target_id = str(edge['target'])
            
            # Only add edge if both nodes exist
            if source_id in [str(v['id']) for v in vertices] and \
               target_id in [str(v['id']) for v in vertices]:
                
                # Create edge title with properties
                edge_title = f"<strong>{edge['label']}</strong>"
                if edge['properties']:
                    for key, value in edge['properties'].items():
                        if isinstance(value, list) and value:
                            edge_title += f"<br>{key}: {html.escape(str(value[0]))}"
                        elif value:
                            edge_title += f"<br>{key}: {html.escape(str(value))}"
                
                net.add_edge(
                    source_id,
                    target_id,
                    title=edge_title,
                    label=edge['label'],  # Keep edge labels visible
                    arrows="to",
                    color={'color': '#666666', 'opacity': 0.6},
                    font={'size': 10, 'color': '#333333'}  # Edge label font
                )
        
        # Set options with better tooltip settings
        net.set_options("""
        var options = {
            "nodes": {
                "borderWidth": 2,
                "shadow": true,
                "font": {
                    "size": 0
                }
            },
            "edges": {
                "arrows": {
                    "to": {
                        "enabled": true,
                        "scaleFactor": 0.5
                    }
                },
                "color": {
                    "opacity": 0.6
                },
                "smooth": {
                    "type": "dynamic"
                },
                "font": {
                    "size": 10,
                    "align": "middle",
                    "background": "rgba(255,255,255,0.7)"
                }
            },
            "physics": {
                "enabled": true,
                "stabilization": {
                    "enabled": true,
                    "iterations": 100
                }
            },
            "interaction": {
                "hover": true,
                "navigationButtons": true,
                "keyboard": true,
                "tooltipDelay": 100,
                "hideEdgesOnDrag": true
            }
        }
        """)
        
        # Save visualization
        net.save_graph(output_file)
        print(f"💾 Visualization saved to {output_file}")
        log.info(f"Visualization saved to {output_file}")
        
        # Add custom CSS, info panel, and click handler
        self._enhance_html_with_details(output_file, len(vertices), len(edges), node_data)
        
        return output_file
    
    def _enhance_html_with_details(self, html_file: str, node_count: int, edge_count: int, node_data: Dict):
        """Add custom styling, info panel, and details view to the HTML."""
        print("🎨 Enhancing HTML with interactive details...")
        
        with open(html_file, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Convert node_data to JSON for JavaScript
        node_data_json = json.dumps(node_data)
        
        # Add info panel, details panel, and custom CSS
        custom_content = f"""
        <div style="position: absolute; top: 10px; left: 10px; background: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); z-index: 1000;">
            <h3 style="margin: 0 0 10px 0;">Cosmos DB Graph Visualization</h3>
            <p style="margin: 5px 0;"><b>Database:</b> {self.database}</p>
            <p style="margin: 5px 0;"><b>Container:</b> {self.container}</p>
            <p style="margin: 5px 0;"><b>Nodes:</b> {node_count}</p>
            <p style="margin: 5px 0;"><b>Edges:</b> {edge_count}</p>
            <hr style="margin: 10px 0;">
            <p style="margin: 5px 0; font-size: 12px;"><b>Instructions:</b></p>
            <ul style="margin: 5px 0; padding-left: 20px; font-size: 12px;">
                <li>Click and drag to pan</li>
                <li>Scroll to zoom</li>
                <li><b>Click a node to see full details</b></li>
                <li>Drag nodes to reposition</li>
            </ul>
            <hr style="margin: 10px 0;">
            <p style="margin: 5px 0; font-size: 12px;"><b>Entity Types:</b></p>
            <div style="font-size: 11px;">
        """
        
        # Add color legend
        for entity_type, color in sorted(self.entity_colors.items()):
            custom_content += f'<div style="margin: 2px 0;"><span style="display: inline-block; width: 12px; height: 12px; background: {color}; margin-right: 5px; border-radius: 2px;"></span>{entity_type}</div>'
        
        custom_content += """
            </div>
        </div>
        
        <!-- Details Panel -->
        <div id="detailsPanel" style="position: absolute; top: 10px; right: 10px; background: white; padding: 15px; border-radius: 5px; box-shadow: 0 2px 5px rgba(0,0,0,0.2); z-index: 1000; width: 350px; max-height: 80vh; overflow-y: auto; display: none;">
            <h3 style="margin: 0 0 10px 0;">Node Details</h3>
            <button onclick="document.getElementById('detailsPanel').style.display='none'" style="position: absolute; top: 10px; right: 10px; background: #f0f0f0; border: none; padding: 5px 10px; border-radius: 3px; cursor: pointer;">✕</button>
            <div id="detailsContent"></div>
        </div>
        
        <style>
            body { margin: 0; padding: 0; }
            #mynetwork { width: 100%; height: 100vh; }
            #detailsPanel { font-family: Arial, sans-serif; }
            #detailsPanel h4 { margin: 15px 0 5px 0; color: #333; }
            #detailsPanel .property { margin: 3px 0; padding: 3px; background: #f5f5f5; border-radius: 3px; font-size: 13px; }
            #detailsPanel .property-key { font-weight: bold; color: #555; }
            #detailsPanel .property-value { color: #333; word-wrap: break-word; }
        </style>
        """
        
        # Find where the network is initialized and add our custom script
        # PyVis generates code that ends with "return network;"
        # We need to insert our code after the network is created
        
        # Find the script tag that contains the network initialization
        script_insert_point = html_content.find("return network;")
        if script_insert_point != -1:
            # Insert our custom JavaScript before "return network;"
            custom_script = f"""
            
            // Custom node data
            var nodeData = {node_data_json};
            
            // Add click event listener
            network.on("click", function(params) {{
                if (params.nodes.length > 0) {{
                    var nodeId = params.nodes[0];
                    showNodeDetails(nodeId);
                }}
            }});
            
            function showNodeDetails(nodeId) {{
                var node = nodeData[nodeId];
                if (!node) return;
                
                var detailsPanel = document.getElementById('detailsPanel');
                var detailsContent = document.getElementById('detailsContent');
                
                var html = '<h4>Type: ' + node.label + '</h4>';
                html += '<div class="property"><span class="property-key">Name:</span> <span class="property-value">' + escapeHtml(node.name) + '</span></div>';
                html += '<div class="property"><span class="property-key">ID:</span> <span class="property-value">' + node.id + '</span></div>';
                
                if (node.properties && Object.keys(node.properties).length > 0) {{
                    html += '<h4>Properties:</h4>';
                    for (var key in node.properties) {{
                        var value = node.properties[key];
                        if (value !== null && value !== undefined) {{
                            html += '<div class="property"><span class="property-key">' + key + ':</span> <span class="property-value">' + escapeHtml(String(value)) + '</span></div>';
                        }}
                    }}
                }} else {{
                    html += '<p style="color: #999; font-style: italic;">No additional properties</p>';
                }}
                
                detailsContent.innerHTML = html;
                detailsPanel.style.display = 'block';
            }}
            
            function escapeHtml(unsafe) {{
                return unsafe
                    .replace(/&/g, "&amp;")
                    .replace(/</g, "&lt;")
                    .replace(/>/g, "&gt;")
                    .replace(/"/g, "&quot;")
                    .replace(/'/g, "&#039;");
            }}
            
            """
            
            # Insert the custom script before "return network;"
            html_content = html_content[:script_insert_point] + custom_script + html_content[script_insert_point:]
        
        # Insert custom content after body tag
        html_content = html_content.replace('<body>', f'<body>\n{custom_content}')
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print("✅ HTML enhanced with click-to-view details functionality")
    
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