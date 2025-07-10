# Single Meeting Graph Viewer

A focused graph visualization tool that displays the knowledge graph for just one meeting day at a time.

## Features

- **Meeting Selection**: Choose from a dropdown of available meetings sorted by date
- **Focused Visualization**: Shows only nodes connected to the selected meeting (within 2 hops)
- **Interactive Graph**: Click on nodes to see detailed properties
- **Multiple Layouts**: Support for hierarchical, force-directed, and other layout algorithms
- **Node Type Filtering**: Visual distinction between different types of nodes (meetings, people, documents, etc.)

## Usage

### Quick Start

1. Make sure you have a virtual environment set up:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run the viewer:
   ```bash
   ./run_single_meeting_viewer.sh
   ```

3. Open your browser and navigate to the URL shown in the terminal (typically `http://localhost:8055`)

4. Select a meeting from the dropdown to view its graph

### Manual Run

Alternatively, you can run it directly:
```bash
source venv/bin/activate
python3 single_meeting_graph_viewer.py
```

## How It Works

1. **Data Loading**: Loads the complete graph from `local_graph_data/city_clerk_graph.graphml`
2. **Meeting Detection**: Automatically identifies all meeting nodes in the graph
3. **Graph Filtering**: When a meeting is selected, creates a subgraph containing:
   - The selected meeting node
   - All directly connected nodes (1 hop)
   - All nodes connected to those nodes (2 hops)
4. **Visualization**: Displays the filtered graph using Cytoscape.js

## Interface Elements

- **Meeting Dropdown**: Select which meeting to visualize
- **Layout Selector**: Choose from different graph layout algorithms
- **Refresh Button**: Reapply the current layout
- **Interactive Graph**: Click and drag nodes, zoom, pan
- **Node Details Panel**: Click on any node to see its properties

## Node Types

The viewer supports various node types, each with distinct visual styling:

- 📅 **MEETING**: Blue rounded rectangles (root nodes)
- 👤 **PERSON**: Red diamonds
- 🏢 **ORGANIZATION**: Green hexagons
- 📋 **AGENDA_ITEM**: Yellow ellipses
- 📂 **SECTION**: Purple rectangles
- 📄 **DOCUMENT**: Gray rectangles
- ⚖️ **LEGAL_DOCUMENT**: Purple octagons
- 📜 **RESOLUTION**: Teal hexagons
- 📋 **ORDINANCE**: Indigo octagons
- 🏛️ **DEPARTMENT**: Brown squares
- 📍 **LOCATION**: Orange triangles

## Comparison with Full Graph Viewer

| Feature | Single Meeting Viewer | Full Graph Viewer |
|---------|----------------------|-------------------|
| **Scope** | One meeting at a time | All meetings simultaneously |
| **Performance** | Faster (fewer nodes) | Slower (more nodes) |
| **Focus** | Deep dive into specific meeting | Overview of all data |
| **Use Case** | Detailed analysis | Broad exploration |

## Troubleshooting

### Port Already in Use
If you see a port error, the viewer will automatically try ports 8055-8059. Close other applications if needed.

### No Meetings Found
Ensure your graph file contains meeting nodes with the correct attributes:
- `label`: should be "meeting"
- `title`: meeting name/description
- `meeting_date`: date in MM.DD.YYYY format

### Graph File Not Found
Make sure `local_graph_data/city_clerk_graph.graphml` exists in your project directory.

## Technical Details

- **Backend**: Python with Dash framework
- **Frontend**: Cytoscape.js for graph visualization
- **Graph Processing**: NetworkX for graph operations
- **Port Range**: 8055-8059 (automatically selected)

## Development

To modify the viewer:

1. Edit `single_meeting_graph_viewer.py`
2. Key classes and methods:
   - `SingleMeetingGraphVisualizer`: Main visualization class
   - `filter_graph_by_meeting()`: Core filtering logic
   - `update_graph()`: Dash callback for graph updates
   - `show_node_details()`: Node detail display

The viewer is designed to be modular and extensible for future enhancements. 