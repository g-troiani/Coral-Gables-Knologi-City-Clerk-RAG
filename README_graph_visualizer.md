# 🏛️ City Clerk Knowledge Graph Visualizer

An interactive web-based visualization tool for exploring your Azure Cosmos DB Gremlin graph database on localhost.

## ✨ Features

- **Interactive Graph Visualization**: Explore nodes and relationships in your knowledge graph
- **Multiple Layout Algorithms**: Spring, circular, and random layouts
- **Real-time Statistics**: View graph metrics and node/edge type distributions
- **Data Tables**: Browse detailed information about nodes and relationships
- **Responsive Design**: Modern web interface that works on desktop and mobile
- **Live Updates**: Refresh data from your database without restarting

## 🚀 Quick Start

### 1. Prerequisites

Make sure you have:
- Python 3.7 or higher
- An active Azure Cosmos DB account with Gremlin API
- Your Cosmos DB connection credentials

### 2. Setup Environment

First, run the setup script to configure your database connection:

```bash
python3 setup_env.py
```

Choose option 2 for interactive setup, or option 1 to create a template file you can edit manually.

You'll need to provide:
- **Cosmos DB Key**: Your primary or secondary key from Azure portal
- **Endpoint**: Usually `wss://your-account.gremlin.cosmos.azure.com:443`
- **Database Name**: Default is `cgGraph`
- **Container Name**: Default is `cityClerk`

### 3. Start the Visualizer

```bash
python3 graph_visualizer.py
```

### 4. Open in Browser

Navigate to: **http://localhost:8050**

## 📊 What You'll See

### Graph Visualization
- **Nodes**: Colored by type (Person, Organization, Location, etc.)
- **Edges**: Connections showing relationships between entities
- **Interactive**: Click and drag to explore, hover for details

### Statistics Panel
- Total nodes and edges count
- Connected components analysis
- Average node degree
- Breakdown by node and edge types

### Controls
- **Layout Dropdown**: Switch between different visualization layouts
- **Refresh Button**: Pull latest data from your database
- **Data Table**: Browse all nodes with searchable/sortable interface

## 🔧 Configuration

### Environment Variables

The visualizer uses these environment variables (set in `.env` file):

```bash
COSMOS_KEY=your_actual_cosmos_key_here
COSMOS_ENDPOINT=wss://aida-graph-db.gremlin.cosmos.azure.com:443
COSMOS_DATABASE=cgGraph
COSMOS_CONTAINER=cityClerk
COSMOS_PARTITION_KEY=partitionKey
COSMOS_PARTITION_VALUE=demo
```

### Checking Configuration

Validate your setup:

```bash
python3 config.py
```

## 🎨 Graph Layout Options

- **Spring Layout**: Physics-based layout that groups related nodes
- **Circular Layout**: Arranges nodes in a circle
- **Random Layout**: Random positioning (useful for dense graphs)

## 📈 Performance Tips

1. **Large Graphs**: Adjust the limit in `query_graph_data(limit=500)` if you have many nodes
2. **Memory Usage**: For very large graphs, consider filtering by node type
3. **Network**: Keep your internet connection stable for Azure Cosmos DB queries

## 🛠️ Customization

### Adding New Node Types

To add support for new entity types, update the color mapping in `create_plotly_visualization()`:

```python
color_map = px.colors.qualitative.Set3  # Extend this for more colors
```

### Changing Query Limits

Modify the default query limit in `graph_visualizer.py`:

```python
graph_data = visualizer.query_graph_data(limit=1000)  # Increase for more data
```

### Custom Styling

Update the Dash layout styling in the `app.layout` section for different themes.

## 🐛 Troubleshooting

### Common Issues

**"Configuration validation failed"**
- Run `python3 setup_env.py` to create/update your `.env` file
- Ensure your Cosmos DB key is correct

**"Connection failed"**
- Check your internet connection
- Verify your Cosmos DB endpoint URL
- Ensure your Cosmos DB account is active

**"No graph data available"**
- Your graph database might be empty
- Check if your container and database names are correct
- Verify partition key configuration

**"Module not found"**
- Ensure you're in the activated virtual environment
- Run: `pip install -r requirements.txt`

### Debug Mode

Run with debug output:

```bash
python3 graph_visualizer.py
```

Check the terminal output for detailed error messages.

## 📁 File Structure

```
graph_database/
├── config.py                 # Configuration management
├── graph_visualizer.py       # Main visualization application
├── setup_env.py             # Environment setup script
├── .env                     # Your database credentials (create this)
└── README_graph_visualizer.md # This file
```

## 🔐 Security Notes

- Never commit your `.env` file to version control
- Use read-only Cosmos DB keys when possible
- Consider using Azure Key Vault for production deployments

## 🎯 Example Use Cases

1. **Knowledge Discovery**: Find unexpected connections between entities
2. **Data Quality**: Identify isolated nodes or missing relationships  
3. **Graph Analysis**: Understand the structure of your knowledge base
4. **Presentation**: Demo your graph database to stakeholders

## 🤝 Contributing

To extend the visualizer:

1. Fork the repository
2. Create feature branches
3. Add new visualization options
4. Submit pull requests

## 📚 Additional Resources

- [Azure Cosmos DB Gremlin API Documentation](https://docs.microsoft.com/en-us/azure/cosmos-db/gremlin/)
- [Plotly Dash Documentation](https://dash.plotly.com/)
- [NetworkX Documentation](https://networkx.org/)

---

**Happy Visualizing!** 🎉

If you encounter any issues, check the troubleshooting section or create an issue in the repository. 