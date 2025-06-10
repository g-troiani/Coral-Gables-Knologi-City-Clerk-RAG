#!/usr/bin/env python3
"""
GraphRAG Query UI
A web interface for querying the City Clerk GraphRAG knowledge base.
"""

import dash
from dash import dcc, html, Input, Output, State, ctx
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import asyncio
from pathlib import Path
import sys
import json
from datetime import datetime
import logging

# Add project root to path
# Handle both cases: script in root or in a subdirectory
current_file = Path(__file__).resolve()
if current_file.parent.name == "scripts" or current_file.parent.name == "ui":
    project_root = current_file.parent.parent
else:
    project_root = current_file.parent
sys.path.append(str(project_root))

from scripts.microsoft_framework import (
    CityClerkQueryEngine,
    SmartQueryRouter,
    QueryIntent
)

# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Initialize the app with a nice theme
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="GraphRAG City Clerk Query System"
)

# Initialize query engine and router
GRAPHRAG_ROOT = project_root / "graphrag_data"
query_engine = None
query_router = SmartQueryRouter()

# Store query history
query_history = []

# Define the layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("🏛️ City Clerk GraphRAG Query System", className="text-center mb-4"),
            html.Hr(),
        ])
    ]),
    
    # Query Input Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("🔍 Enter Your Query")),
                dbc.CardBody([
                    dbc.Textarea(
                        id="query-input",
                        placeholder="Ask about agenda items, ordinances, resolutions, or city proceedings...\n\nExamples:\n- What is agenda item E-1?\n- Tell me about ordinance 2024-01\n- What are the main development themes?\n- How has zoning policy evolved?",
                        style={"height": "150px"},
                        className="mb-3"
                    ),
                    
                    dbc.Row([
                        dbc.Col([
                            html.Label("Query Method:", className="fw-bold"),
                            dbc.RadioItems(
                                id="query-method",
                                options=[
                                    {"label": "🤖 Auto-Select (Recommended)", "value": "auto"},
                                    {"label": "🎯 Local Search", "value": "local"},
                                    {"label": "🌐 Global Search", "value": "global"},
                                    {"label": "🔄 DRIFT Search", "value": "drift"}
                                ],
                                value="auto",
                                inline=False
                            ),
                        ], md=6),
                        
                        dbc.Col([
                            html.Label("Query Options:", className="fw-bold"),
                            dbc.Checklist(
                                id="query-options",
                                options=[
                                    {"label": "Include community context", "value": "community"},
                                    {"label": "Show routing details", "value": "routing"},
                                    {"label": "Verbose results", "value": "verbose"}
                                ],
                                value=["community", "routing"],
                                inline=False
                            ),
                        ], md=6),
                    ]),
                    
                    dbc.Row([
                        dbc.Col([
                            dbc.Button(
                                "🚀 Submit Query",
                                id="submit-query",
                                color="primary",
                                size="lg",
                                className="w-100 mt-3",
                                n_clicks=0
                            ),
                        ], md=6),
                        dbc.Col([
                            dbc.Button(
                                "🧹 Clear",
                                id="clear-all",
                                color="secondary",
                                size="lg",
                                className="w-100 mt-3",
                                n_clicks=0
                            ),
                        ], md=6),
                    ]),
                ])
            ], className="mb-4"),
        ])
    ]),
    
    # Loading indicator
    dcc.Loading(
        id="loading",
        type="default",
        children=[
            html.Div(id="loading-output")
        ]
    ),
    
    # Routing Information
    dbc.Row([
        dbc.Col([
            dbc.Collapse(
                dbc.Card([
                    dbc.CardHeader(html.H5("🎯 Query Routing Analysis")),
                    dbc.CardBody(id="routing-info")
                ], className="mb-4"),
                id="routing-collapse",
                is_open=False
            ),
        ])
    ]),
    
    # Results Section
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader(html.H4("📊 Query Results")),
                dbc.CardBody(id="query-results", style={"min-height": "300px"})
            ], className="mb-4"),
        ])
    ]),
    
    # Query History
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader([
                    html.H5("📜 Query History", className="d-inline"),
                    dbc.Button(
                        "Clear History",
                        id="clear-history",
                        color="danger",
                        size="sm",
                        className="float-end",
                        n_clicks=0
                    )
                ]),
                dbc.CardBody(id="query-history")
            ]),
        ])
    ]),
    
    # Hidden div for storing state
    html.Div(id="query-state", style={"display": "none"})
    
], fluid=True, className="p-4")

# Callback for handling queries
@app.callback(
    [Output("query-results", "children"),
     Output("routing-info", "children"),
     Output("routing-collapse", "is_open"),
     Output("query-history", "children"),
     Output("loading-output", "children")],
    [Input("submit-query", "n_clicks"),
     Input("clear-all", "n_clicks"),
     Input("clear-history", "n_clicks")],
    [State("query-input", "value"),
     State("query-method", "value"),
     State("query-options", "value")]
)
def handle_query(submit_clicks, clear_clicks, clear_history_clicks, query_text, method, options):
    global query_history
    
    # Determine which button was clicked
    triggered = ctx.triggered_id
    
    if triggered == "clear-all":
        return "", "", False, render_query_history(), ""
    
    if triggered == "clear-history":
        query_history = []
        return dash.no_update, dash.no_update, dash.no_update, render_query_history(), ""
    
    if triggered != "submit-query" or not query_text:
        raise PreventUpdate
    
    # Initialize query engine if needed
    global query_engine
    if query_engine is None:
        try:
            query_engine = CityClerkQueryEngine(GRAPHRAG_ROOT)
        except Exception as e:
            return render_error(f"Failed to initialize query engine: {e}"), "", False, dash.no_update, ""
    
    # Show loading message
    loading_msg = html.Div([
        html.H5("🔄 Processing your query..."),
        html.P(f"Query: {query_text[:100]}..."),
        html.P(f"Method: {method}")
    ])
    
    try:
        # Determine method
        if method == "auto":
            # Use router to determine method
            route_info = query_router.determine_query_method(query_text)
            actual_method = route_info['method']
            routing_details = route_info
        else:
            actual_method = method
            routing_details = {"method": method, "params": {}}
        
        # Run query asynchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Add options to params
        params = routing_details.get('params', {})
        if "community" not in options:
            params['include_community_context'] = False
        
        # Execute query
        result = loop.run_until_complete(
            query_engine.query(
                question=query_text,
                method=actual_method if method != "auto" else None,
                **params
            )
        )
        
        # Add to history
        query_history.insert(0, {
            "timestamp": datetime.now(),
            "query": query_text,
            "method": actual_method,
            "auto_routed": method == "auto"
        })
        
        # Limit history to 10 items
        query_history = query_history[:10]
        
        # Render results
        results_content = render_results(result, options)
        routing_content = render_routing_info(routing_details, actual_method) if "routing" in options else ""
        show_routing = "routing" in options
        
        return results_content, routing_content, show_routing, render_query_history(), ""
        
    except Exception as e:
        log.error(f"Query failed: {e}")
        return render_error(f"Query failed: {str(e)}"), "", False, dash.no_update, ""

def render_results(result: dict, options: list) -> html.Div:
    """Render query results in a nice format."""
    
    answer = result.get('answer', 'No answer available')
    metadata = result.get('routing_metadata', {})
    
    # Clean up the answer
    if isinstance(answer, str):
        # Remove any GraphRAG metadata lines
        lines = answer.split('\n')
        cleaned_lines = [line for line in lines if not line.startswith(('INFO:', 'WARNING:', 'DEBUG:'))]
        answer = '\n'.join(cleaned_lines).strip()
    
    content = [
        html.H5("📝 Answer:", className="mb-3"),
        dcc.Markdown(answer, className="p-3 bg-light rounded"),
    ]
    
    if "verbose" in options and metadata:
        content.extend([
            html.Hr(),
            html.H6("📊 Query Metadata:"),
            html.Pre(json.dumps(metadata, indent=2), className="bg-dark text-light p-3 rounded")
        ])
    
    return html.Div(content)

def render_routing_info(routing_details: dict, actual_method: str) -> html.Div:
    """Render routing analysis information."""
    
    intent = routing_details.get('intent')
    params = routing_details.get('params', {})
    
    content = [
        html.P([
            html.Strong("Selected Method: "),
            html.Span(actual_method.upper(), className="badge bg-primary")
        ]),
    ]
    
    if intent:
        content.append(html.P([
            html.Strong("Detected Intent: "),
            html.Span(intent.value if hasattr(intent, 'value') else str(intent))
        ]))
    
    # Show detected entities
    if 'entity_filter' in params:
        entity = params['entity_filter']
        content.append(html.P([
            html.Strong("Primary Entity: "),
            html.Code(f"{entity['type']}: {entity['value']}")
        ]))
    
    if 'multiple_entities' in params:
        entities = params['multiple_entities']
        content.append(html.P([
            html.Strong("Detected Entities: "),
            html.Ul([
                html.Li(html.Code(f"{e['type']}: {e['value']}"))
                for e in entities
            ])
        ]))
    
    # Show key parameters
    key_params = ['top_k_entities', 'community_level', 'comparison_mode', 'strict_entity_focus']
    param_list = []
    for param in key_params:
        if param in params:
            param_list.append(html.Li(f"{param}: {params[param]}"))
    
    if param_list:
        content.append(html.Div([
            html.Strong("Parameters:"),
            html.Ul(param_list)
        ]))
    
    return html.Div(content)

def render_query_history() -> html.Div:
    """Render the query history."""
    if not query_history:
        return html.P("No queries yet", className="text-muted")
    
    history_items = []
    for item in query_history:
        badge_color = "success" if item['auto_routed'] else "info"
        history_items.append(
            html.Li([
                html.Small(item['timestamp'].strftime("%H:%M:%S"), className="text-muted me-2"),
                html.Span(item['method'].upper(), className=f"badge bg-{badge_color} me-2"),
                html.Span(item['query'][:100] + "..." if len(item['query']) > 100 else item['query'])
            ], className="mb-2")
        )
    
    return html.Ul(history_items, className="list-unstyled")

def render_error(error_msg: str) -> html.Div:
    """Render an error message."""
    return dbc.Alert([
        html.H5("❌ Error", className="alert-heading"),
        html.P(error_msg)
    ], color="danger")

# Add some custom CSS
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                background-color: #f8f9fa;
            }
            .card {
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            .card-header {
                background-color: #e9ecef;
            }
            pre {
                white-space: pre-wrap;
                word-wrap: break-word;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == "__main__":
    print("🚀 Starting GraphRAG Query UI...")
    print(f"📁 GraphRAG Root: {GRAPHRAG_ROOT}")
    print("🌐 Open http://localhost:8050 in your browser")
    print("Press Ctrl+C to stop")
    
    app.run(debug=True, host='0.0.0.0', port=8050) 