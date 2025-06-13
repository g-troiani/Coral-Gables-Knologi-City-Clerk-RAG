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
from typing import Dict, Any


# Add project root to path
# Handle both cases: script in root or in a subdirectory
current_file = Path(__file__).resolve()
if current_file.parent.name == "scripts" or current_file.parent.name == "ui":
    project_root = current_file.parent.parent
else:
    project_root = current_file.parent
sys.path.append(str(project_root))

from graph_rag_stages.phase3_querying import (
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
                                    {"label": "Show data sources", "value": "sources"},
                                    {"label": "Verbose results", "value": "verbose"}
                                ],
                                value=["community", "routing", "sources"],
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

def create_data_sources_display(data_sources):
    """Create a formatted display of data sources used in the query."""
    
    # Handle empty or None data_sources
    if not data_sources:
        return html.Div([
            html.Hr(style={'margin': '20px 0', 'border-top': '1px solid #e0e0e0'}),
            html.Div([
                html.H4("📊 Data Sources", style={'margin-bottom': '10px', 'color': '#333'}),
                html.P("No data sources tracked for this query.", style={
                    'background-color': '#f5f5f5',
                    'padding': '10px',
                    'border-radius': '5px',
                    'color': '#666'
                })
            ])
        ])
    
    # Get lists of items
    entities = data_sources.get('entities', [])
    relationships = data_sources.get('relationships', [])
    sources = data_sources.get('sources', [])
    text_units = data_sources.get('text_units', [])
    
    # Create summary
    summary_parts = []
    
    if entities:
        entity_ids = [str(e.get('id', 'Unknown')) for e in entities[:10]]
        ids_str = ', '.join(entity_ids)
        if len(entities) > 10:
            ids_str += f", ... +{len(entities) - 10} more"
        summary_parts.append(f"Entities ({ids_str})")
    
    if relationships:
        rel_ids = [str(r.get('id', 'Unknown')) for r in relationships[:10]]
        ids_str = ', '.join(rel_ids)
        if len(relationships) > 10:
            ids_str += f", ... +{len(relationships) - 10} more"
        summary_parts.append(f"Relationships ({ids_str})")
    
    if sources:
        source_ids = [str(s.get('id', 'Unknown')) for s in sources[:10]]
        ids_str = ', '.join(source_ids)
        if len(sources) > 10:
            ids_str += f", ... +{len(sources) - 10} more"
        summary_parts.append(f"Sources ({ids_str})")
    
    summary_text = "Data: " + "; ".join(summary_parts) + "." if summary_parts else "Data: No sources tracked."
    
    # Create expandable sections
    details_sections = []
    
    # Entities section with better formatting
    if entities:
        entity_items = []
        for entity in entities[:20]:
            entity_items.append(
                html.Li([
                    html.Div([
                        html.Strong(f"[{entity.get('id', 'Unknown')}] {entity.get('title', 'Unknown')}"),
                        html.Span(f" ({entity.get('type', 'Unknown')})", 
                                style={'color': '#666', 'font-size': '0.9em'})
                    ]),
                    html.P(entity.get('description', 'No description available')[:200] + '...' 
                          if len(entity.get('description', '')) > 200 else entity.get('description', ''),
                          style={'margin': '5px 0 0 20px', 'color': '#555', 'font-size': '0.9em'})
                ], style={'margin-bottom': '10px', 'list-style': 'none'})
            )
        
        if len(entities) > 20:
            entity_items.append(
                html.Li(f"... and {len(entities) - 20} more entities", 
                       style={'font-style': 'italic', 'color': '#666'})
            )
        
        details_sections.append(
            html.Details([
                html.Summary([
                    html.Span("📊 ", style={'font-size': '1.2em'}),
                    f"Entities Used ({len(entities)})"
                ], style={'cursor': 'pointer', 'font-weight': 'bold', 'padding': '10px 0'}),
                html.Ul(entity_items, style={'padding-left': '20px', 'margin-top': '10px'})
            ], style={'margin': '15px 0', 'border-left': '3px solid #4a90e2', 'padding-left': '10px'})
        )
    
    # Relationships section
    if relationships:
        rel_items = []
        for rel in relationships[:15]:
            rel_items.append(
                html.Li([
                    html.Div([
                        html.Strong(f"[{rel.get('id', 'Unknown')}] "),
                        html.Span(f"{rel.get('source', 'Unknown')} → {rel.get('target', 'Unknown')}", 
                                style={'color': '#2c5aa0'})
                    ]),
                    html.P([
                        html.Em(rel.get('description', 'No description')[:150] + '...' 
                               if len(rel.get('description', '')) > 150 else rel.get('description', '')),
                        html.Span(f" (weight: {rel.get('weight', 0):.2f})", 
                                style={'color': '#666', 'font-size': '0.9em'})
                    ], style={'margin': '5px 0 0 20px', 'color': '#555', 'font-size': '0.9em'})
                ], style={'margin-bottom': '10px', 'list-style': 'none'})
            )
        
        if len(relationships) > 15:
            rel_items.append(
                html.Li(f"... and {len(relationships) - 15} more relationships", 
                       style={'font-style': 'italic', 'color': '#666'})
            )
        
        details_sections.append(
            html.Details([
                html.Summary([
                    html.Span("🔗 ", style={'font-size': '1.2em'}),
                    f"Relationships Used ({len(relationships)})"
                ], style={'cursor': 'pointer', 'font-weight': 'bold', 'padding': '10px 0'}),
                html.Ul(rel_items, style={'padding-left': '20px', 'margin-top': '10px'})
            ], style={'margin': '15px 0', 'border-left': '3px solid #e24a4a', 'padding-left': '10px'})
        )
    
    # Sources section
    if sources:
        source_items = []
        for source in sources[:10]:
            source_items.append(
                html.Li([
                    html.Div([
                        html.Strong(f"[{source.get('id', 'Unknown')}] {source.get('title', 'Unknown')}"),
                        html.Span(f" ({source.get('type', 'document')})", 
                                style={'color': '#666', 'font-size': '0.9em'})
                    ]),
                    html.P(source.get('text_preview', '')[:150] + '...' 
                          if len(source.get('text_preview', '')) > 150 else source.get('text_preview', ''),
                          style={'margin': '5px 0 0 20px', 'color': '#555', 'font-size': '0.9em', 'font-style': 'italic'})
                ], style={'margin-bottom': '10px', 'list-style': 'none'})
            )
        
        if len(sources) > 10:
            source_items.append(
                html.Li(f"... and {len(sources) - 10} more sources", 
                       style={'font-style': 'italic', 'color': '#666'})
            )
        
        details_sections.append(
            html.Details([
                html.Summary([
                    html.Span("📄 ", style={'font-size': '1.2em'}),
                    f"Source Documents ({len(sources)})"
                ], style={'cursor': 'pointer', 'font-weight': 'bold', 'padding': '10px 0'}),
                html.Ul(source_items, style={'padding-left': '20px', 'margin-top': '10px'})
            ], style={'margin': '15px 0', 'border-left': '3px solid #4ae255', 'padding-left': '10px'})
        )
    
    # Combine everything
    return html.Div([
        html.Hr(style={'margin': '20px 0', 'border-top': '1px solid #e0e0e0'}),
        html.Div([
            html.H4("📊 Data Sources", style={'margin-bottom': '15px', 'color': '#333'}),
            html.Div(summary_text, style={
                'background-color': '#f5f5f5',
                'padding': '12px',
                'border-radius': '5px',
                'font-family': 'monospace',
                'font-size': '14px',
                'color': '#333',
                'border': '1px solid #ddd'
            }),
            html.Div(details_sections, style={'margin-top': '20px'})
        ], style={
            'background-color': '#fafafa',
            'padding': '20px',
            'border-radius': '8px',
            'border': '1px solid #e0e0e0'
        })
    ])

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
        
        # Run the query
        result = loop.run_until_complete(
            query_engine.query(
                query=query_text,
                method=actual_method if method != "auto" else None,
                **params
            )
        )
        
        # Extract data sources
        data_sources = result.get('data_sources', result.get('context_data', {}))
        
        # Format the main answer with proper markdown
        answer_content = dcc.Markdown(
            result.get('answer', 'No response generated.'),
            style={
                'padding': '20px',
                'backgroundColor': '#f8f9fa',
                'borderRadius': '8px',
                'lineHeight': '1.6',
                'whiteSpace': 'pre-wrap'  # Preserve formatting
            }
        )
        
        # Create data sources display if requested
        sources_display = html.Div()
        if "sources" in options:
            sources_display = create_data_sources_display(data_sources)
        
        # Combine results
        results_content = html.Div([
            html.H3("Answer:", style={'marginBottom': '15px'}),
            answer_content,
            sources_display
        ])
        
        # Add to history
        query_history.insert(0, {
            "timestamp": datetime.now(),
            "query": query_text,
            "method": actual_method,
            "auto_routed": method == "auto"
        })
        
        # Limit history to 10 items
        query_history = query_history[:10]
        
        routing_content = render_routing_info(routing_details, actual_method) if "routing" in options else ""
        show_routing = "routing" in options
        
        return results_content, routing_content, show_routing, render_query_history(), ""
        
    except Exception as e:
        log.error(f"Query failed: {e}")
        return render_error(f"Query failed: {str(e)}"), "", False, dash.no_update, ""

def render_results(result, options):
    """Render query results with all source information."""
    
    answer = result.get('answer', 'No answer available')
    sources_info = result.get('sources_info', {})
    entity_chunks = result.get('entity_chunks', [])
    metadata = result.get('routing_metadata', {})
    
    # Clean up the answer (remove metadata lines)
    if isinstance(answer, str):
        lines = answer.split('\n')
        cleaned_lines = [line for line in lines if not line.startswith(('INFO:', 'WARNING:', 'DEBUG:', 'SUCCESS:'))]
        answer = '\n'.join(cleaned_lines).strip()
    
    content = [
        html.H5("📝 Answer:", className="mb-3"),
        dcc.Markdown(answer, className="p-3 bg-light rounded"),
    ]
    
    # Show data sources if requested
    if "sources" in options and sources_info:
        content.extend([
            html.Hr(),
            html.H5("📊 Data Sources:", className="mb-3"),
            render_all_sources(sources_info, entity_chunks)
        ])
    
    # Show verbose metadata if requested
    if "verbose" in options and metadata:
        content.extend([
            html.Hr(),
            html.H6("🔍 Query Metadata:"),
            html.Pre(json.dumps(metadata, indent=2), className="bg-dark text-light p-3 rounded")
        ])
    
    return html.Div(content)

def render_all_sources(sources_info, entity_chunks):
    """Render all source information comprehensively."""
    content = []
    
    # Show raw references first
    raw_refs = sources_info.get('raw_references', {})
    if any(raw_refs.values()):
        ref_text = []
        if raw_refs.get('entities'):
            ref_text.append(f"Entities: {', '.join(raw_refs['entities'])}")
        if raw_refs.get('reports'):
            ref_text.append(f"Reports: {', '.join(raw_refs['reports'])}")
        if raw_refs.get('sources'):
            ref_text.append(f"Sources: {', '.join(raw_refs['sources'])}")
        
        content.append(
            dbc.Alert([
                html.Strong("📋 References in Answer: "),
                html.Br(),
                html.Code(' | '.join(ref_text))
            ], color="info", className="mb-3")
        )
    
    # Show resolved reports (for GLOBAL search)
    reports = sources_info.get('reports', [])
    if reports:
        content.append(html.H6("📑 Community Reports Used:", className="mb-2"))
        for report in reports[:10]:  # Limit to first 10
            content.append(
                dbc.Card([
                    dbc.CardBody([
                        html.Strong(f"Report #{report['id']}"),
                        html.Span(f" (Level {report.get('level', '?')})", className="text-muted"),
                        html.P(report.get('summary', 'No summary available'), 
                               className="small text-muted mt-1 mb-0")
                    ])
                ], className="mb-2", color="light", outline=True)
            )
        if len(reports) > 10:
            content.append(html.P(f"... and {len(reports) - 10} more reports", className="text-muted"))
    
    # Show resolved entities (for LOCAL search)
    entities = sources_info.get('entities', [])
    if entities:
        content.append(html.H6("🎯 Entities Referenced:", className="mb-2 mt-3"))
        for entity in entities[:10]:  # Limit to first 10
            content.append(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Span(entity['type'].upper(), 
                                     className=f"badge bg-{get_entity_color(entity['type'])} me-2"),
                            html.Strong(entity['title']),
                            html.Span(f" (#{entity['id']})", className="text-muted small")
                        ]),
                        html.P(entity.get('description', ''), 
                               className="small text-muted mt-1 mb-0")
                    ])
                ], className="mb-2", color="light", outline=True)
            )
        if len(entities) > 10:
            content.append(html.P(f"... and {len(entities) - 10} more entities", className="text-muted"))
    
    # Show entity chunks (the actual retrieved content)
    if entity_chunks:
        content.append(html.H6("📄 Retrieved Content Chunks:", className="mb-2 mt-3"))
        for chunk in entity_chunks[:5]:  # Show first 5 chunks
            source_info = chunk.get('source', {})
            content.append(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Span(chunk['type'].upper(), 
                                     className=f"badge bg-{get_entity_color(chunk['type'])} me-2"),
                            html.Strong(chunk['title'])
                        ]),
                        html.P(chunk.get('description', '')[:200] + "..." 
                               if len(chunk.get('description', '')) > 200 
                               else chunk.get('description', ''), 
                               className="small mt-2"),
                        html.Hr(className="my-2"),
                        html.Small([
                            html.Strong("Source: "),
                            f"{source_info.get('type', 'Unknown')} - {source_info.get('meeting_date', 'N/A')}",
                            html.Br(),
                            html.Strong("File: "),
                            html.Code(source_info.get('source_file', 'Unknown'), className="small")
                        ], className="text-muted")
                    ])
                ], className="mb-2")
            )
        if len(entity_chunks) > 5:
            content.append(html.P(f"... and {len(entity_chunks) - 5} more chunks", className="text-muted"))
    
    return html.Div(content)

def get_entity_color(entity_type):
    """Get color for entity type badge."""
    color_map = {
        'AGENDA_ITEM': 'primary',
        'ORDINANCE': 'success', 
        'RESOLUTION': 'warning',
        'PERSON': 'info',
        'ORGANIZATION': 'secondary',
        'MEETING': 'danger',
        'DOCUMENT': 'dark'
    }
    return color_map.get(entity_type.upper(), 'light')

def render_entity_card(entity, highlight=False, is_related=False):
    """Render a single entity card with proper formatting."""
    
    # Determine card color based on entity type
    color_map = {
        'AGENDA_ITEM': 'primary',
        'ORDINANCE': 'success',
        'RESOLUTION': 'warning',
        'PERSON': 'info',
        'ORGANIZATION': 'secondary',
        'referenced_entity': 'danger'
    }
    
    border_color = color_map.get(entity.get('entity_type', entity.get('type', '')), 'light')
    
    card_content = [
        html.H6([
            html.Span(
                entity.get('entity_type', entity.get('type', '')).upper(), 
                className=f"badge bg-{border_color} me-2"
            ),
            entity['title'],
            html.Span(
                f" (Entity #{entity.get('id', entity.get('entity_id', ''))})",
                className="text-muted small"
            ) if entity.get('id') or entity.get('entity_id') else ""
        ]),
        html.P(
            entity.get('description', ''), 
            className="text-muted small mb-2",
            style={"maxHeight": "100px", "overflow": "auto"}
        ),
    ]
    
    # Add relationship info if this is a related entity
    if is_related and entity.get('relationship'):
        card_content.insert(1, html.P([
            html.Strong("Relationship: "),
            html.Em(entity['relationship'][:100] + "..." if len(entity['relationship']) > 100 else entity['relationship'])
        ], className="small"))
    
    # Add source document info if available
    source_doc = entity.get('source_document', {})
    if source_doc:
        card_content.append(
            html.Div([
                html.Hr(className="my-2"),
                html.Small([
                    html.Strong("Source: "),
                    f"{source_doc.get('type', 'Document')} - {source_doc.get('meeting_date', 'N/A')}",
                    html.Br(),
                    html.Strong("File: "),
                    html.Code(source_doc.get('source_file', 'Unknown'), className="small")
                ], className="text-muted")
            ])
        )
    
    return dbc.Card(
        dbc.CardBody(card_content),
        className="mb-2",
        color=border_color if highlight else None,
        outline=True,
        style={"border-width": "2px"} if highlight else {}
    )

def render_routing_info(routing_details, actual_method):
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

def render_query_history():
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

def render_error(error_msg):
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