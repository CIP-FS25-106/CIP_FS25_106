"""
loading.py - Enhanced loading component for Swiss Train Delays Dashboard

This module provides an interactive and informative loading screen
with animations, progress updates, and railway facts to improve
the user experience during data loading.
"""

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State


def create_loading_section():
    """
    Create an enhanced loading section with animated progress indicator, 
    updating messages, and professional styling.
    """
    return html.Div(
        className="loading-container",
        style={
            "textAlign": "center",
            "padding": "30px",
            "backgroundColor": "#f8f9fa",
            "borderRadius": "8px",
            "boxShadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
            "maxWidth": "800px",
            "margin": "0 auto"
        },
        children=[
            # Swiss Railways Logo with Animation
            html.Div(
                className="swiss-spinner",
                children=[
                    html.Div(
                        className="swiss-spinner-outer",
                        children=[
                            html.Div(
                                className="swiss-spinner-inner",
                                children=[
                                    html.Div(className="swiss-spinner-center")
                                ]
                            )
                        ]
                    )
                ]
            ),
            
            # Loading Title
            html.H3("Swiss Train Delays Dashboard", 
                   style={"color": "#333", "marginBottom": "20px", "fontWeight": "500", "marginTop": "20px"}),
            
            # Animated Progress Bar
            html.Div(
                className="progress mb-3 animated-progress",
                style={"height": "16px", "borderRadius": "8px", "overflow": "hidden"},
                children=[
                    html.Div(
                        id="loading-progress-bar",
                        className="progress-bar progress-bar-striped progress-bar-animated",
                        style={"width": "100%", "backgroundColor": "#FF0000"}
                    )
                ]
            ),
            
            # Current Operation Display
            html.Div(
                id="loading-operation-text",
                className="fade-in",
                style={"fontWeight": "bold", "fontSize": "1.1em", "margin": "15px 0", "color": "#333"},
                children=dcc.Markdown("**Initializing Data Pipeline...**")
            ),
            
            # Detailed Status with Animation
            html.Div(
                id="loading-status",
                style={"marginBottom": "20px"},
                children=[
                    dbc.Spinner(
                        size="sm", 
                        color="danger", 
                        type="grow",
                        children=html.Span(
                            id="loading-status-text",
                            className="fade-in",
                            children="Connecting to data sources...",
                            style={"marginLeft": "10px", "color": "#666"}
                        )
                    )
                ]
            ),
            
            # Information Panel
            dbc.Card(
                className="mb-4",
                style={"backgroundColor": "#fff", "border": "none", "boxShadow": "0 2px 4px rgba(0,0,0,0.05)"},
                children=[
                    dbc.CardBody([
                        html.H5("Processing Information", className="card-title", style={"fontSize": "1rem", "color": "#555"}),
                        html.Ul(
                            id="loading-progress-list",
                            style={"textAlign": "left", "color": "#666", "paddingLeft": "20px"},
                            children=[
                                html.Li("Streaming data from Cloudinary", id="loading-item-1", className="loading-list-item"),
                                html.Li("Processing data for key stations: Zürich HB, Luzern, Genève", id="loading-item-2", className="loading-list-item"),
                                html.Li("Calculating delay statistics and preparing visualizations", id="loading-item-3", className="loading-list-item"),
                                html.Li("Optimizing memory usage with streaming techniques", id="loading-item-4", className="loading-list-item"),
                            ]
                        )
                    ])
                ]
            ),
            
            # Did You Know Section
            html.Div(
                id="loading-facts",
                style={"marginTop": "15px", "padding": "15px", "backgroundColor": "#f0f5ff", "borderRadius": "6px"},
                children=[
                    html.H6("Did You Know?", style={"color": "#1561ad", "marginBottom": "10px"}),
                    html.Div(
                        id="loading-fact-text",
                        className="fade-in",
                        style={"color": "#444", "fontStyle": "italic", "fontSize": "0.9em"},
                        children="""
                            The Swiss railway network has more than 5,300 km of tracks and over 
                            1,800 stations, making it one of the densest rail networks in the world.
                        """
                    )
                ]
            ),
            
            # Estimated Time
            html.Div(
                style={"marginTop": "25px", "fontSize": "0.9em", "color": "#777"},
                children=[
                    html.Span("Estimated time remaining: "),
                    html.Span(id="loading-time-estimate", className="fade-in", children="2-4 minutes", style={"fontWeight": "bold"})
                ]
            ),
            
            # Hidden containers for callback interactions
            dcc.Store(id="loading-stage", data=1),
            dcc.Interval(id="loading-interval", interval=3000, n_intervals=0),
        ]
    )


def create_loading_callbacks(app):
    """
    Create callbacks to update the loading screen animations and content.
    This function should be called after app initialization.
    """
    # Facts about Swiss railways to display during loading
    loading_facts = [
        "The Swiss railway network has more than 5,300 km of tracks and over 1,800 stations, making it one of the densest rail networks in the world.",
        "Swiss Federal Railways (SBB) trains travel a combined distance of about 133,000 kilometers every day - equivalent to circling the Earth 3.3 times.",
        "The Swiss rail system is known for its punctuality, with over 90% of trains arriving within 3 minutes of their scheduled time.",
        "The world's steepest cogwheel railway is in Switzerland, connecting Alpnachstad to Pilatus with a maximum gradient of 48%.",
        "Switzerland's Gotthard Base Tunnel is the world's longest and deepest traffic tunnel at 57.09 km (35.5 miles).",
        "The Swiss Travel Pass allows unlimited travel on the Swiss Travel System network, including trains, buses, and boats.",
        "The famous Glacier Express connects St. Moritz and Zermatt, crossing 291 bridges and through 91 tunnels during its 8-hour journey.",
        "Nearly one-third of Switzerland's electricity consumption is used to power its railway system."
    ]
    
    # Loading operation status messages
    operation_stages = [
        "**Initializing Data Pipeline...**",
        "**Setting Up Streaming Process...**",
        "**Downloading Train Delay Data...**",
        "**Processing Zürich HB Station Data...**",
        "**Processing Luzern Station Data...**",
        "**Processing Genève Station Data...**",
        "**Aggregating Delay Statistics...**",
        "**Preparing Dashboard Visualizations...**",
        "**Finalizing Dashboard...**"
    ]
    
    # Detailed status messages
    status_messages = [
        "Connecting to data sources...",
        "Configuring memory-efficient processing...",
        "Streaming data from OpenTransportData Switzerland...",
        "Filtering and cleaning Zürich HB records...",
        "Analyzing delay patterns at Luzern station...",
        "Processing cross-border connections at Genève...",
        "Calculating delay statistics across all stations...",
        "Building interactive visualizations...",
        "Rendering dashboard components..."
    ]
    
    # Time estimates
    time_estimates = [
        "2-4 minutes",
        "2-3 minutes",
        "1-2 minutes",
        "about 1 minute",
        "about 1 minute",
        "less than 1 minute",
        "less than 30 seconds",
        "almost complete",
        "just a few moments"
    ]
    
    # Callback to update loading animation and text
    @app.callback(
        [Output("loading-fact-text", "children"),
         Output("loading-operation-text", "children"),
         Output("loading-status-text", "children"),
         Output("loading-time-estimate", "children"),
         Output("loading-stage", "data")],
        [Input("loading-interval", "n_intervals")],
        [State("loading-stage", "data")]
    )
    def update_loading_content(n_intervals, current_stage):
        if n_intervals is None:
            return loading_facts[0], operation_stages[0], status_messages[0], time_estimates[0], 1
        
        # Update the fact with a cycling pattern
        fact_index = n_intervals % len(loading_facts)
        fact = loading_facts[fact_index]
        
        # Update stage if needed, but don't exceed the max stage
        stage = min(current_stage + (1 if n_intervals % 3 == 0 and current_stage < len(operation_stages) else 0), 
                   len(operation_stages) - 1)
        
        return fact, operation_stages[stage], status_messages[stage], time_estimates[stage], stage
    
    # Update list items to show progress
    @app.callback(
        [Output("loading-item-1", "className"),
         Output("loading-item-2", "className"),
         Output("loading-item-3", "className"),
         Output("loading-item-4", "className")],
        [Input("loading-stage", "data")]
    )
    def update_list_progress(stage):
        # Add active class to list items based on the current stage
        active_items = min(stage + 1, 4)  # We have 4 list items
        classes = []
        for i in range(4):
            if i < active_items:
                classes.append("loading-list-item active")
            else:
                classes.append("loading-list-item")
        return classes

    return app