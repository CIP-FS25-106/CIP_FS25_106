"""
app.py - Swiss Train Delays Analysis Dashboard

This is the main application file for the Swiss Train Delays Analysis Dashboard.
It integrates components from the components package and data processing from
the utils package to create an interactive visualization dashboard.
"""

import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import logging
import time
import os
import traceback
from pathlib import Path

# Import components and utilities
from components.header import create_header
from components.visualizations import (
    create_delay_distribution_section,
    create_category_delay_section,
    create_station_delay_section,
    create_time_patterns_section
)
from utils.data_processing import load_and_prepare_data, TARGET_STATIONS, TARGET_STATIONS_ORIGINAL
from components.loading import create_loading_section, create_loading_callbacks


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize the Dash app with Bootstrap
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ],
    title="Swiss Train Delays Dashboard",
    update_title='Loading...'
)

# Make the app servable by gunicorn
server = app.server

# Configure app for performance
app.config.suppress_callback_exceptions = True

# Register loading callbacks
app = create_loading_callbacks(app)

# Error message component
def create_error_section(error_message):
    return html.Div(
        className="dashboard-section",
        children=[
            html.H2("Error Processing Data", className="section-title"),
            html.P(
                "There was an issue streaming or processing the data. See details below:",
                className="section-description text-danger"
            ),
            dbc.Card(
                dbc.CardBody([
                    html.Pre(
                        error_message,
                        className="error-details",
                        style={"whiteSpace": "pre-wrap", "overflow": "auto", "maxHeight": "300px"}
                    )
                ]),
                className="graph-card"
            ),
            html.Div(
                className="mt-4",
                children=[
                    html.P("Possible solutions:"),
                    html.Ul([
                        html.Li("Check your internet connection to ensure data files can be streamed"),
                        html.Li("The application may be experiencing high load - wait a few minutes and try again"),
                        html.Li("Try accessing the dashboard during non-peak hours"),
                        html.Li("Verify that your browser has sufficient memory available"),
                        html.Li(f"The dashboard is expecting data for these stations: {', '.join(TARGET_STATIONS_ORIGINAL)}"),
                    ]),
                    dbc.Button("Retry Loading Data", id="retry-button", color="primary", className="mt-3")
                ]
            )
        ]
    )

# Performance notice component
def create_performance_notice():
    return html.Div(
        className="mb-4",
        children=[
            dbc.Alert(
                [
                    html.H4("Memory-Optimized Processing", className="alert-heading"),
                    html.P(
                        "This dashboard processes train delay data using memory-efficient streaming techniques. "
                        "The data is processed in chunks without requiring local storage, making it suitable for "
                        "deployment on platforms with limited resources."
                    ),
                    html.Hr(),
                    html.P(
                        "The analysis uses a representative sample of data focusing on key stations: "
                        f"{', '.join(TARGET_STATIONS_ORIGINAL)}. Each visualization is calculated incrementally "
                        "to provide accurate insights while minimizing memory usage.",
                        className="mb-0"
                    )
                ],
                color="info",
                dismissable=True
            )
        ]
    )

# Main layout
app.layout = dbc.Container(
    fluid=True,
    className="container",
    children=[
        # Store loading state
        dcc.Store(id="loading-complete", data=False),
        dcc.Store(id="error-message", data=None),
        dcc.Store(id="available-stations", data=[]),
        dcc.Store(id="missing-stations", data=[]),
        
        # Header
        create_header(),
        
        # Initial loading screen
        html.Div(
            id="loading-section",
            children=[
                dbc.Card(
                    dbc.CardBody([
                        create_loading_section()
                    ]),
                    className="graph-card"
                )
            ]
        ),
        
        # Warning for long processing time
        html.Div(id="timeout-warning", className="mt-3"),
        
        # Performance notice (initially hidden)
        html.Div(
            id="performance-notice",
            style={"display": "none"},
            children=create_performance_notice()
        ),
        
        # Dashboard content (initially hidden)
        html.Div(
            id="dashboard-content",
            style={"display": "none"},
            children=[]
        ),
        
        # Error content (initially hidden)
        html.Div(
            id="error-content",
            style={"display": "none"},
            children=[]
        ),
        
        # Footer
        html.Footer(
            className="dashboard-footer",
            children=[
                html.Hr(),
                html.P(
                    [
                        "Swiss Train Delays Analysis Dashboard • ",
                        html.A("Data Source: OpenTransportData Archive", 
                               href="https://archive.opentransportdata.swiss/actual_data_archive.htm", 
                               target="_blank"),
                        " • ",
                        f"Created {time.strftime('%Y')}"
                    ],
                    className="text-center text-muted"
                )
            ]
        ),
        
        # Initial data loading trigger
        dcc.Interval(id="initial-load-trigger", interval=100, n_intervals=0, max_intervals=1),
        
        # Progress update timer
        dcc.Interval(id="processing-timer", interval=5000, n_intervals=0)
    ]
)

# Callback for timeout warning
@app.callback(
    Output("timeout-warning", "children"),
    [Input("processing-timer", "n_intervals"),
     Input("loading-complete", "data")]
)
def show_timeout_warning(n_intervals, loading_complete):
    if loading_complete:
        # Clear warning when loading is complete
        return None
    elif n_intervals and n_intervals > 5:  # After ~25 seconds
        return dbc.Alert(
            [
                html.Strong("Processing is taking longer than expected. "), 
                "Please continue to wait. The data is being streamed and processed in chunks to optimize memory usage."
            ],
            color="warning",
            dismissable=True
        )
    return None

# Callback to load data on startup
@app.callback(
    [Output("loading-complete", "data"),
     Output("dashboard-content", "children"),
     Output("error-message", "data"),
     Output("available-stations", "data"),
     Output("missing-stations", "data"),
     Output("loading-interval", "max_intervals")],
    [Input("initial-load-trigger", "n_intervals")],
    prevent_initial_call=True
)
def load_data_on_startup(n_intervals):
    try:
        logger.info("Loading data for dashboard...")
        
        # Attempt to load and prepare the data
        df = load_and_prepare_data()
        
        # Check which stations are actually available
        available_stations = df["station_name"].unique().tolist()
        missing_stations = [station for station in TARGET_STATIONS_ORIGINAL if station not in available_stations]
        
        logger.info(f"Available stations: {available_stations}")
        logger.info(f"Missing stations: {missing_stations}")
        
        # Create dashboard sections with loaded data
        sections = [
            create_delay_distribution_section(df),
            create_category_delay_section(df),
            create_station_delay_section(df),
            create_time_patterns_section(df)
        ]
        
        logger.info("Data loaded and dashboard sections created successfully")
        return True, sections, None, available_stations, missing_stations, 0  # Stop the timer
    
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        
        # Get full traceback for detailed error information
        error_details = traceback.format_exc()
        logger.error(f"Traceback: {error_details}")
        
        return False, [], error_details, [], [], 0    # Stop the timer

# Callback to show dashboard or error content based on loading result
@app.callback(
    [Output("loading-section", "style"),
     Output("dashboard-content", "style"),
     Output("error-content", "style"),
     Output("error-content", "children"),
     Output("performance-notice", "style")],
    [Input("loading-complete", "data"),
     Input("error-message", "data")],
    prevent_initial_call=True
)
def update_visibility(loading_complete, error_message):
    if error_message:
        # Show error content
        return {"display": "none"}, {"display": "none"}, {"display": "block"}, create_error_section(error_message), {"display": "none"}
    elif loading_complete:
        # Show dashboard content
        return {"display": "none"}, {"display": "block"}, {"display": "none"}, [], {"display": "block"}
    else:
        # Keep showing loading
        return {"display": "block"}, {"display": "none"}, {"display": "none"}, [], {"display": "none"}

# Callback for retry button
@app.callback(
    Output("initial-load-trigger", "max_intervals"),
    [Input("retry-button", "n_clicks")],
    prevent_initial_call=True
)
def retry_loading(n_clicks):
    if n_clicks:
        return 2  # Increment to trigger the loading callback again
    return 1

# Run the app
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run(host='0.0.0.0', port=port, debug=False)