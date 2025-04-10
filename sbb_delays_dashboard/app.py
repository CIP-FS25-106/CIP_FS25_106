import os
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Import utility functions and components
from utils.data_processing import (
    load_historical_data, filter_data, calculate_delay_stats,
    get_delay_by_time, get_delay_by_station_and_category, get_delay_categories_distribution
)
from components.header import create_header, create_filter_bar, create_kpi_cards
from components.visualizations import (
    create_delay_distribution_chart, create_train_category_chart,
    create_station_comparison_chart, create_time_of_day_chart,
    create_day_of_week_chart, create_bubble_chart
)

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
app.title = "SBB Train Delays Dashboard"
server = app.server

# Load the data
df = load_historical_data()

# Get unique values for filters
available_stations = sorted(df['station_name'].unique().tolist()) if 'station_name' in df.columns else []
available_categories = sorted(df['train_category'].unique().tolist()) if 'train_category' in df.columns else []

# Set default date range
if 'arrival_planned' in df.columns:
    default_start_date = df['arrival_planned'].min().date()
    default_end_date = df['arrival_planned'].max().date()
else:
    default_start_date = datetime(2022, 1, 1).date()
    default_end_date = datetime(2024, 12, 31).date()

# App layout
app.layout = html.Div(
    children=[
        # App header
        create_header(),
        
        # Main content container
        html.Div(
            className="container",
            children=[
                # Data Store for holding filtered data
                dcc.Store(id='filtered-data'),
                
                # Dummy div for clientside callback
                html.Div(id="dummy-output", style={"display": "none"}),
                
                # Filter bar
                create_filter_bar(available_stations, available_categories),
                
                # KPI Cards
                html.Div(id="kpi-container", children=create_kpi_cards()),
                
                # Overview Section
                html.Div(
                    id="overview",
                    children=[
                        html.H2("Overview of Train Delays", className="section-title"),
                        html.P(
                            "This dashboard visualizes train delay patterns across Swiss railway stations. "
                            "It analyzes historical data from 2022-2024, focusing on key stations and various train categories.",
                            className="section-description"
                        ),
                        
                        # Delay Distribution and Train Category Analysis
                        html.Div(
                            className="chart-row",
                            children=[
                                html.Div(
                                    className="chart-column",
                                    children=[
                                        html.Div(
                                            className="chart-section",
                                            children=[
                                                html.H3("Delay Distribution"),
                                                html.P(
                                                    "The distribution of train delays across different categories.",
                                                    className="chart-description"
                                                ),
                                                html.Div(id="delay-distribution-container"),
                                            ]
                                        )
                                    ]
                                ),
                                html.Div(
                                    className="chart-column",
                                    children=[
                                        html.Div(
                                            className="chart-section",
                                            children=[
                                                html.H3("Train Category Analysis"),
                                                html.P(
                                                    "Average delay by train category, showing which train types experience longer delays.",
                                                    className="chart-description"
                                                ),
                                                html.Div(id="train-category-container"),
                                            ]
                                        )
                                    ]
                                )
                            ]
                        ),
                    ]
                ),
                
                # Station Performance Section
                html.Div(
                    id="stations",
                    children=[
                        html.H2("Station Performance", className="section-title"),
                        html.P(
                            "Compare delay patterns across different stations in the Swiss railway network.",
                            className="section-description"
                        ),
                        
                        # Station Comparison and Bubble Chart
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Station Delay Comparison"),
                                html.P(
                                    "Breakdown of delay categories at each station, showing the percentage of trains in each delay category.",
                                    className="chart-description"
                                ),
                                html.Div(id="station-comparison-container"),
                            ]
                        ),
                        
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Station Delay Analysis: Frequency vs Severity"),
                                html.P(
                                    "Bubble chart comparing stations by the frequency and severity of delays. "
                                    "Bubble size represents the number of trains.",
                                    className="chart-description"
                                ),
                                html.Div(id="station-bubble-container"),
                            ]
                        ),
                    ]
                ),
                
                # Temporal Patterns Section
                html.Div(
                    id="temporal",
                    children=[
                        html.H2("Temporal Patterns", className="section-title"),
                        html.P(
                            "Analyze how train delays vary by time of day and day of the week.",
                            className="section-description"
                        ),
                        
                        # Day of Week and Time of Day Analysis
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Delays by Day of Week"),
                                html.P(
                                    "Heatmap showing the percentage of delayed trains by day of the week for each station.",
                                    className="chart-description"
                                ),
                                html.Div(id="day-of-week-container"),
                            ]
                        ),
                        
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Delays by Hour of Day"),
                                html.P(
                                    "Line chart showing how delays vary throughout the day for each station.",
                                    className="chart-description"
                                ),
                                html.Div(id="time-of-day-container"),
                            ]
                        ),
                    ]
                ),
                
                # Train Categories Section
                html.Div(
                    id="categories",
                    children=[
                        html.H2("Train Categories", className="section-title"),
                        html.P(
                            "Explore how different train types perform in terms of punctuality and delays.",
                            className="section-description"
                        ),
                    ]
                ),
                
                # Footer
                html.Footer(
                    className="footer",
                    children=[
                        html.Div(
                            html.P(
                                "SBB Train Delays Analysis Dashboard - Created with Dash by Roger Jeasy, Sahra Baettig, Mikaël Bonvin",
                                style={"textAlign": "center", "margin": "40px 0", "color": "#6c757d"}
                            ),
                            style={"maxWidth": "1200px", "margin": "0 auto"}
                        )
                    ]
                )
            ]
        )
    ]
)

# Define callback to update filtered data and all visualizations
@app.callback(
    Output('filtered-data', 'data'),
    [Input('filter-button', 'n_clicks')],
    [
        State('station-filter', 'value'),
        State('category-filter', 'value'),
        State('date-range-filter', 'start_date'),
        State('date-range-filter', 'end_date')
    ],
    prevent_initial_call=False
)
def update_filtered_data(n_clicks, stations, categories, start_date, end_date):
    # Convert date strings to datetime objects if provided
    if start_date:
        start_date = pd.to_datetime(start_date)
    if end_date:
        end_date = pd.to_datetime(end_date)
    
    # Filter the data
    filtered_df = filter_data(df, stations, categories, start_date, end_date)
    
    # Convert to JSON for storage
    return filtered_df.to_json(date_format='iso', orient='split')

# Callback to update KPI values
@app.callback(
    [
        Output('total-trains-value', 'children'),
        Output('avg-delay-value', 'children'),
        Output('on-time-rate-value', 'children'),
        Output('delayed-rate-value', 'children')
    ],
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_kpis(json_data):
    if not json_data:
        return "0", "0.0", "0.0", "0.0"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Calculate statistics
    stats = calculate_delay_stats(filtered_df)
    
    # Format values for display
    total_trains = f"{stats['total_trains']:,}"
    avg_delay = f"{stats['avg_delay']:.1f}"
    on_time_rate = f"{stats['pct_on_time']:.1f}"
    delayed_rate = f"{stats['pct_delayed']:.1f}"
    
    return total_trains, avg_delay, on_time_rate, delayed_rate

# Callback to update the delay distribution chart
@app.callback(
    Output('delay-distribution-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_delay_distribution(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_delay_distribution_chart(filtered_df)

# Callback to update the train category chart
@app.callback(
    Output('train-category-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_train_category_chart(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_train_category_chart(filtered_df)

# Callback to update the station comparison chart
@app.callback(
    Output('station-comparison-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_station_comparison(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_station_comparison_chart(filtered_df)

# Callback to update the station bubble chart
@app.callback(
    Output('station-bubble-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_station_bubble(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_bubble_chart(filtered_df)

# Callback to update the day of week chart
@app.callback(
    Output('day-of-week-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_day_of_week(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_day_of_week_chart(filtered_df)

# Callback to update the time of day chart
@app.callback(
    Output('time-of-day-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_time_of_day(json_data):
    if not json_data:
        return "No data available"
    
    # Parse the JSON data
    filtered_df = pd.read_json(json_data, orient='split')
    
    # Create the chart
    return create_time_of_day_chart(filtered_df)

# Callback for navigation links
@app.callback(
    [
        Output("overview", "style"),
        Output("stations", "style"),
        Output("temporal", "style"),
        Output("categories", "style"),
        Output("nav-overview", "active"),
        Output("nav-stations", "active"),
        Output("nav-temporal", "active"),
        Output("nav-categories", "active")
    ],
    [
        Input("nav-overview", "n_clicks"),
        Input("nav-stations", "n_clicks"),
        Input("nav-temporal", "n_clicks"),
        Input("nav-categories", "n_clicks")
    ],
    prevent_initial_call=False
)
def navigate_to_section(overview_clicks, stations_clicks, temporal_clicks, categories_clicks):
    # Determine which button was clicked
    ctx = dash.callback_context
    
    # Default style - all sections visible
    section_styles = [{"display": "block"} for _ in range(4)]
    active_states = [False] * 4
    
    # If this is the initial call, make Overview active by default
    if not ctx.triggered:
        active_states[0] = True
        return section_styles + active_states
    
    # Get the ID of the button that was clicked
    button_id = ctx.triggered[0]['prop_id'].split('.')[0]
    
    # Set the appropriate section to active based on which button was clicked
    if button_id == "nav-overview":
        active_states[0] = True
        # Scroll to overview section
        section_styles[0]["scrollMarginTop"] = "70px"
    elif button_id == "nav-stations":
        active_states[1] = True
        # Scroll to stations section
        section_styles[1]["scrollMarginTop"] = "70px"
    elif button_id == "nav-temporal":
        active_states[2] = True
        # Scroll to temporal section
        section_styles[2]["scrollMarginTop"] = "70px"
    elif button_id == "nav-categories":
        active_states[3] = True
        # Scroll to categories section
        section_styles[3]["scrollMarginTop"] = "70px"
        
    return section_styles + active_states

# Add clientside callback for smooth scrolling
app.clientside_callback(
    """
    function(overview_clicks, stations_clicks, temporal_clicks, categories_clicks) {
        return window.dash_clientside.clientside.smooth_scroll(
            overview_clicks, stations_clicks, temporal_clicks, categories_clicks
        );
    }
    """,
    Output("dummy-output", "children"),
    [
        Input("nav-overview", "n_clicks"),
        Input("nav-stations", "n_clicks"),
        Input("nav-temporal", "n_clicks"),
        Input("nav-categories", "n_clicks")
    ],
    prevent_initial_call=True
)

# Callback to reset filters
@app.callback(
    [
        Output('station-filter', 'value'),
        Output('category-filter', 'value'),
        Output('date-range-filter', 'start_date'),
        Output('date-range-filter', 'end_date')
    ],
    [Input('reset-button', 'n_clicks')],
    prevent_initial_call=True
)
def reset_filters(n_clicks):
    if n_clicks:
        return (
            available_stations,  # Reset to all stations
            available_categories[:5] if len(available_categories) > 5 else available_categories,  # Reset to initial categories
            default_start_date,  # Reset to default start date
            default_end_date     # Reset to default end date
        )
    # This should never execute since we're preventing initial call
    return dash.no_update, dash.no_update, dash.no_update, dash.no_update

# Run the app
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run(host='0.0.0.0', port=port)