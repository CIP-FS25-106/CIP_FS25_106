import json
import os
import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
import plotly.graph_objects as go
from plotly.colors import qualitative

# Load environment variables
load_dotenv()

# Import utility functions and components
from utils.data_processing import (
    load_historical_data, filter_data, calculate_delay_stats,
    get_delay_by_time, get_delay_by_station_and_category, 
    get_delay_categories_distribution
)
from components.header import create_header, create_filter_bar, create_kpi_cards
from components.visualizations import (
    create_overview_delay_plot,
    create_delay_distribution_chart, 
    create_train_category_chart,
    create_station_comparison_chart, 
    create_bubble_chart,
    create_day_of_week_chart, 
    create_time_of_day_chart
)

# SBB color scheme
SBB_COLORS = {
    'primary': '#CF0015',  # SBB red
    'light_bg': '#f8f9fa',
    'text': '#212529',
    'secondary_text': '#6c757d',
    'border': '#dee2e6',
    'on_time': '#88CCEE',
    'slight_delay': '#117733',
    'medium_delay': '#DDCC77',
    'severe_delay': '#CC6677',
    'cancelled': '#AA4499'
}

# Constants
DELAY_THRESHOLD = 2  # Minutes threshold for considering a train delayed

# Initialize the Dash app
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
app.title = "SBB Train Delays Dashboard"
server = app.server

# Configure Cloudinary URLs
cloudinary_urls_file = 'sbb_delays_dashboard/utils/cloudinary_urls.json'
urls = [
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315794/historical_transformed_part001.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315806/historical_transformed_part002.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315817/historical_transformed_part003.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315828/historical_transformed_part004.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315838/historical_transformed_part005.gz"
]
print(f"Using {len(urls)} hardcoded Cloudinary URLs")

# Load the data
print("Loading data...")
try:
    df = load_historical_data(urls=urls, urls_file=cloudinary_urls_file if urls is None else None)
    print(f"Data loaded with {len(df)} records")
except Exception as e:
    print(f"Error loading data: {e}")
    raise

# Get unique values for filters with safety checks
available_stations = sorted(df['station_name'].unique().tolist()) if 'station_name' in df.columns and len(df) > 0 else ['Zürich HB', 'Luzern', 'Genève']
if 'train_category' in df.columns and len(df) > 0:
    # Convert all categories to strings and filter out NaN values
    categories = df['train_category'].fillna('Unknown').astype(str).unique().tolist()
    available_categories = sorted(categories)
else:
    available_categories = ['IC', 'IR', 'S', 'RE', 'EC', 'TGV']
    
# Set default date range with safety checks
try:
    if 'arrival_planned' in df.columns and len(df) > 0 and hasattr(df['arrival_planned'], 'min') and hasattr(df['arrival_planned'].min(), 'date'):
        default_start_date = df['arrival_planned'].min().date()
        default_end_date = df['arrival_planned'].max().date()
    else:
        default_start_date = datetime(2022, 1, 1).date()
        default_end_date = datetime(2023, 12, 31).date()
except (AttributeError, TypeError) as e:
    print(f"Error setting date range: {e}")
    default_start_date = datetime(2022, 1, 1).date()
    default_end_date = datetime(2023, 12, 31).date()

print(f"Default date range: {default_start_date} to {default_end_date}")

# Add client-side JavaScript for smooth scrolling
app.clientside_callback(
    """
    function(overview_clicks, stations_clicks, temporal_clicks, categories_clicks) {
        // Get the current trigger
        var ctx = dash_clientside.callback_context;
        
        // If no trigger, return
        if (!ctx.triggered.length) {
            return window.dash_clientside.no_update;
        }
        
        // Get the triggered component ID
        var input_id = ctx.triggered[0]['prop_id'].split('.')[0];
        
        // Define the section IDs
        var sections = {
            "nav-overview": "overview",
            "nav-stations": "stations",
            "nav-temporal": "temporal",
            "nav-categories": "categories"
        };
        
        // Get the target section ID
        var target = sections[input_id];
        
        // If we have a target, scroll to it
        if (target) {
            var element = document.getElementById(target);
            if (element) {
                element.scrollIntoView({
                    behavior: 'smooth', 
                    block: 'start'
                });
            }
        }
        
        return null;
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

# App layout
app.layout = html.Div(
    className="dashboard-container",
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
                create_filter_bar(available_stations, available_categories, 
                                 default_start_date, default_end_date),
                
                # KPI Cards
                html.Div(id="kpi-container", children=create_kpi_cards()),
                
                # Overview Section
                html.Div(
                    id="overview",
                    className="dashboard-section",
                    children=[
                        html.H2("Overview of Train Delays", className="section-title"),
                        html.P(
                            "This dashboard visualizes train delay patterns across Swiss railway stations. "
                            "It analyzes historical data from 2022-2024, focusing on key stations and various train categories.",
                            className="section-description"
                        ),
                        
                        # Overview Delay Distribution Plot
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Overall Delay Distribution"),
                                html.P(
                                    "Overview of the distribution of delays across all trains, showing the frequency of each delay duration.",
                                    className="chart-description"
                                ),
                                html.Div(id="overview-delay-container"),
                            ]
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
                                                html.H3("Delay Categories Distribution"),
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
                    className="dashboard-section",
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
                    className="dashboard-section",
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
                    className="dashboard-section",
                    children=[
                        html.H2("Train Categories", className="section-title"),
                        html.P(
                            "Explore how different train types perform in terms of punctuality and delays.",
                            className="section-description"
                        ),
                        # Train Categories Analysis
                        html.Div(
                            className="chart-section",
                            children=[
                                html.H3("Performance by Train Category"),
                                html.P(
                                    "In-depth analysis of delay patterns across different train categories.",
                                    className="chart-description"
                                ),
                                html.Div(id="train-category-detail-container"),
                            ]
                        ),
                    ]
                ),
                
                # Footer
                html.Footer(
                    className="footer",
                    children=[
                        html.P(
                            "SBB Train Delays Analysis Dashboard - Created with Dash by Roger Jeasy, Sahra Baettig, Mikaël Bonvin",
                            className="footer-text"
                        ),
                        html.P(
                            "Updated to include all visualizations from the historical_data_analysis.py script",
                            className="footer-text"
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
    
    try:
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
    except Exception as e:
        print(f"Error updating KPIs: {e}")
        return "0", "0.0", "0.0", "0.0"

# Callback to update the overview delay plot
@app.callback(
    Output('overview-delay-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_overview_delay(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_overview_delay_plot(filtered_df)
    except Exception as e:
        print(f"Error updating overview delay plot: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the delay distribution chart
@app.callback(
    Output('delay-distribution-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_delay_distribution(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_delay_distribution_chart(filtered_df)
    except Exception as e:
        print(f"Error updating delay distribution: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the train category chart
@app.callback(
    Output('train-category-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_train_category_chart(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_train_category_chart(filtered_df)
    except Exception as e:
        print(f"Error updating train category chart: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the train category detail chart
@app.callback(
    Output('train-category-detail-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_train_category_detail(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create a more detailed version of the train category chart
        # This could show delay distributions per category
        fig = go.Figure()
        
        # Get unique categories
        categories = filtered_df['train_category'].unique()
        
        # For each category, create a box plot of delays
        for category in categories:
            category_data = filtered_df[filtered_df['train_category'] == category]
            fig.add_trace(go.Box(
                y=category_data['delay'],
                name=category,
                boxmean=True,  # Show mean line
                jitter=0.3,
                pointpos=-1.8,
                boxpoints='outliers',  # Only show outliers as points
                marker_color=SBB_COLORS['primary'],
                line_color=SBB_COLORS['primary']
            ))
        
        # Update layout
        fig.update_layout(
            title="Delay Distribution by Train Category",
            yaxis_title="Delay (minutes)",
            xaxis_title="Train Category",
            height=500,
            margin=dict(l=40, r=40, t=50, b=40),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text']),
            boxmode='group'
        )
        
        return dcc.Graph(figure=fig, id="train-category-detail-chart", className="chart-container")
    except Exception as e:
        print(f"Error updating train category detail chart: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the station comparison chart
@app.callback(
    Output('station-comparison-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_station_comparison(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_station_comparison_chart(filtered_df)
    except Exception as e:
        print(f"Error updating station comparison: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the station bubble chart
@app.callback(
    Output('station-bubble-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_station_bubble(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_bubble_chart(filtered_df)
    except Exception as e:
        print(f"Error updating station bubble chart: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the day of week chart
@app.callback(
    Output('day-of-week-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_day_of_week(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_day_of_week_chart(filtered_df)
    except Exception as e:
        print(f"Error updating day of week chart: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback to update the time of day chart
@app.callback(
    Output('time-of-day-container', 'children'),
    [Input('filtered-data', 'data')],
    prevent_initial_call=False
)
def update_time_of_day(json_data):
    if not json_data:
        return "No data available"
    
    try:
        # Parse the JSON data
        filtered_df = pd.read_json(json_data, orient='split')
        
        # Create the chart
        return create_time_of_day_chart(filtered_df)
    except Exception as e:
        print(f"Error updating time of day chart: {e}")
        return html.Div("Error loading chart. Please try refreshing the page.")

# Callback for navigation links
@app.callback(
    [
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
    
    # Default state - make overview active
    active_states = [True, False, False, False]
    
    # If this is not the initial call
    if ctx.triggered:
        # Get the ID of the button that was clicked
        button_id = ctx.triggered[0]['prop_id'].split('.')[0]
        
        # Set the appropriate section to active based on which button was clicked
        if button_id == "nav-overview":
            active_states = [True, False, False, False]
        elif button_id == "nav-stations":
            active_states = [False, True, False, False]
        elif button_id == "nav-temporal":
            active_states = [False, False, True, False]
        elif button_id == "nav-categories":
            active_states = [False, False, False, True]
        
    return active_states

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