"""
visualizations.py - Dashboard visualization components

This module provides the visualization components for the Swiss Train Delays Analysis Dashboard.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from dash import dcc, html
import numpy as np
import logging
from typing import Dict, List
from utils.data_processing import (
    DELAY_THRESHOLD, 
    TARGET_STATIONS, 
    TARGET_STATIONS_ORIGINAL,
    get_delay_category_data,
    get_category_delay_data,
    get_bubble_chart_data,
    get_weekday_heatmap_data,
    get_hourly_lineplot_data,
    load_and_prepare_data
)

# Configure logger
logger = logging.getLogger(__name__)


def create_delay_distribution_section(df: pd.DataFrame) -> html.Div:
    """
    Create the overview delay distribution section.
    
    Args:
        df: Prepared DataFrame
        
    Returns:
        html.Div: Overview section component
    """
    # Sample data for histogram to improve performance
    if len(df) > 50000:
        sample_size = min(50000, int(len(df) * 0.2))
        df_sample = df.sample(sample_size, random_state=42)
        logger.info(f"Sampled data for histogram from {len(df)} to {len(df_sample)} records")
    else:
        df_sample = df
    
    # Create histogram for delay distribution
    fig = px.histogram(
        df_sample, 
        x="DELAY",
        title="Distribution of Train Delays",
        labels={"DELAY": "Delay (minutes)"},
        opacity=0.8,
        color_discrete_sequence=['#4472C4'],
        marginal="box",
        range_x=[-5, 30]  # Limit x-axis range for better visualization
    )
    
    fig.update_layout(
        template="plotly_white",
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial"
        ),
        title_font=dict(size=20),
        xaxis_title_font=dict(size=14),
        yaxis_title_font=dict(size=14),
        yaxis_title="Number of Trains",
        autosize=True,
        margin=dict(l=40, r=40, t=70, b=40),
        legend_title_font=dict(size=14),
        legend_font=dict(size=12),
        height=500,  # Set a fixed height for better appearance
    )
    
    # Add a vertical line to mark delays above threshold
    fig.add_vline(
        x=DELAY_THRESHOLD, 
        line_width=2, 
        line_dash="dash", 
        line_color="red",
        annotation_text=f"Delay Threshold ({DELAY_THRESHOLD} min)",
        annotation_position="top right"
    )
    
    # Create section
    return html.Div(
        id="overview-section",
        className="dashboard-section",
        children=[
            html.H2("Delay Distribution Overview", className="section-title"),
            html.P(
                f"Analysis of train delay distribution across all stations, with delays greater than {DELAY_THRESHOLD} minutes marked as late.",
                className="section-description"
            ),
            dbc.Card(
                dbc.CardBody([
                    dcc.Graph(
                        id='delay-distribution-graph',
                        figure=fig,
                        config={'displayModeBar': True, 'responsive': True},
                        className="graph-container"
                    ),
                ]),
                className="graph-card"
            ),
            html.Div(
                className="insights-container",
                children=[
                    dbc.Card(
                        dbc.CardBody([
                            html.H4("Key Insights", className="insights-title"),
                            html.Ul([
                                html.Li("Most trains run on time or with minimal delays"),
                                html.Li(f"A significant cluster of delays falls within the {DELAY_THRESHOLD}-5 minute range"),
                                html.Li("Severe delays (15+ minutes) are relatively rare but impactful"),
                            ]),
                        ]),
                        className="insights-card"
                    ),
                ]
            )
        ]
    )


def create_category_delay_section(df: pd.DataFrame) -> html.Div:
    """
    Create the train category delay section.
    
    Args:
        df: Prepared DataFrame
        
    Returns:
        html.Div: Train category section component
    """
    # Get pre-aggregated data
    avg_by_category = get_category_delay_data()
    
    # Limit to top 10 categories for better visualization
    if len(avg_by_category) > 10:
        avg_by_category = avg_by_category.head(10)
    
    # Create bar chart
    fig = px.bar(
        avg_by_category, 
        x="train_category", 
        y="DELAY", 
        title="Average Delay per Train Category",
        labels={"train_category": "Train Category", "DELAY": "Average Delay (minutes)"},
        color="DELAY",
        color_continuous_scale="Viridis",
        text_auto='.1f'
    )
    
    fig.update_traces(
        textfont_size=12, 
        textangle=0, 
        textposition="outside", 
        cliponaxis=False
    )
    
    fig.update_layout(
        template="plotly_white",
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial"
        ),
        title_font=dict(size=20),
        xaxis_title_font=dict(size=14),
        yaxis_title_font=dict(size=14),
        autosize=True,
        margin=dict(l=40, r=40, t=70, b=40),
        coloraxis_showscale=False,
        xaxis={'categoryorder':'total descending'},
        height=500,  # Set a fixed height
    )
    
    # Create section
    return html.Div(
        id="category-section",
        className="dashboard-section",
        children=[
            html.H2("Train Category Analysis", className="section-title"),
            html.P(
                "Comparison of average delays across different train categories in the Swiss railway system.",
                className="section-description"
            ),
            dbc.Card(
                dbc.CardBody([
                    dcc.Graph(
                        id='category-delay-graph',
                        figure=fig,
                        config={'displayModeBar': True, 'responsive': True},
                        className="graph-container"
                    ),
                ]),
                className="graph-card"
            ),
            html.Div(
                className="insights-container",
                children=[
                    dbc.Card(
                        dbc.CardBody([
                            html.H4("Key Insights", className="insights-title"),
                            html.Ul([
                                html.Li("Long-distance and international trains experience higher delays"),
                                html.Li("Regional and commuter services tend to be more punctual"),
                                html.Li("Complex routes and cross-border services face more disruptions"),
                            ]),
                        ]),
                        className="insights-card"
                    ),
                ]
            )
        ]
    )


def create_station_delay_section(df: pd.DataFrame) -> html.Div:
    """
    Create the station delay analysis section.
    
    Args:
        df: Prepared DataFrame
        
    Returns:
        html.Div: Station section component
    """
    try:
        # Get list of available stations in the data
        available_stations = df["station_name"].unique().tolist()
        logger.info(f"Available stations for station delay section: {available_stations}")
        
        # Get pre-aggregated data
        counts = get_delay_category_data()
        
        # Define the categories order and colors
        categories = [
            "On time", 
            "2 to 5minutes", 
            "5 to 15minutes", 
            "more than 15minutes", 
            "Cancelled"
        ]
        
        colors = {
            "On time": "#88CCEE",
            "2 to 5minutes": "#117733",
            "5 to 15minutes": "#DDCC77",
            "more than 15minutes": "#CC6677",
            "Cancelled": "#AA4499"
        }
        
        # Create stacked bar chart for delay categories
        fig1 = go.Figure()
        
        for cat in categories:
            subset = counts[counts["DELAY_CAT"] == cat]
            station_data = {}
            
            for station in available_stations:
                val = subset[subset["station_name"] == station]["percentage"]
                station_data[station] = val.values[0] if not val.empty else 0
            
            fig1.add_trace(go.Bar(
                name=cat,
                y=list(station_data.keys()),
                x=list(station_data.values()),
                orientation='h',
                marker=dict(color=colors[cat]),
                text=[f"{x:.1f}%" for x in station_data.values()],
                textposition="inside",
                insidetextanchor="middle",
                width=0.6
            ))
        
        fig1.update_layout(
            title="Train Delay Categories per Station",
            template="plotly_white",
            barmode='stack',
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Arial"
            ),
            title_font=dict(size=20),
            xaxis_title="Percentage of Trains",
            yaxis_title="Station",
            xaxis_title_font=dict(size=14),
            yaxis_title_font=dict(size=14),
            autosize=True,
            margin=dict(l=40, r=40, t=70, b=40),
            legend_title="Delay Category",
            legend_title_font=dict(size=14),
            legend_font=dict(size=12),
            height=500,  # Set a fixed height
        )
        
        # Get pre-aggregated data for bubble chart
        summary = get_bubble_chart_data()
        
        # Create bubble chart
        fig2 = px.scatter(
            summary, 
            x="pct_delayed", 
            y="avg_delay",
            size="total_trains",
            hover_name="station_name",
            text="station_name",
            labels={
                "pct_delayed": "Delayed Trains (%)",
                "avg_delay": "Average Delay (minutes)",
                "total_trains": "Total Number of Trains"
            },
            title="Station Delay Analysis: Frequency vs Severity",
            color_discrete_sequence=["#4472C4"],
            size_max=60,
        )
        
        fig2.update_traces(
            textposition="top center",
            marker=dict(opacity=0.7, line=dict(width=1, color='DarkSlateGrey')),
        )
        
        fig2.update_layout(
            template="plotly_white",
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Arial"
            ),
            title_font=dict(size=20),
            xaxis_title_font=dict(size=14),
            yaxis_title_font=dict(size=14),
            autosize=True,
            margin=dict(l=40, r=40, t=70, b=40),
            height=500,  # Set a fixed height
        )
        
        # Create available/missing stations notification
        missing_stations = [station for station in TARGET_STATIONS_ORIGINAL if station not in available_stations]
        station_notification = ""
        if missing_stations:
            station_notification = html.Div([
                dbc.Alert(
                    [
                        html.Strong("Note: "), 
                        f"Some target stations are missing from the data. Showing data for {', '.join(available_stations)}.",
                        html.Br(),
                        f"Missing stations: {', '.join(missing_stations)}"
                    ],
                    color="warning",
                    className="mb-4"
                )
            ])
        
        # Create section with both visualizations (now in separate rows)
        return html.Div(
            id="station-section",
            className="dashboard-section",
            children=[
                html.H2("Station Performance Comparison", className="section-title"),
                html.P(
                    "Analysis of delay patterns across key Swiss railway stations.",
                    className="section-description"
                ),
                station_notification,
                # First graph in its own row
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            dbc.CardBody([
                                dcc.Graph(
                                    id='delay-category-graph',
                                    figure=fig1,
                                    config={'displayModeBar': True, 'responsive': True},
                                    className="graph-container"
                                ),
                            ]),
                            className="graph-card"
                        ),
                    ], width=12),
                ], className="mb-4"),
                # Second graph in its own row
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            dbc.CardBody([
                                dcc.Graph(
                                    id='station-bubble-graph',
                                    figure=fig2,
                                    config={'displayModeBar': True, 'responsive': True},
                                    className="graph-container"
                                ),
                            ]),
                            className="graph-card"
                        ),
                    ], width=12),
                ]),
                html.Div(
                    className="insights-container",
                    children=[
                        dbc.Card(
                            dbc.CardBody([
                                html.H4("Key Insights", className="insights-title"),
                                html.Ul([
                                    html.Li("Zürich HB maintains better punctuality despite handling more train traffic"),
                                    html.Li("Luzern experiences the highest percentage of moderate delays (5-15 minutes)"),
                                    html.Li("Genève shows a balanced profile with intermediate performance on most metrics"),
                                ]),
                            ]),
                            className="insights-card"
                        ),
                    ]
                )
            ]
        )
    except Exception as e:
        logger.error(f"Error in create_station_delay_section: {e}")
        
        # Fallback visualization with error message
        return html.Div(
            id="station-section",
            className="dashboard-section",
            children=[
                html.H2("Station Performance Comparison", className="section-title"),
                html.P(
                    "Error creating station performance comparison. Please check data integrity.",
                    className="section-description text-danger"
                ),
                dbc.Alert(
                    f"Error: {str(e)}", 
                    color="danger",
                    dismissable=True
                ),
                html.Div(
                    f"Available stations: {', '.join(df['station_name'].unique())}",
                    className="mt-3"
                )
            ]
        )


def create_time_patterns_section(df: pd.DataFrame) -> html.Div:
    """
    Create the time patterns analysis section.
    
    Args:
        df: Prepared DataFrame
        
    Returns:
        html.Div: Time patterns section component
    """
    try:
        # Get list of available stations in the data
        available_stations = df["station_name"].unique().tolist()
        logger.info(f"Available stations for time patterns section: {available_stations}")
        
        # Get pre-aggregated data
        heatmap_data = get_weekday_heatmap_data()
        
        # Create pivoted dataframe for the heatmap
        pivot_data = heatmap_data.pivot(index="station_name", columns="day_of_week", values="pct_delayed")
        
        # Convert to the format needed for Plotly
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        z_data = []
        y_labels = []
        
        for station in available_stations:
            y_labels.append(station)
            station_data = []
            for day in weekday_order:
                try:
                    val = pivot_data.loc[station, day]
                    station_data.append(val)
                except (KeyError, ValueError):
                    # Handle missing combinations
                    station_data.append(0)
            z_data.append(station_data)
        
        # Create heatmap
        fig1 = go.Figure(data=go.Heatmap(
            z=z_data,
            x=weekday_order,
            y=y_labels,
            colorscale='RdYlGn_r',
            zmin=0,
            zmax=20,
            text=[[f"{val:.1f}%" for val in row] for row in z_data],
            texttemplate="%{text}",
            textfont={"size":10},
            hovertemplate='Station: %{y}<br>Day: %{x}<br>Delayed trains: %{text}<extra></extra>'
        ))
        
        fig1.update_layout(
            title="Percentage of Delayed Trains by Station and Day of Week",
            template="plotly_white",
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Arial"
            ),
            title_font=dict(size=20),
            xaxis_title="Day of Week",
            yaxis_title="Station",
            xaxis_title_font=dict(size=14),
            yaxis_title_font=dict(size=14),
            autosize=True,
            margin=dict(l=40, r=40, t=70, b=40),
            height=500,  # Set a fixed height
        )
        
        # Get pre-aggregated data for hourly line plot
        hourly_data = get_hourly_lineplot_data()
        
        # Create hourly line plot
        fig2 = px.line(
            hourly_data, 
            x="hour", 
            y="pct_delayed", 
            color="station_name",
            markers=True,
            labels={
                "hour": "Hour of Day",
                "pct_delayed": "Delayed Trains (%)",
                "station_name": "Station"
            },
            title="Percentage of Delayed Trains by Hour of the Day",
            color_discrete_sequence=["#4472C4", "#ED7D31", "#A5A5A5"]
        )
        
        fig2.update_layout(
            template="plotly_white",
            hoverlabel=dict(
                bgcolor="white",
                font_size=12,
                font_family="Arial"
            ),
            title_font=dict(size=20),
            xaxis_title_font=dict(size=14),
            yaxis_title_font=dict(size=14),
            autosize=True,
            margin=dict(l=40, r=40, t=70, b=40),
            legend_title="Station",
            legend_title_font=dict(size=14),
            legend_font=dict(size=12),
            xaxis=dict(tickmode='linear', dtick=1),  # Show all hours
            height=500,  # Set a fixed height
        )
        
        # Create available/missing stations notification
        missing_stations = [station for station in TARGET_STATIONS_ORIGINAL if station not in available_stations]
        station_notification = ""
        if missing_stations:
            station_notification = html.Div([
                dbc.Alert(
                    [
                        html.Strong("Note: "), 
                        f"Some target stations are missing from the data. Showing data for {', '.join(available_stations)}.",
                        html.Br(),
                        f"Missing stations: {', '.join(missing_stations)}"
                    ],
                    color="warning",
                    className="mb-4"
                )
            ])
        
        # Create section with both visualizations (now in separate rows)
        return html.Div(
            id="time-section",
            className="dashboard-section",
            children=[
                html.H2("Temporal Delay Patterns", className="section-title"),
                html.P(
                    "Analysis of how train delays vary by day of week and hour of day across different stations.",
                    className="section-description"
                ),
                station_notification,
                # First graph in its own row
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            dbc.CardBody([
                                dcc.Graph(
                                    id='weekday-heatmap-graph',
                                    figure=fig1,
                                    config={'displayModeBar': True, 'responsive': True},
                                    className="graph-container"
                                ),
                            ]),
                            className="graph-card"
                        ),
                    ], width=12),
                ], className="mb-4"),
                # Second graph in its own row
                dbc.Row([
                    dbc.Col([
                        dbc.Card(
                            dbc.CardBody([
                                dcc.Graph(
                                    id='hourly-line-graph',
                                    figure=fig2,
                                    config={'displayModeBar': True, 'responsive': True},
                                    className="graph-container"
                                ),
                            ]),
                            className="graph-card"
                        ),
                    ], width=12),
                ]),
                html.Div(
                    className="insights-container",
                    children=[
                        dbc.Card(
                            dbc.CardBody([
                                html.H4("Key Insights", className="insights-title"),
                                html.Ul([
                                    html.Li("Weekday delays (especially Tuesday) are higher than weekend delays"),
                                    html.Li("Morning and evening rush hours show pronounced delay peaks"),
                                    html.Li("Luzern experiences the most significant rush hour delay spikes"),
                                ]),
                            ]),
                            className="insights-card"
                        ),
                    ]
                )
            ]
        )
    except Exception as e:
        logger.error(f"Error in create_time_patterns_section: {e}")
        
        # Fallback visualization with error message
        return html.Div(
            id="time-section",
            className="dashboard-section",
            children=[
                html.H2("Temporal Delay Patterns", className="section-title"),
                html.P(
                    "Error creating temporal delay patterns visualization. Please check data integrity.",
                    className="section-description text-danger"
                ),
                dbc.Alert(
                    f"Error: {str(e)}", 
                    color="danger",
                    dismissable=True
                ),
                html.Div(
                    f"Available stations: {', '.join(df['station_name'].unique())}",
                    className="mt-3"
                )
            ]
        )