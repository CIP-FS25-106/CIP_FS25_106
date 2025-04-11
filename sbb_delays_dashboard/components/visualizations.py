"""
visualizations.py - Module for creating visualizations in the SBB Train Delays Dashboard.
This module implements all the visualizations from historical_data_analysis.py
in a Dash/Plotly compatible format.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from dash import html, dcc
import dash_bootstrap_components as dbc

# Constants
DELAY_THRESHOLD = 2  # Minutes threshold for considering a train delayed

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

def create_overview_delay_plot(df):
    """
    Create an overview stripplot of delay distribution.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        # Create a Plotly figure
        fig = px.strip(df, x="delay", opacity=0.5)
        
        # Update layout
        fig.update_layout(
            title="Overview of DELAY",
            xaxis_title="Delay [min]",
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        return dcc.Graph(figure=fig, id="overview-delay-plot")
    except Exception as e:
        print(f"Error creating overview plot: {e}")
        return html.Div("Error creating overview delay plot")


def create_train_category_chart(df):
    """
    Create a barplot showing average delay per train category.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        # Group and sort
        if 'train_category' not in df.columns or 'delay' not in df.columns:
            return html.Div("Required columns missing for train category chart")
        
        avg_by_category = df.groupby("train_category")["delay"].mean().reset_index()
        avg_by_category = avg_by_category.sort_values(by="delay", ascending=False)
        
        # Create the figure
        fig = px.bar(
            avg_by_category, 
            x="train_category", 
            y="delay",
            color_discrete_sequence=[SBB_COLORS['primary']]
        )
        
        # Add value labels on top of bars
        fig.update_traces(
            texttemplate='%{y:.2f}',
            textposition='outside'
        )
        
        # Update layout
        fig.update_layout(
            title="Average Delay per Train Category",
            xaxis_title="Train Category",
            yaxis_title="Average Delay [min]",
            yaxis=dict(range=[0, 26]),
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        # Rotate x-axis labels
        fig.update_xaxes(tickangle=45)
        
        return dcc.Graph(figure=fig, id="train-category-chart")
    except Exception as e:
        print(f"Error creating train category chart: {e}")
        return html.Div("Error creating train category chart")


def create_delay_distribution_chart(df):
    """
    Create a pie chart showing the distribution of trains across delay categories.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        if 'DELAY_CAT' not in df.columns:
            # If the DELAY_CAT column is not present, create it
            if 'delay' in df.columns:
                conditions = [
                    (df['delay'] <= DELAY_THRESHOLD),
                    (df['delay'] > DELAY_THRESHOLD) & (df['delay'] <= 5),
                    (df['delay'] > 5) & (df['delay'] <= 15),
                    (df['delay'] > 15)
                ]
                choices = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes']
                df['DELAY_CAT'] = np.select(conditions, choices, default='Cancelled')
            else:
                return html.Div("Required columns missing for delay distribution chart")
        
        # Count the occurrences of each delay category
        delay_counts = df['DELAY_CAT'].value_counts().reset_index()
        delay_counts.columns = ['category', 'count']
        
        # Define the colors for each category
        colors = {
            "On time": SBB_COLORS['on_time'],
            "2 to 5minutes": SBB_COLORS['slight_delay'],
            "5 to 15minutes": SBB_COLORS['medium_delay'],
            "more than 15minutes": SBB_COLORS['severe_delay'],
            "Cancelled": SBB_COLORS['cancelled']
        }
        
        # Define category order
        category_order = ["On time", "2 to 5minutes", "5 to 15minutes", "more than 15minutes", "Cancelled"]
        delay_counts['category'] = pd.Categorical(delay_counts['category'], categories=category_order, ordered=True)
        delay_counts = delay_counts.sort_values('category')
        
        # Assign colors based on category
        color_values = [colors.get(cat, "#999999") for cat in delay_counts['category']]
        
        # Create the pie chart
        fig = go.Figure(data=[go.Pie(
            labels=delay_counts['category'],
            values=delay_counts['count'],
            hole=.4,
            marker_colors=color_values
        )])
        
        # Update layout
        fig.update_layout(
            title="Delay Categories Distribution",
            height=300,
            margin=dict(l=20, r=20, t=50, b=20),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        return dcc.Graph(figure=fig, id="delay-distribution-chart")
    except Exception as e:
        print(f"Error creating delay distribution chart: {e}")
        return html.Div("Error creating delay distribution chart")


def create_station_comparison_chart(df):
    """
    Create a horizontal barplot showing percentage of trains in each delay category per station.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        if 'station_name' not in df.columns:
            return html.Div("Required columns missing for station comparison chart")
            
        # Ensure DELAY_CAT exists
        if 'DELAY_CAT' not in df.columns:
            if 'delay' in df.columns:
                conditions = [
                    (df['delay'] <= DELAY_THRESHOLD),
                    (df['delay'] > DELAY_THRESHOLD) & (df['delay'] <= 5),
                    (df['delay'] > 5) & (df['delay'] <= 15),
                    (df['delay'] > 15)
                ]
                choices = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes']
                df['DELAY_CAT'] = np.select(conditions, choices, default='Cancelled')
            else:
                return html.Div("Required columns missing for station comparison chart")
        
        # Get unique stations
        stations = df['station_name'].unique().tolist()
        
        # Count number of trains in each delay category
        counts = df.groupby(["station_name", "DELAY_CAT"]).size().reset_index(name="count")
        
        # Calculate percentages
        totals = counts.groupby("station_name")["count"].sum().reset_index(name="total")
        counts = counts.merge(totals, on="station_name")
        counts["percentage"] = 100 * counts["count"] / counts["total"]
        
        # Define the categories order and colors
        categories = ["On time", "2 to 5minutes", "5 to 15minutes", "more than 15minutes", "Cancelled"]
        colors = {
            "On time": SBB_COLORS['on_time'],
            "2 to 5minutes": SBB_COLORS['slight_delay'],
            "5 to 15minutes": SBB_COLORS['medium_delay'],
            "more than 15minutes": SBB_COLORS['severe_delay'],
            "Cancelled": SBB_COLORS['cancelled']
        }
        
        # Create figure
        fig = go.Figure()
        
        # Prepare data for stacked bars
        for category in categories:
            # Filter for current category
            category_data = counts[counts["DELAY_CAT"] == category]
            
            # Create a trace for each category
            fig.add_trace(go.Bar(
                y=category_data["station_name"],
                x=category_data["percentage"],
                name=category,
                orientation='h',
                marker=dict(color=colors.get(category, "#999999")),
                text=category_data["percentage"].apply(lambda x: f"{x:.1f}%" if x > 5 else ""),
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(color="white")
            ))
        
        # Set up the layout for a stacked bar chart
        fig.update_layout(
            barmode='stack',
            title="Train Delay Categories per Station",
            xaxis_title="Trains [%]",
            yaxis_title="Station",
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        return dcc.Graph(figure=fig, id="station-comparison-chart")
    except Exception as e:
        print(f"Error creating station comparison chart: {e}")
        return html.Div("Error creating station comparison chart")


def create_bubble_chart(df):
    """
    Create a bubble chart showing delay frequency vs severity for each station.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        if 'station_name' not in df.columns or 'delay' not in df.columns:
            return html.Div("Required columns missing for bubble chart")
        
        # Get unique stations
        stations = df['station_name'].unique().tolist()
        
        # Mean, total and sum of delayed trains more than DELAY_THRESHOLD minutes by station
        summary = df.groupby("station_name").agg(
            avg_delay=("delay", "mean"),
            total_trains=("delay", "count"),
            delayed_trains=("delay", lambda x: (x > DELAY_THRESHOLD).sum())
        ).reset_index()
        
        # Calculate percentage of delayed trains
        summary["pct_delayed"] = 100 * summary["delayed_trains"] / summary["total_trains"]
        
        # Create bubble chart
        fig = px.scatter(
            summary, 
            x="pct_delayed",
            y="avg_delay",
            size="total_trains",
            size_max=50,
            text="station_name",
            opacity=0.7,
            color_discrete_sequence=[SBB_COLORS['primary']]
        )
        
        # Update traces for better appearance
        fig.update_traces(
            marker=dict(
                line=dict(width=1, color='black')
            ),
            textposition="top right"
        )
        
        # Update layout
        fig.update_layout(
            title="Station Delay Analysis: Frequency vs Severity",
            xaxis_title="Delayed Trains [%]",
            yaxis_title="Average Delay [min]",
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        # Add grid
        fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        
        return dcc.Graph(figure=fig, id="station-bubble-chart")
    except Exception as e:
        print(f"Error creating bubble chart: {e}")
        return html.Div("Error creating bubble chart")


def create_day_of_week_chart(df):
    """
    Create a heatmap showing percentage of delayed trains by station and day of week.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        # Ensure required columns exist
        if 'station_name' not in df.columns or 'ride_day' not in df.columns:
            # Check if there's a date column that could be used instead
            if 'arrival_planned' in df.columns:
                df['ride_day'] = pd.to_datetime(df['arrival_planned']).dt.date
            else:
                return html.Div("Required columns missing for day of week chart")
        
        # Convert ride_day to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['ride_day']):
            df['ride_day'] = pd.to_datetime(df['ride_day'])
        
        # Extract weekday name
        df["day_of_week"] = df["ride_day"].dt.day_name()
        
        # Order weekdays
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        df["day_of_week"] = pd.Categorical(df["day_of_week"], categories=weekday_order, ordered=True)
        
        # Define what is considered a delay
        df["is_delayed"] = df["delay"] > DELAY_THRESHOLD
        
        # Group by station and weekday
        heatmap_data = df.groupby(["station_name", "day_of_week"]).agg(
            total=("delay", "count"),
            delayed=("is_delayed", "sum")
        ).reset_index()
        
        heatmap_data["pct_delayed"] = 100 * heatmap_data["delayed"] / heatmap_data["total"]
        
        # Pivot for heatmap
        pivot = heatmap_data.pivot(index="station_name", columns="day_of_week", values="pct_delayed")
        
        # Create heatmap
        fig = px.imshow(
            pivot,
            color_continuous_scale='RdYlGn_r',
            labels=dict(x="Day of Week", y="Station", color="Delayed [%]"),
            zmin=0,
            zmax=20,
            text_auto='.1f'
        )
        
        # Update layout
        fig.update_layout(
            title=f"Percentage of Delayed Trains (>{DELAY_THRESHOLD}min) by Station and Day of Week",
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            coloraxis_colorbar=dict(title="Delayed [%]"),
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        return dcc.Graph(figure=fig, id="day-of-week-chart")
    except Exception as e:
        print(f"Error creating day of week chart: {e}")
        return html.Div("Error creating day of week chart")


def create_time_of_day_chart(df):
    """
    Create a line plot showing percentage of delayed trains by hour of the day for each station.
    
    Args:
        df: Prepared DataFrame
    """
    try:
        # Check for required columns
        if 'station_name' not in df.columns:
            return html.Div("Required columns missing for time of day chart")
        
        # Convert arrival_planned column if it exists, otherwise look for alternatives
        if 'scheduled_arrival' in df.columns:
            df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"], errors="coerce")
            df["hour"] = df["scheduled_arrival"].dt.hour
        elif 'arrival_planned' in df.columns:
            df["scheduled_arrival"] = pd.to_datetime(df["arrival_planned"], errors="coerce")
            df["hour"] = df["scheduled_arrival"].dt.hour
        else:
            return html.Div("Required columns missing for time of day chart")
        
        # Define what is considered a delay
        df["is_delayed"] = df["delay"] > DELAY_THRESHOLD
        
        # Group by hour and station
        delay_by_hour = df.groupby(["hour", "station_name"]).agg(
            total=("delay", "count"),
            delayed=("is_delayed", "sum")
        ).reset_index()
        
        # Calculate percentage
        delay_by_hour["pct_delayed"] = 100 * delay_by_hour["delayed"] / delay_by_hour["total"]
        
        # Create line plot
        fig = px.line(
            delay_by_hour, 
            x="hour", 
            y="pct_delayed", 
            color="station_name",
            markers=True,
            line_shape='linear',
            labels={
                "hour": "Hour of the Day",
                "pct_delayed": "Delayed Trains [%]",
                "station_name": "Station"
            }
        )
        
        # Update layout
        fig.update_layout(
            title=f"Percentage of Delayed Trains (>{DELAY_THRESHOLD} min) by Hour of the Day",
            xaxis_title="Hour of the Day",
            yaxis_title="Delayed Trains [%]",
            height=300,
            margin=dict(l=40, r=40, t=50, b=40),
            xaxis=dict(tickmode='linear', tick0=0, dtick=1),
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            plot_bgcolor=SBB_COLORS['light_bg'],
            paper_bgcolor='white',
            font=dict(color=SBB_COLORS['text'])
        )
        
        # Add grid
        fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
        
        return dcc.Graph(figure=fig, id="time-of-day-chart")
    except Exception as e:
        print(f"Error creating time of day chart: {e}")
        return html.Div("Error creating time of day chart")