import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np
from dash import dcc, html
import dash_bootstrap_components as dbc

def create_delay_distribution_chart(df):
    """
    Create a chart showing the distribution of delay categories
    """
    if df.empty or 'delay_category' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate the distribution
    delay_dist = df['delay_category'].value_counts().reset_index()
    delay_dist.columns = ['Category', 'Count']
    delay_dist['Percentage'] = (delay_dist['Count'] / delay_dist['Count'].sum() * 100).round(1)
    
    # Define a custom order for delay categories
    category_order = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes', 'Cancelled']
    
    # Ensure all categories are present
    all_categories = pd.DataFrame({'Category': category_order})
    delay_dist = pd.merge(all_categories, delay_dist, on='Category', how='left').fillna(0)
    
    # Sort by the predefined order
    delay_dist['Category_order'] = delay_dist['Category'].map({cat: i for i, cat in enumerate(category_order)})
    delay_dist = delay_dist.sort_values('Category_order')
    
    # Create the figure using go.Figure instead of px.bar
    fig = go.Figure()
    
    # Custom colors for delay categories
    colors = {
        'On time': '#66c2a5',
        '2 to 5minutes': '#fc8d62',
        '5 to 15minutes': '#8da0cb',
        'more than 15minutes': '#e78ac3',
        'Cancelled': '#a6d854'
    }
    
    # Add bars
    for category in category_order:
        if category in delay_dist['Category'].values:
            category_data = delay_dist[delay_dist['Category'] == category]
            fig.add_trace(go.Bar(
                x=[category],
                y=[category_data['Percentage'].values[0]],
                name=category,
                text=[f"{category_data['Percentage'].values[0]:.1f}%"],
                textposition='outside',
                marker_color=colors.get(category, '#1f77b4'),
                hoverinfo='text',
                hovertext=[f"{category}: {category_data['Percentage'].values[0]:.1f}%"]
            ))
    
    # Update layout
    fig.update_layout(
        title="Distribution of Train Delays",
        xaxis_title="Delay Category",
        yaxis_title="Percentage (%)",
        legend_title="Delay Category",
        height=400,
        margin=dict(l=40, r=40, t=50, b=40),
        plot_bgcolor='rgba(245, 246, 249, 1)',
        xaxis=dict(
            categoryorder='array',
            categoryarray=category_order
        )
    )
    
    return dcc.Graph(figure=fig, id="delay-distribution-chart")

def create_train_category_chart(df):
    """
    Create a chart showing the average delay by train category
    """
    if df.empty or 'train_category' not in df.columns or 'delay' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate average delay by train category
    category_delay = df.groupby('train_category')['delay'].mean().reset_index()
    category_delay.columns = ['Train Category', 'Average Delay']
    
    # Sort by average delay in descending order
    category_delay = category_delay.sort_values('Average Delay', ascending=False)
    
    # Create the figure using go.Figure instead of px.bar
    fig = go.Figure()
    
    # Add the bar trace
    fig.add_trace(go.Bar(
        x=category_delay['Train Category'],
        y=category_delay['Average Delay'],
        text=category_delay['Average Delay'].round(1),
        textposition='outside',
        marker_color='rgba(50, 171, 96, 0.7)',
        hoverinfo='x+y'
    ))
    
    # Update layout
    fig.update_layout(
        title="Average Delay by Train Category",
        xaxis_title="Train Category",
        yaxis_title="Average Delay (minutes)",
        height=400,
        margin=dict(l=40, r=40, t=50, b=40),
        plot_bgcolor='rgba(245, 246, 249, 1)',
        showlegend=False
    )
    
    return dcc.Graph(figure=fig, id="train-category-chart")

def create_station_comparison_chart(df):
    """
    Create a chart comparing delay categories across stations
    """
    if df.empty or 'station_name' not in df.columns or 'delay_category' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate the distribution of delay categories by station
    station_delay = df.groupby(['station_name', 'delay_category']).size().reset_index(name='count')
    
    # Calculate percentages within each station
    station_totals = station_delay.groupby('station_name')['count'].sum().reset_index()
    station_delay = pd.merge(station_delay, station_totals, on='station_name')
    station_delay['percentage'] = (station_delay['count_x'] / station_delay['count_y'] * 100).round(1)
    
    # Define category order
    category_order = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes', 'Cancelled']
    
    # Custom colors for delay categories
    colors = {
        'On time': '#66c2a5',
        '2 to 5minutes': '#fc8d62',
        '5 to 15minutes': '#8da0cb',
        'more than 15minutes': '#e78ac3',
        'Cancelled': '#a6d854'
    }
    
    # Create the figure using go.Figure
    fig = go.Figure()
    
    # Get all stations
    stations = station_delay['station_name'].unique()
    
    # For each category, add a horizontal bar for each station
    for category in category_order:
        category_data = station_delay[station_delay['delay_category'] == category]
        
        # Create a dict mapping station to percentage
        station_to_pct = dict(zip(category_data['station_name'], category_data['percentage']))
        
        # Create y positions and percentages for all stations
        y_pos = list(range(len(stations)))
        percentages = [station_to_pct.get(station, 0) for station in stations]
        
        fig.add_trace(go.Bar(
            y=stations,
            x=percentages,
            name=category,
            orientation='h',
            marker_color=colors.get(category, '#1f77b4'),
            hovertemplate='%{y}: %{x:.1f}%<extra>' + category + '</extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title="Delay Categories per Station",
        xaxis_title="Percentage (%)",
        yaxis_title="Station",
        legend_title="Delay Category",
        height=300 + 50 * len(stations),
        margin=dict(l=40, r=40, t=50, b=40),
        barmode='stack',
        plot_bgcolor='rgba(245, 246, 249, 1)'
    )
    
    return dcc.Graph(figure=fig, id="station-comparison-chart")

def create_time_of_day_chart(df):
    """
    Create a chart showing delay percentages by hour of the day
    """
    if df.empty or 'hour' not in df.columns or 'delay' not in df.columns or 'station_name' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate percentage of delayed trains by hour and station
    hourly_data = df.groupby(['station_name', 'hour']).apply(
        lambda x: pd.Series({
            'percent_delayed': (x['delay'] > 2).mean() * 100,
            'total': len(x)
        })
    ).reset_index()
    
    # Create the figure using go.Figure instead of px.line
    fig = go.Figure()
    
    # Add lines for each station
    for station in hourly_data['station_name'].unique():
        station_data = hourly_data[hourly_data['station_name'] == station]
        
        fig.add_trace(go.Scatter(
            x=station_data['hour'],
            y=station_data['percent_delayed'],
            mode='lines+markers',
            name=station,
            hovertemplate='Hour: %{x}<br>Delayed: %{y:.1f}%<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title="Percentage of Delayed Trains by Hour of Day",
        xaxis=dict(
            title="Hour of Day",
            tickmode='array',
            tickvals=list(range(24)),
            ticktext=[f'{h:02d}:00' for h in range(24)]
        ),
        yaxis=dict(
            title="Delayed Trains (%)"
        ),
        legend_title="Station",
        height=450,
        margin=dict(l=40, r=40, t=50, b=40),
        hovermode="closest",
        plot_bgcolor='rgba(245, 246, 249, 1)'
    )
    
    return dcc.Graph(figure=fig, id="time-of-day-chart")

def create_day_of_week_chart(df):
    """
    Create a chart showing delay percentages by day of the week
    """
    if df.empty or 'day_of_week' not in df.columns or 'day_name' not in df.columns or 'delay' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate percentage of delayed trains by day and station
    daily_data = df.groupby(['station_name', 'day_of_week', 'day_name']).apply(
        lambda x: pd.Series({
            'percent_delayed': (x['delay'] > 2).mean() * 100,
            'total': len(x)
        })
    ).reset_index()
    
    # Create a pivot table for better data organization
    stations = daily_data['station_name'].unique()
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    # Create a pivot table for better data organization
    pivot_data = daily_data.pivot_table(
        index='station_name', 
        columns='day_name', 
        values='percent_delayed',
        aggfunc='mean'
    ).reindex(columns=day_order)
    
    # Create the heatmap manually
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=day_order,
        y=pivot_data.index,
        colorscale='Viridis',
        hoverongaps=False,
        hovertemplate='Station: %{y}<br>Day: %{x}<br>Delayed Trains: %{z:.1f}%<extra></extra>'
    ))
    
    # Add text annotations for each cell
    annotations = []
    for i, station in enumerate(pivot_data.index):
        for j, day in enumerate(day_order):
            if day in pivot_data.columns and not pd.isna(pivot_data.at[station, day]):
                value = pivot_data.at[station, day]
                annotations.append(dict(
                    x=day,
                    y=station,
                    text=f"{value:.1f}%",
                    showarrow=False,
                    font=dict(color='white' if value > 15 else 'black')
                ))
    
    # Update layout with annotations
    fig.update_layout(
        title="Percentage of Delayed Trains by Day of Week",
        xaxis_title="Day of Week",
        yaxis_title="Station",
        height=300 + 50 * len(stations),  # Adjust height based on station count
        margin=dict(l=40, r=40, t=50, b=40),
        annotations=annotations
    )
    
    return dcc.Graph(figure=fig, id="day-of-week-chart")

def create_bubble_chart(df):
    """
    Create a bubble chart showing frequency vs severity of delays by station
    """
    if df.empty or 'station_name' not in df.columns or 'delay' not in df.columns:
        return dcc.Graph(
            figure=go.Figure().add_annotation(
                text="No data available",
                showarrow=False,
                font=dict(size=20)
            )
        )
    
    # Calculate metrics by station
    station_metrics = df.groupby('station_name').apply(
        lambda x: pd.Series({
            'avg_delay': x['delay'].mean(),
            'pct_delayed': (x['delay'] > 2).mean() * 100,
            'total_trains': len(x)
        })
    ).reset_index()
    
    # Create the figure using go.Figure
    fig = go.Figure()
    
    # Add scatter trace for each station
    for i, row in station_metrics.iterrows():
        fig.add_trace(go.Scatter(
            x=[row['pct_delayed']],
            y=[row['avg_delay']],
            mode='markers',
            marker=dict(
                size=row['total_trains'] / 100 if row['total_trains'] > 100 else 20,  # Scale bubble size
                sizemode='area',
                sizeref=2.*max(station_metrics['total_trains'])/(100**2),
                sizemin=10
            ),
            name=row['station_name'],
            text=row['station_name'],
            hovertemplate='Station: %{text}<br>Delayed Trains: %{x:.1f}%<br>Avg Delay: %{y:.1f} min<br>Total Trains: %{marker.size}<extra></extra>'
        ))
    
    # Update layout
    fig.update_layout(
        title="Station Delay Analysis: Frequency vs Severity",
        xaxis_title="Delayed Trains (%)",
        yaxis_title="Average Delay (minutes)",
        legend_title="Station",
        height=450,
        margin=dict(l=40, r=40, t=50, b=40),
        plot_bgcolor='rgba(245, 246, 249, 1)',
        hovermode="closest"
    )
    
    return dcc.Graph(figure=fig, id="station-bubble-chart")