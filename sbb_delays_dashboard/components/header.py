"""
header.py - Dashboard header component

This module provides the header component for the Swiss Train Delays Analysis Dashboard.
"""

import dash_bootstrap_components as dbc
from dash import html


def create_header() -> html.Div:
    """
    Create the dashboard header with title, description, and navigation.
    
    Returns:
        html.Div: Header component
    """
    return html.Div(
        className="dashboard-header",
        children=[
            html.Div(
                className="header-content",
                children=[
                    html.H1(
                        "Swiss Train Delays Analysis Dashboard", 
                        className="header-title"
                    ),
                    html.P(
                        "Interactive visualization of SBB train delays across key Swiss stations",
                        className="header-description"
                    ),
                ]
            ),
            html.Div(
                className="header-nav",
                children=[
                    dbc.Nav(
                        [
                            dbc.NavItem(dbc.NavLink("Overview", href="#overview-section", external_link=True)),
                            dbc.NavItem(dbc.NavLink("Train Categories", href="#category-section", external_link=True)),
                            dbc.NavItem(dbc.NavLink("Stations", href="#station-section", external_link=True)),
                            dbc.NavItem(dbc.NavLink("Time Patterns", href="#time-section", external_link=True)),
                        ],
                        pills=True,
                    ),
                ]
            ),
            html.Div(
                className="header-stats",
                children=[
                    dbc.Row([
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody([
                                    html.H4("3", className="card-title"),
                                    html.P("Key Stations Analyzed", className="card-text"),
                                ]),
                                className="stats-card"
                            ),
                            width={"size": 3, "offset": 0},
                            lg=3, md=6, sm=12
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody([
                                    html.H4("13.8%", className="card-title"),
                                    html.P("Average Delay Rate", className="card-text"),
                                ]),
                                className="stats-card"
                            ),
                            width={"size": 3, "offset": 0},
                            lg=3, md=6, sm=12
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody([
                                    html.H4("2022-2024", className="card-title"),
                                    html.P("Historical Data Range", className="card-text"),
                                ]),
                                className="stats-card"
                            ),
                            width={"size": 3, "offset": 0},
                            lg=3, md=6, sm=12
                        ),
                        dbc.Col(
                            dbc.Card(
                                dbc.CardBody([
                                    html.H4("1.3M+", className="card-title"),
                                    html.P("Daily Passengers", className="card-text"),
                                ]),
                                className="stats-card"
                            ),
                            width={"size": 3, "offset": 0},
                            lg=3, md=6, sm=12
                        ),
                    ]),
                ]
            ),
        ]
    )