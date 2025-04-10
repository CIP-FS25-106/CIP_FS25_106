import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

def create_header():
    """
    Creates the page header with title and navigation
    """
    return html.Div(
        className="header",
        children=[
            html.Div(
                className="header-content",
                children=[
                    html.H1("SBB Train Delays Analysis Dashboard", className="header-title"),
                    html.P(
                        "Interactive visualization of train delays in the Swiss public transport system",
                        className="header-description"
                    ),
                ]
            ),
            html.Div(
                className="header-nav",
                children=[
                    html.Div(
                        style={"display": "flex", "justifyContent": "center", "width": "100%"},
                        children=[
                            dbc.Nav(
                                [
                                    dbc.NavItem(dbc.NavLink("Overview", id="nav-overview")),
                                    dbc.NavItem(dbc.NavLink("Stations", id="nav-stations")),
                                    dbc.NavItem(dbc.NavLink("Temporal Patterns", id="nav-temporal")),
                                    dbc.NavItem(dbc.NavLink("Train Categories", id="nav-categories")),
                                ],
                                pills=True,
                                className="nav-pills-centered",
                            ),
                        ]
                    )
                ]
            )
        ]
    )

def create_filter_bar(available_stations, available_categories):
    """
    Creates a filter bar for interactive data filtering
    """
    return html.Div(
        className="filter-bar",
        children=[
            html.H4("Filter Data", className="filter-title"),
            html.Div(
                className="filter-row",
                children=[
                    html.Div(
                        className="filter-group",
                        children=[
                            html.Label("Stations:"),
                            dcc.Dropdown(
                                id="station-filter",
                                options=[{"label": station, "value": station} for station in available_stations],
                                value=available_stations,  # Default select all
                                multi=True,
                                placeholder="Select stations"
                            )
                        ],
                        style={"flex": 1}
                    ),
                    html.Div(
                        className="filter-group",
                        children=[
                            html.Label("Train Categories:"),
                            dcc.Dropdown(
                                id="category-filter",
                                options=[{"label": cat, "value": cat} for cat in available_categories],
                                value=available_categories[:5] if len(available_categories) > 5 else available_categories,  # Default select top 5
                                multi=True,
                                placeholder="Select train categories"
                            )
                        ],
                        style={"flex": 1}
                    ),
                    html.Div(
                        className="filter-group",
                        children=[
                            html.Label("Date Range:"),
                            dcc.DatePickerRange(
                                id="date-range-filter",
                                start_date_placeholder_text="Start Date",
                                end_date_placeholder_text="End Date",
                                calendar_orientation="horizontal"
                            )
                        ],
                        style={"flex": 1}
                    )
                ]
            ),
            html.Div(
                className="filter-actions",
                children=[
                    html.Button("Reset Filters", id="reset-button", className="filter-button reset-button", style={"marginRight": "10px"}),
                    html.Button("Apply Filters", id="filter-button", className="filter-button")
                ]
            )
        ]
    )

def create_kpi_cards():
    """
    Creates KPI cards for high-level metrics
    """
    return html.Div(
        className="kpi-cards",
        children=[
            dbc.Card(
                dbc.CardBody([
                    html.H4("Total Trains", className="kpi-title"),
                    html.H2(id="total-trains-value", className="kpi-value"),
                ]),
                className="kpi-card"
            ),
            dbc.Card(
                dbc.CardBody([
                    html.H4("Average Delay", className="kpi-title"),
                    html.H2(id="avg-delay-value", className="kpi-value"),
                    html.P("minutes", className="kpi-unit")
                ]),
                className="kpi-card"
            ),
            dbc.Card(
                dbc.CardBody([
                    html.H4("On-Time Rate", className="kpi-title"),
                    html.H2(id="on-time-rate-value", className="kpi-value"),
                    html.P("%", className="kpi-unit")
                ]),
                className="kpi-card"
            ),
            dbc.Card(
                dbc.CardBody([
                    html.H4("Delayed Trains", className="kpi-title"),
                    html.H2(id="delayed-rate-value", className="kpi-value"),
                    html.P("%", className="kpi-unit")
                ]),
                className="kpi-card"
            )
        ]
    )