# Train Delays Analysis Combining SBB API

This project analyzes train delays in the Swiss public transport system by leveraging real-time and historical data from the Swiss Federal Railways (SBB) APIs and other open transport data sources.

## Project Overview

Swiss public transportation is essential, with SBB carrying over 1.3 million passengers daily. This project aims to:
- Analyze patterns in train delays across different stations and lines
- Identify temporal variations in delays (time of day, day of week)
- Examine how construction and other disruptions impact train service
- Compare delay distributions across different transport modes

## Research Questions

The project addresses the following research questions:

1. Are certain train lines or stations more prone to delays than others?
2. How do delays in Switzerland public transport vary by time of the day or day of the week?
3. How do construction and other causes of disruptions impact train delays?

**Bonus**: What is the distribution of delays in Swiss public transport across transport modes (type of trains, tram, bus, ship etc.)?

## Data Sources

The project utilizes the following data sources:

1. **Swiss Transport API (transport.opendata.ch)**:
   - Station Board API (`/v1/stationboard`): For departures and arrivals information
   - Connections API (`/v1/connections`): For route-specific delay tracking
   - Locations API (`/v1/locations`): For station information

2. **Historical data sources**:
   - Station Board archive: For archived departure/arrival information
   - Delay Information API v2.1: For comprehensive delay information

## Project Structure

```
CIP_FS25_106/
│
├── main.py
├── data_collection/
│   ├── __init__.py
│   ├── api_client.py
│   ├── station_board.py
    ├── historical_data.py
│   └── connections.py
│
├── data_processing/
│   ├── __init__.py
│   ├── cleaning.py
│   └── integration.py
│
├── analysis/
│   ├── __init__.py
│   ├── station_analysis.py
│   ├── temporal_analysis.py
│   └── disruption_analysis.py
│
├── visualization/
│   ├── __init__.py
│   ├── delay_maps.py
│   └── time_series.py
│
├── utils/
│   ├── __init__.py
│   └── helpers.py
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
├── requirements.txt
└── README.md
```

## Methodology

Data preparation includes retrieving all relevant data, cleaning, pre-filtering, generating KeyIDs, and joining datasets iteratively. The analysis will utilize Python's Matplotlib for visualization and may incorporate an interactive dashboard using the shiny library for enhanced exploration. Quality control steps ensure consistency throughout the process.

## Setup and Installation

1. Create a virtual environment:
```
python -m venv venv
```

2. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - macOS/Linux: `source venv/bin/activate`

3. Install the requirements:
```
pip install -r requirements.txt
```

## Usage

### Running the Data Collection

To collect data with default settings:
```
python main.py
```

With specific options:
```
python main.py --stations zurich,bern,basel --date-range 2025-01-01:2025-03-01 --transport-types IR,IC,S
```

Options:
- `--stations`: Which stations to analyze (comma-separated)
- `--date-range`: Date range for historical data
- `--transport-types`: Types of transportation to include
- `--max-requests`: Maximum number of API requests to make (to stay within rate limits)

### Data Processing and Analysis

The collected data will be automatically processed and saved in the `data` directory. Analysis scripts can be run separately to generate insights.

## Risks, Limitations, and Uncertainties

| Risk | Description | Mitigation Strategy |
|------|-------------|---------------------|
| Limited historical data in API v1 | The Transport API focuses on current and future schedules, with limited historical data access. Data before 01.01.2025 not available. | - Other data sources will been consider which can be retrieved in archived websites as .zip file |
| API rate limiting (v1) | The Transport API imposes request limits that restrict the data collection. The number of requests per day is limited to 1000 route queries and 10080 departure/arrival tables. | - Implement appropriate request throttling (a technique used to control the rate at which API requests are made to a server)<br>- Spread data collection over time<br>- Store collected data efficiently to minimize redundant requests. |
| Inconsistent historical data formats | Historical data uses different formatting (csv), identifiers, and schema compared to current API (json) data, making integration challenging. | - Develop robust data transformation pipelines with clear mapping between old and new formats<br>- Implement data validation checks to ensure consistency<br>- Focus analysis on stations/routes with more complete data. |
| Historical vs. current station identifiers | Station and train codes, names, or identifiers may have changed over time. | - Create a master reference table mapping historical station and train identifiers to current ones<br>- Document all station and train identification changes over the analysis period |

Additional challenges include:
- Inconsistency in train numbers across different datasets
- Large amount of data requiring significant computational resources
- Uncertainty with the new API version launching on March 17th

## Requirements

- Python 3.8+
- Pandas
- NumPy
- Requests
- Matplotlib
- Seaborn
- Jupyter (for notebooks)

## Contributors

- Roger ([@rogerjeasy](https://github.com/rogerjeasy))
- Sahra ([@sahrabaettig](https://github.com/sahrabaettig))
- Mika ([@mikachulab](https://github.com/mikachulab))

## Sources

Baumgartner, S. (2024, March 11). SBB schreibt wieder schwarze Zahlen dank Rekordzahl an Reisenden.