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

## Notes for Development

- The system currently focuses on three main stations: Lucern, Zürich HB, and Geneva
- The code is structured to easily add more stations as needed
- Rate limiting is implemented to respect the Swiss Transport API limits
- Data is automatically organized by month for easier processing

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
│   ├── historical_data.py
│   └── connections.py
├── data_processing/
│   ├── __init__.py
│   ├── cleaning.py
│   └── integration.py
├── analysis/
│   ├── __init__.py
│   ├── station_analysis.py
│   ├── temporal_analysis.py
│   └── disruption_analysis.py
├── visualization/
│   ├── __init__.py
│   ├── delay_maps.py
│   └── time_series.py
├── utils/
│   ├── __init__.py
│   └── helpers.py
├── data/
│   ├── raw/
│   └── processed/
├── notebooks/
└── requirements.txt
```

## Methodology

Data preparation includes retrieving all relevant data, cleaning, pre-filtering, generating KeyIDs, and joining datasets iteratively. The analysis will utilize Python's Matplotlib for visualization and may incorporate an interactive dashboard using the shiny library for enhanced exploration. Quality control steps ensure consistency throughout the process.

## Setup and Installation

1. Clone the repository
2. Install required packages:

```bash
pip install -r requirements.txt
```

## Usage

### Collecting Data for a Specific Month

```bash
python main.py --year 2025 --month 1
```

### Collecting Data for Multiple Months

```bash
python main.py --year 2025 --month 6 --all-months
```

This will collect data for January through June 2025.

### Data Storage Structure

Data is organized by month in the following structure:

```
data/
├── raw/
│   ├── 2025-01/
│   │   ├── Luzern_2025-01-01.csv
│   │   ├── Zürich_HB_2025-01-01.csv
│   │   ├── connection_Zürich_HB_to_Luzern_2025-01-01.csv
│   │   └── delay_info_2025-01-01.csv
│   └── 2025-02/
│       └── ...
└── processed/
    └── ...
```

## Key Features Collected

### Station Board Data
- Station ID and name
- Train category and number
- Scheduled and actual departure/arrival times
- Delay information
- Platform
- Destination/origin

### Connection Data
- Origin and destination stations
- Departure and arrival times
- Duration and delays
- Number of transfers
- Train categories used
- Capacity information

### Historical Data
- Archived station board data
- Delay information and causes
- Disruption details

## Data Collection Modules

### api_client.py

Core client for interacting with the Swiss Transport API with rate limiting, caching, and error handling features.

- `get_station_info()`: Retrieve information about train stations
- `get_station_board()`: Get departure/arrival information for a station
- `get_connections()`: Get connections between two stations

### station_board.py

Handles collecting and processing station board data (departures and arrivals).

- `collect_station_data()`: Collect departure/arrival data for a single station
- `collect_data_for_period()`: Collect data for a specific date range
- `collect_monthly_data()`: Collect data for all stations for an entire month

### connections.py

Manages collecting and processing connection data between stations.

- `collect_connection_data()`: Collect connections between two stations
- `collect_daily_connections()`: Collect all connection pairs for a specific day
- `collect_monthly_connections()`: Collect connections for an entire month

## Connection Pairs

- Zürich HB to Luzern,
- Zürich HB to Genève,
- Luzern to Genève

## Time Slots
time_slots = [06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00]

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

## Dependencies

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