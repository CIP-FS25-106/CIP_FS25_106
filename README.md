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

**Future work**: What is the distribution of delays in Swiss public transport across transport modes (type of trains, tram, bus, ship etc.)?

## Project Structure

```
CIP_FS25_106/
│
├── main.py
├── data_collection/
│   ├── __init__.py
│   ├── api_client.py
│   ├── station_board.py
│   ├── historical_data/
│   │   ├── __init__.py
│   │   ├──   zip_scrapper.py  
│   │   ├── historical_df_intermediate.py
│   │   └── zip_extract_delete.py
│   └── connections.py
├── data_processing/
│   ├── __init__.py
│   ├── historical/
|   │   ├── __init__.py
|   │   └── historical_transform.py
├── analysis/
│   ├── __init__.py
│   ├── historical/
│   │   ├── __init__.py
│   │   ├── historical_data_analysis.py  
│   └── delay_analysis.py
├── data/
│   ├── raw/
│   │   ├── 2025-01/
│   │   │   ├── Luzern_2025-01-01.csv
│   │   │   ├── Zürich_HB_2025-01-01.csv
│   │   │   ├── connection_Zürich_HB_to_Luzern_2025-01-01.csv
│   │   │   └── delay_info_2025-01-01.csv
│   │   └── 2025-02/
│   │       └── ...
│   └── historical/
│       ├── downloads_2022/
│       ├── downloads_2023/
│       ├── downloads_2024/
│       └── processed/
│           ├── intermediate_filtered_data.csv
│           └── historical_transformed.csv
├── drivers/
│   └── chromedriver.exe  # For Selenium-based web scraping
├── notebooks/
└── requirements.txt
```

## Data Pipeline

The project implements a comprehensive data pipeline for historical data processing:

### 1. Historical Data Collection (`zip_scrapper.py`)

Downloads ZIP files from the [OpenTransportData Archive](https://archive.opentransportdata.swiss/actual_data_archive.htm) for the years 2022-2024.

- Uses Selenium for web scraping
- Targets specific years (2022, 2023, 2024)
- Saves ZIP files to `data/historical/downloads_YYYY` directories

### 2. Archive Extraction (`zip_extract_delete.py`)

Extracts all downloaded ZIP files and cleans up after extraction.

- Processes all archives by year
- Uses `patoolib` for handling various archive formats
- Deletes ZIP files after successful extraction to save space

### 3. Data Filtering (`historical_df_intermediate.py`)

Processes and filters the raw CSV files to create an intermediate dataset.

- Selects relevant stations (Zürich HB, Luzern, Genève)
- Filters for SBB train services only
- Keeps only essential columns and clean rows (e.g., valid prognosis)
- Processes large files in chunks to manage memory efficiently
- Produces `intermediate_filtered_data.csv`

### 4. Data Transformation (`historical_transform.py`)

Transforms the pre-filtered data to compute train delays and categorize them.

- Converts timestamp columns with mixed formats
- Computes delay in minutes
- Categorizes delays into:
  - `On time`
  - `2 to 5minutes`
  - `5 to 15minutes`
  - `more than 15minutes`
  - `Cancelled`
- Renames columns to standardized English names
- Saves the result to `data/historical/processed/historical_transformed.csv`

### 5. Visualization and Analysis (`historical_data_analysis.py`)

Performs exploratory analysis of the historical train delay data with various visualizations:

- **Overview of delay distribution**: Point plot showing the overall delay distribution
- **Average delay by train category**: Bar chart showing which train types experience the most delays
- **Delay breakdown by station and category**: Stacked horizontal bar chart showing the proportion of each delay category by station
- **Bubble chart of frequency vs severity**: Compares stations by delay frequency and severity
- **Heatmap of delay percentages by weekday**: Shows which days experience more delays at each station
- **Line plot of delay percentages by hour**: Shows how delays vary throughout the day for each station

## Driver Files

The `drivers/` directory contains the chromedriver.exe driver files for Win64, needed for web scraping components using Selenium.

### Chromedriver

The project requires a compatible version of chromedriver.exe for your Chrome browser.

If the included chromedriver doesn't work with your version of Chrome:

1. Check your Chrome version (Help > About Google Chrome)
2. Download the matching chromedriver from: https://chromedriver.chromium.org/downloads
3. Replace the chromedriver.exe file in the `drivers/` directory

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
   - OpenTransportData Archive: For historical transport data from 2022-2024
   - Station Board archive: For archived departure/arrival information
   - Delay Information API v2.1: For comprehensive delay information

## Connection Pairs

- Zürich HB to Luzern
- Zürich HB to Genève
- Luzern to Genève

## Time Slots
```
time_slots = [06:00, 08:00, 10:00, 12:00, 14:00, 16:00, 18:00, 20:00]
```

## Methodology

Data preparation includes retrieving all relevant data, cleaning, pre-filtering, generating KeyIDs, and joining datasets iteratively. The analysis will utilize Python's Matplotlib and Seaborn for visualization and may incorporate an interactive dashboard using the shiny library for enhanced exploration. Quality control steps ensure consistency throughout the process.

## Setup and Installation

1. Clone the repository
2. Install required packages:

```bash
pip install -r requirements.txt
```

### Requirements

- Python 3.8+
- Pandas
- NumPy
- Requests
- Matplotlib
- Seaborn
- Selenium (`pip install selenium`)
- Patoolib (`pip install patool`)
- Jupyter (for notebooks)

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

### Running the Historical Data Pipeline

To process historical data:

1. Download historical data:
   ```bash
   python data_collection/historical_data/zip_scrapper.py
   ```

2. Extract archives:
   ```bash
   python data_collection/historical_data/zip_extract_delete.py
   ```

3. Filter data:
   ```bash
   python data_collection/historical_data/historical_df_intermediate.py
   ```

4. Transform data:
   ```bash
   python data_processing/historical/historical_transform.py
   ```

5. Analyze and visualize:
   ```bash
   python analysis/historical/historical_data_analysis.py
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

## Contributors

- Sahra ([@sahrabaettig](https://github.com/sahrabaettig))
- Mika ([@mikachulab](https://github.com/mikachulab))
- Roger ([@rogerjeasy](https://github.com/rogerjeasy))

## Sources

Baumgartner, S. (2024, March 11). SBB schreibt wieder schwarze Zahlen dank Rekordzahl an Reisenden.