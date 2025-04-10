# Train Delays Analysis Combining SBB API

This project analyzes train delays in the Swiss public transport system by leveraging real-time and historical data from the Swiss Federal Railways (SBB) APIs and other open transport data sources.

## Table of Contents
- [Introduction](#introduction)
- [Research Questions](#research-questions)
- [Project Structure](#project-structure)
- [Historical Data Pipeline](#historical-data-pipeline)
  - [1. Historical Data Collection](#1-historical-data-collection-zip_scrapperpy)
  - [2. Archive Extraction](#2-archive-extraction-zip_extract_deletepy)
  - [3. Data Filtering](#3-data-filtering-historical_df_intermediatepy)
  - [4. Data Transformation](#4-data-transformation-historical_transformpy)
  - [5. Visualization and Analysis](#5-visualization-and-analysis-historical_data_analysispy)
- [Current Data Pipeline](#current-data-pipeline)
  - [1. API Client](#1-api-client-api_clientpy)
  - [2. Station Board Collection](#2-station-board-collection-station_boardpy)
  - [3. Connections Collection](#3-connections-collection-connectionspy)
  - [4. Main Orchestration](#4-main-orchestration-mainpy)
  - [Data Flow](#data-flow)
  - [Challenges and Limitations](#challenges-and-limitations-of-the-current-data-pipeline)
- [Methodology](#methodology)
  - [Archive & Historical Data](#archive--historical-data)
  - [Current Data](#current-data)
  - [Causes and Disruptions](#causes-and-disruptions)
- [Results and Analysis](#results-and-analysis)
  - [Delay Distribution by Train Category](#delay-distribution-by-train-category)
  - [Station Performance Comparison](#station-performance-comparison)
  - [Temporal Patterns](#temporal-patterns)
- [Conclusion](#conclusion)
- [Setup and Installation](#setup-and-installation)
  - [Requirements](#requirements)
- [Usage](#usage)
  - [Collecting Data for a Specific Month](#collecting-data-for-a-specific-month)
  - [Collecting Data for Multiple Months](#collecting-data-for-multiple-months)
  - [Running the Historical Data Pipeline](#running-the-historical-data-pipeline)
- [Driver Files](#driver-files)
  - [Chromedriver](#chromedriver)
- [Contributors](#contributors)
- [Sources](#sources)

## Introduction

Switzerland depends on public transportation to move more than 1.3 million passengers daily through its Swiss Federal Railways (SBB) network (Baumgartner, 2024). Train delays remain a major problem despite Switzerland's well-known reputation for efficiency and punctuality. The disruptions which arise from infrastructure limitations and maintenance requirements, operational problems and outside factors negatively affect schedule dependability, passenger satisfaction and network efficiency.

Our research project examines train delays by analyzing SBB public API data and additional open data sources because of these problems. The research investigates which train lines and stations experience the most delays and how time and day affect disruptions and whether construction incidents and other delays contribute to these problems.

The project follows a complete data science pipeline starting with data acquisition and transformation followed by analysis and communication.

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
│   │   ├── YYYY-MM/
│   │   │   ├── Luzern_YYYY-MM-DD.csv
│   │   │   ├── Zürich_HB_YYYY-MM-DD.csv
│   │   │   ├── Genève_YYYY-MM-DD.csv
│   │   │   ├── connection_Zürich_HB_to_Luzern_YYYY-MM-DD.csv
│   │   │   ├── connection_Zürich_HB_to_Genève_YYYY-MM-DD.csv
|   |   │   ├── ...
│   │   │   └── delay_info_YYYY-MM-DD.csv
│   │   └── ...
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

## Historical Data Pipeline

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

## Current Data Pipeline

The current data pipeline uses the Swiss Transport API to collect, process, and analyze train delays in real-time. The pipeline consists of several coordinated modules working together:

### 1. API Client (`api_client.py`)

This module serves as the foundation for all API interactions:

- **Rate Limiting**: Implements sophisticated rate limiting to respect the API's constraints (limited to 1000 route queries per day)
- **Caching**: Stores responses to reduce duplicate requests
- **Retry Logic**: Uses exponential backoff for failed requests
- **Error Handling**: Gracefully handles API errors and network issues

The client provides three main functions:
- `get_station_info()`: Retrieves station information based on search queries
- `get_station_board()`: Gets departure or arrival boards for a station
- `get_connections()`: Fetches connections between two stations

### 2. Station Board Collection (`station_board.py`)

This module collects arrival information for target stations:

- **Data Collection**: Retrieves arrival data in hourly time slots from 05:00 to 23:59
- **Data Enrichment**: Extracts additional information from the train's journey (pass_list)
- **Data Processing**: Converts raw API responses into structured data with consistent formats
- **Data Storage**: Organizes files by month in CSV format

Key functions:
- `collect_station_data()`: Collects data for a single station
- `collect_monthly_data()`: Orchestrates collection for all stations for an entire month
- `process_stationboard_entry()`: Transforms raw API data into structured records

### 3. Connections Collection (`connections.py`)

This module collects data about connections between important station pairs:

- **Data Collection**: Retrieves connections at predefined time slots throughout the day
- **Data Processing**: Extracts detailed information about each connection section
- **Delay Calculation**: Attempts to calculate delays from available timestamps
- **Data Storage**: Saves processed connections as CSV files organized by month

Key functions:
- `collect_connection_data()`: Collects connections between two specific stations
- `collect_monthly_connections()`: Orchestrates collection for all connection pairs for a month
- `process_connection()`: Transforms raw connection data into structured records

### 4. Main Orchestration (`main.py`)

This script coordinates the entire data collection and analysis process:

- **CLI Interface**: Provides command-line options for year, month, and analysis options
- **Directory Management**: Creates and maintains the project's directory structure
- **Pipeline Coordination**: Orchestrates the collection and analysis workflow
- **Reporting**: Generates monthly summaries and analysis reports

Key functions:
- `collect_data_for_month()`: Coordinates collection of all data types for a month
- `analyze_existing_data()`: Performs analysis on previously collected data

### Data Flow

1. The process starts with `main.py`, which parses command-line arguments and determines the collection period
2. `main.py` calls the appropriate collection functions in the station_board and connections modules
3. These modules use the api_client to make requests to the Swiss Transport API
4. Raw API responses are processed into structured data records
5. Processed data is saved to CSV files organized by month
6. Analysis functions are called to generate insights and reports

### Challenges and Limitations of the Current Data Pipeline

As noted in the methodology, the current data pipeline faces significant limitations:

1. **Stationboard API Issue**: The API's arrival data (`type="arrival"`) omits actual arrival timestamps, providing only departure times for continuing journeys
2. **Connections API Limitation**: The connections endpoint returns only scheduled times rather than actual arrival timestamps
3. **Missing Delay Data**: The absence of actual arrival timestamps makes accurate delay calculation impossible using current data alone

## Methodology

To address our research questions, we used a structured methodology that spans data acquisition, transformation, and preparation for analysis. Given the complexity and heterogeneity of the available data, we segmented our approach into three main components: historical and archived data, current real-time data, and disruption-related information.

### Archive & Historical Data

To build a comprehensive view of past delays and train activity in Switzerland, we retrieved and processed historical train data made publicly available through the Swiss Open Transport Archive. Our workflow consisted of four main stages:

1. **Scraping ZIP Files**: Using a custom script with Selenium and Requests, we automated the download of all relevant .zip files containing train event logs from the opentransportdata.swiss archive. We filtered links by year (2022, 2023, 2024), storing the files in year-specific folders.

2. **Unzipping and Cleanup**: Once downloaded, the ZIP files were automatically extracted using the patoolib library, and the original archives were deleted to preserve disk space.

3. **Initial Filtering of Relevant Rows**: Given the large file sizes, we processed the CSV files in chunks and filtered rows that matched our criteria:
   - Only Swiss Federal Railways (SBB) trains (PRODUKT_ID = 'Zug')
   - Excluded passing trains and extra trains
   - Included only specific key stations (Zürich HB, Luzern, Genève)
   - Ensured arrival time predictions were available

   Only relevant columns were kept, and the cleaned chunks were aggregated into a single intermediate file.

4. **Data Transformation**: In the final transformation step, we parsed and standardized date and time formats, combining planned and predicted arrival times into datetime objects. A new DELAY column was calculated (in minutes), and delays were categorized into four groups: On time, 2 to 5 minutes, 5 to 15 minutes, and more than 15 minutes. Cancelled rides were also explicitly labeled.

### Current Data

Our data collection strategy relied primarily on two API endpoints from the Swiss Transport API (transport.opendata.ch): `/stationboard` and `/connections`. We leveraged Python's `requests` library to develop a robust collection system following modular design principles, creating dedicated modules for API client interactions, station board data processing, and connection data retrieval. This system systematically collected hourly data from January 2025 through April 06, 2025, for our three target stations: Zurich, Luzern, and Geneva.

Despite implementing rate limiting, caching mechanisms, and error handling in our API client to work within the API's constraints (limited to 1000 route queries per day), we discovered critical limitations in both endpoints that prevented us from accurately measuring train delays:

1. **Stationboard API Issue**: When requesting arrival data with the `type="arrival"`, the API returns records of trains arriving at the station but surprisingly omits their actual arrival timestamps. Instead, it provides only the departure times for when these trains continue their journey, making it impossible to determine when trains actually arrive at the station.

2. **Connections API Limitation**: The `/connections` endpoint, which we hoped would fill this gap, only returns scheduled times rather than actual arrival timestamps needed for delay calculation, preventing us from calculating actual delays for journeys between stations.

We attempted two approaches to address these limitations:
- Extracting arrival times from the passList information in train journeys
- Cross-referencing departure and arrival data to infer delays

Unfortunately, neither approach yielded reliable results, as the necessary actual arrival timestamps were consistently absent from the API responses. This made it impossible to accurately calculate train delays using current data.

Due to these constraints, our analysis ultimately relied on historical data sources rather than real-time API data. This experience revealed an important discrepancy between API documentation and actual data availability.

### Causes and Disruptions

The dataset for the causes and disruptions from the SBB has records since 2020. The data is unstructured, with essential details such as location, reason, affected line embedded in text blocks, as well as the start and end time for each interruption. To prepare the data for analysis, several transformation and cleaning steps were necessary.

Historical data is downloaded from data.sbb.ch, while new data is retrieved via an API. Since the API limits each request to 100 records, a function was implemented to compare the "published" date of new entries with the latest entry in the historical dataset. This approach ensures that only missing entries are added.

Duplicate entries were removed, as planned interruptions were republished nightly. Only the first published record of each interruption was kept. Rows containing "Aufgehoben" (removed) were dropped, as they were irrelevant.

Additional columns were added, including Duration, Days until start, and a classification for planned or unplanned interruptions. Interruptions were categorized as unplanned if they were published on the same day they started. The reason for interruptions was extracted using regex patterns that identify phrases like "Der Grund dafür ist...". Affected train lines were extracted in a similar way.

## Results and Analysis

Our analysis of Swiss train delays revealed several key insights about delay patterns across different stations, train categories, and time periods.

### Delay Distribution by Train Category

Long-distance and international trains (NightJet, RailJet, RJX) show the highest average delays, while regional and suburban trains (S-Bahn, RegioExpress) tend to be more punctual. This suggests that longer routes with more potential disruption points lead to greater delays.

![Average Delay per Train Category](./images/average_delay.png)

### Station Performance Comparison

All three analyzed stations (Zürich HB, Luzern, and Genève) show a high percentage of on-time trains (>84%). However, Luzern exhibits a higher proportion of delays in the 2-5 minute range (13.2%) compared to Zürich HB (11.2%), despite Zürich HB handling significantly more train traffic.

![Delay Categories per Station](./images/train_delay.png)

### Temporal Patterns

Delays are more frequent during weekdays compared to weekends, with Tuesday being the peak day for delays at Luzern (18.9%). Throughout the day, delay rates rise noticeably during morning (7:00-9:00) and evening (17:00-19:00) rush hours, with Luzern reaching up to 30% delayed trains in the evening peak.

![Delays by Day of Week](./images/frequencyvsseverity.png)

![Delays by Hour of Day](./images/hour_delay.png)

![Delays by Weekday](./images/percentage_train_delay.png)

## Conclusion

The Swiss Transport API reveals important limitations in current public transport data availability that impact delay analysis. Despite collecting extensive data from January to April 2025, the absence of actual arrival timestamps in both API endpoints created an insurmountable obstacle for accurate delay calculating using current data.

This experience highlights a critical gap between data availability and analytical requirements for transportation research. While the APIs provide comprehensive schedule information, they lack the essential actual timing data needed for meaningful delay analysis. Future efforts might benefit from direct collaboration with Swiss Federal Railways to access more complete timing data, or from enhancing the existing APIs to include the missing actual arrival timestamp information.


## Setup and Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/CIP-FS25-106/CIP_FS25_106.git
   cd CIP_FS25_106
   ```

2. Create a virtual environment:

   **Windows:**
   ```bash
   # Using venv (built into Python)
   python -m venv venv
   
   # Activate the environment
   venv\Scripts\activate
   
   # For PowerShell, use:
   # .\venv\Scripts\Activate.ps1
   ```

   **macOS/Linux:**
   ```bash
   # Using venv
   python3 -m venv venv
   
   # Activate the environment
   source venv/bin/activate
   ```

   **Using Anaconda (all platforms):**
   ```bash
   # Create environment
   conda create --name traindelay python=3.8
   
   # Activate environment
   conda activate traindelay
   ```

3. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up ChromeDriver (for web scraping components):
   - Ensure the ChromeDriver executable in the `drivers/` directory matches your Chrome version
   - If needed, download the appropriate version from: https://chromedriver.chromium.org/downloads

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

### Collecting Data Through the API for a Specific Month (e.g., January 2025)

```bash
python main.py --year 2025 --month 1
```

### Collecting Data Through the API for Multiple Months (e.g., January to March 2025)

```bash
python main.py --year 2025 --month 3 --all-months
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

## Driver Files

The `drivers/` directory contains the chromedriver.exe driver files for Win64, needed for web scraping components using Selenium.

### Chromedriver

The project requires a compatible version of chromedriver.exe for your Chrome browser.

If the included chromedriver doesn't work with your version of Chrome:

1. Check your Chrome version (Help > About Google Chrome)
2. Download the matching chromedriver from: https://chromedriver.chromium.org/downloads
3. Replace the chromedriver.exe file in the `drivers/` directory

## Contributors

- Sahra ([@sahrabaettig](https://github.com/sahrabaettig))
- Mika ([@mikachulab](https://github.com/mikachulab))
- Roger ([@rogerjeasy](https://github.com/rogerjeasy))

## Sources

Baumgartner, S. (2024, March 11). SBB schreibt wieder schwarze Zahlen dank Rekordzahl an Reisenden.