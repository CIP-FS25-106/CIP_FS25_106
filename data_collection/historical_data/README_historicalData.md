# Historical Data Collection

This folder contains scripts to download, extract, and process historical train data from the Swiss Federal Railways archives.

## Files Overview

- **zip_scraper.py**: Downloads historical data ZIP files from the SBB archive
- **zip_extractor.py**: Extracts the downloaded ZIP files 
- **data_preparation.py**: Processes the extracted data and creates a filtered dataset

## Prerequisites

Before running these scripts, make sure you have:

1. Python 3.8 or later installed
2. The following Python packages:
   - selenium
   - requests
   - pandas
   - numpy
   - patoolib

Install them with:
```
pip install selenium requests pandas numpy patoolib
```

3. ChromeDriver in the 'drivers' folder at the project root
   - Download a version compatible with your Chrome browser from: https://chromedriver.chromium.org/downloads
   - Place the chromedriver.exe file in the 'drivers' folder

## Directory Structure

The scripts will create and use the following directory structure:
```
CIP_FS25_106/
├── drivers/
│   └── chromedriver.exe
├── data/
│   └── historical/
│       ├── downloads_2022/  (created automatically)
│       ├── downloads_2023/  (created automatically)
│       ├── downloads_2024/  (created automatically)
│       └── processed/       (created automatically)
```

## Usage Instructions

### Step 1: Download the Historical Data

```bash
python zip_scraper.py
```

This will:
- Download historical data ZIP files from the SBB archive
- Save them in folders like 'data/historical/downloads_2022'
- Note: This step requires an internet connection and may take some time depending on your connection speed

### Step 2: Extract the ZIP Files

```bash
python zip_extractor.py
```

This will:
- Extract all downloaded ZIP files 
- Delete the original ZIP files to save space
- The extracted CSV files will be in the same folder as the ZIP files

### Step 3: Process the Data

```bash
python data_preparation.py
```

This will:
- Process all CSV files from the extracted data
- Apply filters to keep only SBB train data for Zürich HB, Luzern, and Genève
- Calculate delay statistics and categorize delays
- Rename all columns to English
- Save the processed data in 'data/historical/processed/historical_data_YYYYMMDD_HHMMSS.csv'

## Customizing the Scripts

If you want to modify which years to process or which stations to include:

### To Change the Years

Edit the `years` list near the top of each script:

```python
# List of years to process
years = ["2022", "2023", "2024"]  # Modify this list as needed
```

### To Change the Stations

Edit the `stations` list in data_preparation.py:

```python
# Define the stations to filter for
stations = ["Zürich HB", "Luzern", "Genève"]  # Modify this list as needed
```

## Output

The final processed data will be saved in:
```
data/historical/processed/historical_data_YYYYMMDD_HHMMSS.csv
```

This file contains the following columns:
- ride_day: Date of the train ride
- ride_description: Train identifier
- train_operator: Operator of the train (filtered to SBB only)
- product_id: Type of product
- line_text: Line information
- train_category: Category of train
- extra_train: Whether it's an extra train
- cancelled: Whether the train was cancelled
- station_name: Name of the station
- scheduled_arrival: Scheduled arrival time
- arrival_prognosis: Actual/predicted arrival time
- arrival_prognosis_status: Status of the arrival prediction
- nostopping: Whether the train passes through without stopping
- DELAY: Calculated delay in minutes
- DELAY_CAT: Categorized delay ("On time", "2 to 5minutes", "5 to 15minutes", "more than 15minutes", "Cancelled")