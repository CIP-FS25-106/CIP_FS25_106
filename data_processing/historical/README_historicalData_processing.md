# Historical Data Transformation Script

This script transforms pre-filtered Swiss public transport data to compute train delays and categorize them for further analysis.

## What It Does

- Loads the file:  data/historical/processed/intermediate_filtered_data.csv
- Converts timestamp columns with mixed formats
- Computes delay in minutes
- Categorizes delay into:
- `On time`
- `2 to 5minutes`
- `5 to 15minutes`
- `more than 15minutes`
- `Cancelled`
- Renames columns to standardized English names
- Saves the result to: data/historical/processed/historical_transformed.csv


## Requirements

- Python 3.8+
- `pandas`, `numpy`