# Historical Train Data Pipeline

This project automates the retrieval, extraction, and transformation of Swiss public transport data from the OpenTransportData archive.

## Scripts Overview

### 1. `zip_scrapper.py`
Downloads ZIP files for the years 2022–2024 from [OpenTransportData Archive](https://archive.opentransportdata.swiss/actual_data_archive.htm) using Selenium. The files are saved to to data/historical/downloads_YYYY

### 2. `zip_extract_delete.py`
Extracts all downloaded ZIP files per year and deletes the ZIPs after extraction. Uses `patoolib` for archive handling.

### 3. `historical_df_intermediate.py`
Processes and filters raw `.csv` files:
- Selects relevant stations (Zürich HB, Luzern, Genève)
- Keeps only essential columns and clean rows (e.g. SBB, valid prognosis)
- Produces an intermediate filtered dataset


## Requirements
- Python 3.8+
- Selenium (`pip install selenium`)
- Patoolib (`pip install patool`)
- ChromeDriver installed in `drivers/`