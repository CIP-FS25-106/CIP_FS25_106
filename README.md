# Swiss Real Estate Market Analysis

This project scrapes and analyzes real estate data from three major Swiss property portals:
- ImmoScout24
- Homegate
- Comparis.ch

## Project Structure

```
swiss_realestate_analysis/
│
├── main.py                   # Main script to run the scraper
│
├── scrapers/                 # Website scrapers
│   ├── __init__.py           # Make scrapers a package
│   ├── immoscout_scraper.py  # ImmoScout24 scraper
│   ├── homegate_scraper.py   # Homegate scraper
│   └── comparis_scraper.py   # Comparis scraper
│
├── processors/               # Data processing modules
│   ├── __init__.py           # Make processors a package
│   └── data_processor.py     # Data cleaning and processing
│
├── analysis/                 # Analysis scripts
│   ├── __init__.py           # Make analysis a package
│   ├── price_analysis.py     # Price analysis functions
│   ├── location_analysis.py  # Geographic analysis functions
│   └── feature_analysis.py   # Property feature analysis
│
├── utils/                    # Utility functions
│   ├── __init__.py           # Make utils a package
│   └── helpers.py            # Helper functions
│
├── data/                     # Data storage (created by the scripts)
│   ├── raw/                  # Raw scraped data
│   └── processed/            # Cleaned and processed data
│
├── visualizations/           # Output visualizations
│
├── notebooks/                # Jupyter notebooks for analysis
│
├── requirements.txt          # Project dependencies
│
└── README.md                 # Project documentation
```

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

4. Set up ChromeDriver for Selenium:
- Download the appropriate version of ChromeDriver for your Chrome browser
- Add it to your PATH or specify its location in the code

## Usage

### Running the Scraper

To run the scraper with default settings:
```
python main.py
```

With specific options:
```
python main.py --sources immoscout homegate --property-type rent --max-pages 10 --location zurich
```

Options:
- `--sources`: Which sources to scrape ('immoscout', 'homegate', 'comparis', or 'all')
- `--property-type`: Type of properties to scrape ('buy', 'rent', or 'both')
- `--max-pages`: Maximum number of pages to scrape per source
- `--location`: Location to search for properties (default: 'schweiz')

### Data Processing

The scraped data will be automatically processed and saved in the `data` directory.

## Requirements

- Python 3.8+
- BeautifulSoup4
- Selenium
- Pandas
- NumPy
- Matplotlib
- Seaborn
- GeoPy

## Analysis Focus

The project implements the necessary Python code to answer the following research questions:

1. How do property prices per square meter vary across different cantons in Switzerland, and what regional factors most strongly correlate with these price differences?

2. What property features contribute most significantly to price premiums in the Swiss housing market, and does the importance of these features vary between rental and purchase properties?

3. How does the relationship between property supply and demand vary across different Swiss cities, and what patterns emerge when comparing high-season versus low-season periods?

## Notes

- The scrapers respect rate limiting and use appropriate delays between requests
- The project follows ethical web scraping practices by checking robots.txt
- Be aware that website structures may change, requiring updates to the scrapers