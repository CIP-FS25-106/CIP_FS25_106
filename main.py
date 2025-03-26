"""
main.py - Main script for the Swiss Train Delays Analysis project

This script coordinates the data collection process using the data_collection package.
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

from data_collection.station_board import collect_monthly_data
from data_collection.connections import collect_monthly_connections
# from data_collection.historical_data import collect_monthly_historical_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_collection.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define target stations with their IDs
TARGET_STATIONS = {
    "Luzern": "8505000",
    "Zürich HB": "8503000",
    "Genève": "8501008"
}


def create_directory_structure(base_dir: str = "data"):
    """
    Create the directory structure for the project.
    
    Args:
        base_dir: Base directory for data
    """
    # Create raw data directory
    raw_dir = os.path.join(base_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    
    # Create processed data directory
    processed_dir = os.path.join(base_dir, "processed")
    os.makedirs(processed_dir, exist_ok=True)
    
    logger.info(f"Created directory structure in {base_dir}")


def collect_data_for_month(year: int, month: int, data_dir: str = "data"):
    """
    Collect all types of data for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        data_dir: Base directory for data
    """
    logger.info(f"Starting data collection for {year}-{month:02d}")
    
    # Create directory structure
    create_directory_structure(data_dir)
    raw_dir = os.path.join(data_dir, "raw")
    
    # Collect station board data
    logger.info("Collecting station board data...")
    station_files = collect_monthly_data(year, month, raw_dir)
    logger.info(f"Collected {len(station_files)} station board files")
    
    # Collect connection data
    logger.info("Collecting connection data...")
    connection_files = collect_monthly_connections(year, month, data_dir=raw_dir)
    logger.info(f"Collected {len(connection_files)} connection files")
    
    # Collect historical data
    logger.info("Collecting historical data...")
    target_station_ids = list(TARGET_STATIONS.values())
    # historical_files = collect_monthly_historical_data(year, month, target_station_ids, raw_dir)
    # logger.info(f"Collected {len(historical_files)} historical data files")
    
    # Combined report
    # total_files = len(station_files) + len(connection_files) + len(historical_files)
    total_files = len(station_files) + len(connection_files)
    logger.info(f"Data collection completed for {year}-{month:02d}. Total files: {total_files}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Collect train delay data")
    
    parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Year to collect data for (default: current year)"
    )
    
    parser.add_argument(
        "--month",
        type=int,
        default=datetime.now().month,
        help="Month to collect data for (default: current month)"
    )
    
    parser.add_argument(
        "--all-months",
        action="store_true",
        help="Collect data for all months from January to specified month or current month"
    )
    
    parser.add_argument(
        "--data-dir",
        type=str,
        default="data",
        help="Base directory for data storage (default: 'data')"
    )
    
    return parser.parse_args()


def main():
    """Main entry point for the data collection script."""
    args = parse_args()
    
    if args.all_months:
        # Collect data for all months from January to specified month
        for month in range(1, args.month + 1):
            collect_data_for_month(args.year, month, args.data_dir)
    else:
        # Collect data for the specified month only
        collect_data_for_month(args.year, args.month, args.data_dir)


if __name__ == "__main__":
    main()