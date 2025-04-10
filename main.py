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

# Import the delay analysis functionality from analysis package
from analysis.delay_analysis import analyze_connections_for_month, create_monthly_summary

# Import configuration
from config import (
    TARGET_STATIONS, DEFAULT_DATA_DIR, DEFAULT_RAW_DIR, DEFAULT_PROCESSED_DIR,
    DEFAULT_ANALYSIS_DIR
)

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


def create_directory_structure(base_dir: str = DEFAULT_DATA_DIR):
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
    
    # Create analysis directory
    analysis_dir = os.path.join(base_dir, "analysis")
    os.makedirs(analysis_dir, exist_ok=True)
    
    logger.info(f"Created directory structure in {base_dir}")


def collect_data_for_month(year: int, month: int, data_dir: str = DEFAULT_DATA_DIR):
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
    analysis_dir = os.path.join(data_dir, "analysis")
    
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
    
    # Perform delay analysis using the collected data
    logger.info("Performing delay analysis...")
    analysis_files = analyze_connections_for_month(year, month, raw_data_dir=raw_dir, output_dir=analysis_dir)
    logger.info(f"Generated {len(analysis_files)} delay analysis files")
    
    # Create monthly summary
    logger.info("Creating monthly summary report...")
    summary_file = create_monthly_summary(year, month, output_dir=analysis_dir)
    if summary_file:
        logger.info(f"Monthly summary saved to {summary_file}")
    else:
        logger.warning("Failed to create monthly summary")
    
    # Combined report
    # total_files = len(station_files) + len(connection_files) + len(historical_files) + len(analysis_files)
    total_files = len(station_files) + len(connection_files) + len(analysis_files)
    logger.info(f"Data collection and analysis completed for {year}-{month:02d}. Total files: {total_files}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Collect and analyze train delay data")
    
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
        default=DEFAULT_DATA_DIR,
        help=f"Base directory for data storage (default: '{DEFAULT_DATA_DIR}')"
    )
    
    parser.add_argument(
        "--analysis-only",
        action="store_true",
        help="Skip data collection and only perform analysis on existing data"
    )
    
    return parser.parse_args()


def analyze_existing_data(year: int, month: int, data_dir: str = DEFAULT_DATA_DIR):
    """
    Analyze existing data for a specific month without collecting new data.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        data_dir: Base directory for data
    """
    logger.info(f"Starting offline data analysis for {year}-{month:02d}")
    
    # Ensure analysis directory exists
    raw_dir = os.path.join(data_dir, "raw")
    analysis_dir = os.path.join(data_dir, "analysis")
    os.makedirs(analysis_dir, exist_ok=True)
    
    # Check if raw data exists for this month
    month_str = f"{year}-{month:02d}"
    month_dir = os.path.join(raw_dir, month_str)
    
    if not os.path.exists(month_dir):
        logger.error(f"No raw data found for {month_str}. Please collect data first or check the path.")
        return
    
    # Perform delay analysis
    logger.info("Performing offline delay analysis using existing data...")
    analysis_files = analyze_connections_for_month(year, month, raw_data_dir=raw_dir, output_dir=analysis_dir)
    logger.info(f"Generated {len(analysis_files)} delay analysis files")
    
    # Create monthly summary
    logger.info("Creating monthly summary report...")
    summary_file = create_monthly_summary(year, month, output_dir=analysis_dir)
    if summary_file:
        logger.info(f"Monthly summary saved to {summary_file}")
    else:
        logger.warning("Failed to create monthly summary")


def main():
    """Main entry point for the data collection script."""
    args = parse_args()
    
    if args.analysis_only:
        # Only perform analysis on existing data
        if args.all_months:
            for month in range(1, args.month + 1):
                analyze_existing_data(args.year, month, args.data_dir)
        else:
            analyze_existing_data(args.year, args.month, args.data_dir)
    else:
        # Collect data and perform analysis
        if args.all_months:
            # Collect data for all months from January to specified month
            for month in range(1, args.month + 1):
                collect_data_for_month(args.year, month, args.data_dir)
        else:
            # Collect data for the specified month only
            collect_data_for_month(args.year, args.month, args.data_dir)


if __name__ == "__main__":
    main()