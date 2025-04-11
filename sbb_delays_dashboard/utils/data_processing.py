"""
data_processing.py - Utility module for loading and processing train delay data

This module handles downloading, concatenating, and processing the historical 
train delay data for visualization in the dashboard.
"""

import pandas as pd
import numpy as np
import requests
import gzip
import io
import logging
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from functools import lru_cache
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DELAY_THRESHOLD = 2  # Minutes threshold for considering a train delayed
DATA_DIR = Path("./data")
DATA_URLS = [
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315794/historical_transformed_part001.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315806/historical_transformed_part002.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315817/historical_transformed_part003.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315828/historical_transformed_part004.gz",
    "https://res.cloudinary.com/db5dgs9zy/raw/upload/v1744315838/historical_transformed_part005.gz"
]

# Target stations to analyze - these should match exactly with the data after encoding fixes
TARGET_STATIONS_ORIGINAL = ["Zürich HB", "Luzern", "Genève"]

# Map of encoded station names to original station names
STATION_NAME_MAP = {
    "ZÃ¼rich HB": "Zürich HB",
    "Zurich HB": "Zürich HB",
    "Zürich HB": "Zürich HB",
    "Luzern": "Luzern",
    "GenÃ¨ve": "Genève",
    "Geneve": "Genève",
    "Genève": "Genève"
}

# We'll use both the original names and the encoded names
TARGET_STATIONS = list(STATION_NAME_MAP.values())

# Maximum number of rows to sample per station
MAX_ROWS_PER_STATION = 100000


def ensure_data_directory() -> Path:
    """
    Create a data directory if it doesn't exist.
    
    Returns:
        Path: Path to data directory
    """
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True)
        logger.info(f"Created data directory at {DATA_DIR}")
    
    return DATA_DIR


def download_and_cache_data() -> Path:
    """
    Download and concatenate the gzipped CSV files if not already cached.
    
    Returns:
        Path: Path to the combined CSV file
    """
    data_dir = ensure_data_directory()
    combined_file = data_dir / "historical_transformed_combined.csv"
    
    # Return the cached file if it exists
    if combined_file.exists():
        logger.info(f"Using cached data file: {combined_file}")
        return combined_file
    
    logger.info("Downloading and combining data files...")
    combined_df = pd.DataFrame()
    
    for i, url in enumerate(DATA_URLS):
        try:
            logger.info(f"Downloading part {i+1} of {len(DATA_URLS)}...")
            response = requests.get(url)
            response.raise_for_status()
            
            # Decompress and load into DataFrame
            with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
                part_df = pd.read_csv(f)
                combined_df = pd.concat([combined_df, part_df], ignore_index=True)
            
            logger.info(f"Successfully added part {i+1}, current shape: {combined_df.shape}")
            
        except Exception as e:
            logger.error(f"Error downloading or processing part {i+1}: {e}")
            raise
    
    # Save the combined DataFrame to a CSV file
    combined_df.to_csv(combined_file, index=False, encoding='utf-8')
    logger.info(f"Combined data saved to {combined_file}")
    
    return combined_file


def fix_station_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix encoding issues in station names.
    
    Args:
        df: DataFrame with station names
        
    Returns:
        pd.DataFrame: DataFrame with fixed station names
    """
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Map station names using the dictionary
    df['station_name_original'] = df['station_name']
    
    # Function to map station names
    def map_station_name(name):
        return STATION_NAME_MAP.get(name, name)
    
    # Apply the mapping
    df['station_name'] = df['station_name'].apply(map_station_name)
    
    # Log the mapping that occurred
    # Count unique original and new station names
    name_mapping_count = df.groupby(['station_name_original', 'station_name']).size().reset_index(name='count')
    logger.info(f"Station name mapping statistics:\n{name_mapping_count}")
    
    return df


@lru_cache(maxsize=1)
def load_and_prepare_data() -> pd.DataFrame:
    """
    Load and prepare the data for visualization. Results are cached to improve performance.
    
    Returns:
        pd.DataFrame: Prepared DataFrame
    """
    try:
        file_path = download_and_cache_data()
        logger.info(f"Loading data from {file_path}")
        
        # Explicitly specify encoding
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Log column names to help with debugging
        logger.info(f"Columns in dataset: {df.columns.tolist()}")
        
        # Log unique station names before fixing
        if 'station_name' in df.columns:
            unique_stations_before = df['station_name'].unique()
            logger.info(f"Unique station names before fixing: {unique_stations_before}")
        else:
            logger.error("No 'station_name' column found in the data")
            logger.info(f"First 5 rows of data: {df.head(5)}")
            raise ValueError("Missing 'station_name' column in the dataset")
        
        # Fix encoding issues in station names
        df = fix_station_names(df)
        
        # Log unique station names after fixing
        unique_stations_after = df['station_name'].unique()
        logger.info(f"Unique station names after fixing: {unique_stations_after}")
        
        # Filter for target stations
        df_filtered = df[df["station_name"].isin(TARGET_STATIONS)].copy()
        logger.info(f"Filtered for stations: {TARGET_STATIONS}, {len(df_filtered)} records remaining")
        
        # Verify that we have at least some data for each target station
        stations_in_filtered_data = df_filtered["station_name"].unique()
        logger.info(f"Stations in filtered data: {stations_in_filtered_data}")
        
        # To improve performance, sample the data for each station
        logger.info(f"Data size before sampling: {len(df_filtered)}")
        sampled_df = pd.DataFrame()
        
        for station in stations_in_filtered_data:
            station_data = df_filtered[df_filtered["station_name"] == station]
            if len(station_data) > MAX_ROWS_PER_STATION:
                # Sample with stratification by DELAY_CAT to maintain distribution
                station_sample = station_data.groupby("DELAY_CAT", group_keys=False).apply(
                    lambda x: x.sample(
                        min(len(x), int(MAX_ROWS_PER_STATION * len(x) / len(station_data))),
                        random_state=42
                    )
                )
                logger.info(f"Sampled {station}: from {len(station_data)} to {len(station_sample)} records")
            else:
                station_sample = station_data
                logger.info(f"Using all {len(station_data)} records for {station}")
            
            sampled_df = pd.concat([sampled_df, station_sample], ignore_index=True)
        
        logger.info(f"Data size after sampling: {len(sampled_df)}")
        
        # Continue with the sampled data
        df = sampled_df
        
        # Convert ride_day to datetime
        df["ride_day"] = pd.to_datetime(df["ride_day"], errors="coerce")
        logger.info(f"Date range: {df['ride_day'].min()} to {df['ride_day'].max()}")
        
        # Convert arrival planned column
        df["scheduled_arrival"] = pd.to_datetime(df["scheduled_arrival"], errors="coerce")
        
        # Remove extreme negative delays
        df = df[(df["DELAY"] >= -500)].copy()  # Use .copy() to avoid SettingWithCopyWarning
        
        # Add derived columns for analysis
        df["is_delayed"] = df["DELAY"] > DELAY_THRESHOLD
        df["day_of_week"] = df["ride_day"].dt.day_name()
        df["hour"] = df["scheduled_arrival"].dt.hour
        
        # Create ordered categorical variable for day of week
        weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        df["day_of_week"] = pd.Categorical(
            df["day_of_week"], 
            categories=weekday_order, 
            ordered=True
        )
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading or preparing data: {e}")
        # Add more detailed error information
        if 'df' in locals() and isinstance(df, pd.DataFrame):
            logger.error(f"DataFrame shape: {df.shape}")
            logger.error(f"DataFrame columns: {df.columns.tolist()}")
            if 'station_name' in df.columns:
                logger.error(f"Unique station names: {df['station_name'].unique()}")
        raise


def get_pre_aggregated_data() -> Dict[str, pd.DataFrame]:
    """
    Get pre-aggregated data for all visualizations to improve performance.
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of pre-aggregated datasets
    """
    df = load_and_prepare_data()
    
    result = {}
    
    # Pre-aggregate data for delay categories visualization
    result["delay_categories"] = df.groupby(["station_name", "DELAY_CAT"], observed=True).size().reset_index(name="count")
    
    # Pre-aggregate data for train categories visualization
    result["train_categories"] = df.groupby("train_category", observed=True)["DELAY"].mean().reset_index()
    
    # Pre-aggregate data for weekday heatmap
    result["weekday_heatmap"] = df.groupby(["station_name", "day_of_week"], observed=True).agg(
        total=("DELAY", "count"),
        delayed=("is_delayed", "sum")
    ).reset_index()
    result["weekday_heatmap"]["pct_delayed"] = 100 * result["weekday_heatmap"]["delayed"] / result["weekday_heatmap"]["total"]
    
    # Pre-aggregate data for hourly lineplot
    result["hourly_lineplot"] = df.groupby(["hour", "station_name"], observed=True).agg(
        total=("DELAY", "count"),
        delayed=("is_delayed", "sum")
    ).reset_index()
    result["hourly_lineplot"]["pct_delayed"] = 100 * result["hourly_lineplot"]["delayed"] / result["hourly_lineplot"]["total"]
    
    # Pre-aggregate data for station summary
    result["station_summary"] = df.groupby("station_name", observed=True).agg(
        avg_delay=("DELAY", "mean"),
        total_trains=("DELAY", "count"),
        delayed_trains=("is_delayed", "sum")
    ).reset_index()
    result["station_summary"]["pct_delayed"] = 100 * result["station_summary"]["delayed_trains"] / result["station_summary"]["total_trains"]
    
    return result


def get_delay_category_data() -> pd.DataFrame:
    """
    Process data for the delay category visualizations.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    counts = aggregated_data["delay_categories"]
    
    # Calculate percentages
    totals = counts.groupby("station_name", observed=True)["count"].sum().reset_index(name="total")
    counts = counts.merge(totals, on="station_name")
    counts["percentage"] = 100 * counts["count"] / counts["total"]
    
    return counts


def get_category_delay_data() -> pd.DataFrame:
    """
    Process data for the train category delay visualizations.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    avg_by_category = aggregated_data["train_categories"]
    avg_by_category = avg_by_category.sort_values(by="DELAY", ascending=False)
    
    return avg_by_category


def get_bubble_chart_data() -> pd.DataFrame:
    """
    Process data for the bubble chart visualization.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    return aggregated_data["station_summary"]


def get_weekday_heatmap_data() -> pd.DataFrame:
    """
    Process data for the weekday heatmap visualization.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    return aggregated_data["weekday_heatmap"]


def get_hourly_lineplot_data() -> pd.DataFrame:
    """
    Process data for the hourly line plot visualization.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    return aggregated_data["hourly_lineplot"]