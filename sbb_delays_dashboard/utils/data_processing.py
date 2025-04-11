"""
data_processing.py - Utility module for loading and processing train delay data

This module handles streaming and processing the historical train delay data
for visualization in the dashboard, optimized for memory-constrained environments.
"""

import pandas as pd
import numpy as np
import requests
import gzip
import io
import logging
import os
from typing import List, Dict, Optional, Tuple, Iterator
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

# Maximum number of rows to process per station for memory efficiency
MAX_ROWS_PER_STATION = 50000

# Pre-aggregated data cache (in-memory)
_AGGREGATED_DATA_CACHE = {}


def stream_data_in_chunks(url: str, chunksize: int = 10000) -> Iterator[pd.DataFrame]:
    """
    Stream data from URL in chunks to avoid loading entire file into memory.
    
    Args:
        url: URL to gzipped CSV file
        chunksize: Number of rows per chunk
        
    Yields:
        pd.DataFrame: Chunk of data
    """
    try:
        logger.info(f"Streaming data from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Create a streaming decompression object
        with gzip.open(io.BytesIO(response.content), 'rt', encoding='utf-8') as f:
            # Stream chunks from the CSV
            for chunk in pd.read_csv(f, chunksize=chunksize):
                # Immediately filter for target stations to reduce memory usage
                chunk_filtered = chunk[
                    chunk["station_name"].apply(
                        lambda x: any(target in str(x) for target in TARGET_STATIONS_ORIGINAL)
                    )
                ].copy()
                
                if not chunk_filtered.empty:
                    # Fix encoding issues in station names
                    chunk_filtered = fix_station_names(chunk_filtered)
                    # Only yield if there's data matching our criteria
                    if not chunk_filtered[chunk_filtered["station_name"].isin(TARGET_STATIONS)].empty:
                        yield chunk_filtered
                        
    except Exception as e:
        logger.error(f"Error streaming data from {url}: {e}")
        raise


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
    
    return df


def process_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Process a chunk of data with necessary transformations.
    
    Args:
        chunk: DataFrame chunk to process
        
    Returns:
        pd.DataFrame: Processed chunk
    """
    # Filter for target stations
    chunk_filtered = chunk[chunk["station_name"].isin(TARGET_STATIONS)].copy()
    
    if chunk_filtered.empty:
        return pd.DataFrame()
    
    # Convert date columns
    chunk_filtered["ride_day"] = pd.to_datetime(chunk_filtered["ride_day"], errors="coerce")
    chunk_filtered["scheduled_arrival"] = pd.to_datetime(chunk_filtered["scheduled_arrival"], errors="coerce")
    
    # Remove extreme negative delays
    chunk_filtered = chunk_filtered[(chunk_filtered["DELAY"] >= -500)]
    
    # Add derived columns for analysis
    chunk_filtered["is_delayed"] = chunk_filtered["DELAY"] > DELAY_THRESHOLD
    chunk_filtered["day_of_week"] = chunk_filtered["ride_day"].dt.day_name()
    chunk_filtered["hour"] = chunk_filtered["scheduled_arrival"].dt.hour
    
    # Create ordered categorical variable for day of week
    weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    chunk_filtered["day_of_week"] = pd.Categorical(
        chunk_filtered["day_of_week"], 
        categories=weekday_order, 
        ordered=True
    )
    
    return chunk_filtered


def incrementally_aggregate_data(
    chunk: pd.DataFrame, 
    aggregations: Dict
) -> Dict:
    """
    Incrementally aggregate data from chunks.
    
    Args:
        chunk: DataFrame chunk to aggregate
        aggregations: Current aggregation state
        
    Returns:
        Dict: Updated aggregation state
    """
    if chunk.empty:
        return aggregations
    
    # Initialize aggregations dictionary if not already done
    if not aggregations:
        aggregations = {
            "delay_categories": {},
            "train_categories": {},
            "weekday_heatmap": {},
            "hourly_lineplot": {},
            "station_summary": {}
        }
    
    # Update delay categories aggregation
    delay_cat_counts = chunk.groupby(["station_name", "DELAY_CAT"]).size().reset_index(name="count")
    for _, row in delay_cat_counts.iterrows():
        key = (row["station_name"], row["DELAY_CAT"])
        if key in aggregations["delay_categories"]:
            aggregations["delay_categories"][key] += row["count"]
        else:
            aggregations["delay_categories"][key] = row["count"]
    
    # Update train categories aggregation
    train_cat_groups = chunk.groupby("train_category")
    for cat, group in train_cat_groups:
        if cat in aggregations["train_categories"]:
            aggregations["train_categories"][cat]["sum"] += group["DELAY"].sum()
            aggregations["train_categories"][cat]["count"] += len(group)
        else:
            aggregations["train_categories"][cat] = {
                "sum": group["DELAY"].sum(),
                "count": len(group)
            }
    
    # Update weekday heatmap aggregation
    weekday_groups = chunk.groupby(["station_name", "day_of_week"])
    for key, group in weekday_groups:
        if key in aggregations["weekday_heatmap"]:
            aggregations["weekday_heatmap"][key]["total"] += len(group)
            aggregations["weekday_heatmap"][key]["delayed"] += group["is_delayed"].sum()
        else:
            aggregations["weekday_heatmap"][key] = {
                "total": len(group),
                "delayed": group["is_delayed"].sum()
            }
    
    # Update hourly lineplot aggregation
    hourly_groups = chunk.groupby(["hour", "station_name"])
    for key, group in hourly_groups:
        if key in aggregations["hourly_lineplot"]:
            aggregations["hourly_lineplot"][key]["total"] += len(group)
            aggregations["hourly_lineplot"][key]["delayed"] += group["is_delayed"].sum()
        else:
            aggregations["hourly_lineplot"][key] = {
                "total": len(group),
                "delayed": group["is_delayed"].sum()
            }
    
    # Update station summary aggregation
    station_groups = chunk.groupby("station_name")
    for station, group in station_groups:
        if station in aggregations["station_summary"]:
            aggregations["station_summary"][station]["delay_sum"] += group["DELAY"].sum()
            aggregations["station_summary"][station]["total_trains"] += len(group)
            aggregations["station_summary"][station]["delayed_trains"] += group["is_delayed"].sum()
        else:
            aggregations["station_summary"][station] = {
                "delay_sum": group["DELAY"].sum(),
                "total_trains": len(group),
                "delayed_trains": group["is_delayed"].sum()
            }
    
    return aggregations


def finalize_aggregations(aggregations: Dict) -> Dict[str, pd.DataFrame]:
    """
    Convert aggregation dictionaries to DataFrames.
    
    Args:
        aggregations: Aggregation state dictionary
        
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of final aggregated DataFrames
    """
    result = {}
    
    # Finalize delay categories
    delay_cat_data = []
    station_totals = {}
    
    for (station, delay_cat), count in aggregations["delay_categories"].items():
        delay_cat_data.append({
            "station_name": station,
            "DELAY_CAT": delay_cat,
            "count": count
        })
        
        if station in station_totals:
            station_totals[station] += count
        else:
            station_totals[station] = count
    
    delay_cats_df = pd.DataFrame(delay_cat_data)
    
    if not delay_cats_df.empty:
        # Add total and percentage columns
        delay_cats_df["total"] = delay_cats_df["station_name"].map(station_totals)
        delay_cats_df["percentage"] = 100 * delay_cats_df["count"] / delay_cats_df["total"]
        result["delay_categories"] = delay_cats_df
    else:
        result["delay_categories"] = pd.DataFrame(columns=["station_name", "DELAY_CAT", "count", "total", "percentage"])
    
    # Finalize train categories
    train_cat_data = []
    
    for cat, values in aggregations["train_categories"].items():
        avg_delay = values["sum"] / values["count"] if values["count"] > 0 else 0
        train_cat_data.append({
            "train_category": cat,
            "DELAY": avg_delay
        })
    
    result["train_categories"] = pd.DataFrame(train_cat_data).sort_values(by="DELAY", ascending=False)
    
    # Finalize weekday heatmap
    weekday_data = []
    
    for (station, day), values in aggregations["weekday_heatmap"].items():
        pct_delayed = 100 * values["delayed"] / values["total"] if values["total"] > 0 else 0
        weekday_data.append({
            "station_name": station,
            "day_of_week": day,
            "total": values["total"],
            "delayed": values["delayed"],
            "pct_delayed": pct_delayed
        })
    
    result["weekday_heatmap"] = pd.DataFrame(weekday_data)
    
    # Finalize hourly lineplot
    hourly_data = []
    
    for (hour, station), values in aggregations["hourly_lineplot"].items():
        pct_delayed = 100 * values["delayed"] / values["total"] if values["total"] > 0 else 0
        hourly_data.append({
            "hour": hour,
            "station_name": station,
            "total": values["total"],
            "delayed": values["delayed"],
            "pct_delayed": pct_delayed
        })
    
    result["hourly_lineplot"] = pd.DataFrame(hourly_data)
    
    # Finalize station summary
    station_data = []
    
    for station, values in aggregations["station_summary"].items():
        avg_delay = values["delay_sum"] / values["total_trains"] if values["total_trains"] > 0 else 0
        pct_delayed = 100 * values["delayed_trains"] / values["total_trains"] if values["total_trains"] > 0 else 0
        station_data.append({
            "station_name": station,
            "avg_delay": avg_delay,
            "total_trains": values["total_trains"],
            "delayed_trains": values["delayed_trains"],
            "pct_delayed": pct_delayed
        })
    
    result["station_summary"] = pd.DataFrame(station_data)
    
    return result


def stream_and_process_data() -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Stream and process data incrementally, without storing to disk.
    
    Returns:
        Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]: Sample DataFrame for visualizations and aggregated data
    """
    logger.info("Stream processing train delay data...")
    
    # For storing a representative sample
    sample_df = pd.DataFrame()
    station_samples = {station: pd.DataFrame() for station in TARGET_STATIONS}
    
    # For incremental aggregation
    aggregations = {}
    
    try:
        # Process each data URL
        for url_idx, url in enumerate(DATA_URLS):
            logger.info(f"Processing URL {url_idx+1}/{len(DATA_URLS)}: {url}")
            
            # Stream and process chunks
            for chunk_idx, chunk in enumerate(stream_data_in_chunks(url)):
                processed_chunk = process_chunk(chunk)
                
                if processed_chunk.empty:
                    continue
                
                # Update aggregations
                aggregations = incrementally_aggregate_data(processed_chunk, aggregations)
                
                # Update station samples
                for station in TARGET_STATIONS:
                    station_chunk = processed_chunk[processed_chunk["station_name"] == station]
                    if not station_chunk.empty:
                        current_sample = station_samples[station]
                        combined = pd.concat([current_sample, station_chunk])
                        
                        # If we have more than maximum rows, sample to maintain distribution
                        if len(combined) > MAX_ROWS_PER_STATION:
                            # Maintain distribution by delay category
                            station_samples[station] = combined.groupby("DELAY_CAT", group_keys=False).apply(
                                lambda x: x.sample(
                                    min(len(x), int(MAX_ROWS_PER_STATION * len(x) / len(combined))),
                                    random_state=42
                                )
                            )
                        else:
                            station_samples[station] = combined
                
                # Log progress
                if chunk_idx % 10 == 0:
                    logger.info(f"Processed {chunk_idx+1} chunks from URL {url_idx+1}")
        
        # Combine station samples into final sample DataFrame
        sample_df = pd.concat([df for df in station_samples.values()])
        
        # Finalize aggregations
        final_aggregations = finalize_aggregations(aggregations)
        
        logger.info(f"Completed stream processing with {len(sample_df)} sample rows")
        
        # Store in cache
        global _AGGREGATED_DATA_CACHE
        _AGGREGATED_DATA_CACHE = final_aggregations
        
        return sample_df, final_aggregations
        
    except Exception as e:
        logger.error(f"Error in stream_and_process_data: {e}")
        raise


@lru_cache(maxsize=1)
def load_and_prepare_data() -> pd.DataFrame:
    """
    Load and prepare the data for visualization. Results are cached to improve performance.
    
    Returns:
        pd.DataFrame: Prepared DataFrame (sample for visualizations)
    """
    try:
        # Process data streams and get the sample DataFrame and aggregations
        sample_df, _ = stream_and_process_data()
        
        # Log sample information
        logger.info(f"Sample data shape: {sample_df.shape}")
        stations_in_sample = sample_df["station_name"].unique()
        logger.info(f"Stations in sample data: {stations_in_sample}")
        
        return sample_df
        
    except Exception as e:
        logger.error(f"Error loading or preparing data: {e}")
        raise


def get_pre_aggregated_data() -> Dict[str, pd.DataFrame]:
    """
    Get pre-aggregated data for all visualizations to improve performance.
    
    Returns:
        Dict[str, pd.DataFrame]: Dictionary of pre-aggregated datasets
    """
    global _AGGREGATED_DATA_CACHE
    
    # Check if cache is already populated
    if not _AGGREGATED_DATA_CACHE:
        # If not, we need to process the data
        _, _AGGREGATED_DATA_CACHE = stream_and_process_data()
    
    return _AGGREGATED_DATA_CACHE


def get_delay_category_data() -> pd.DataFrame:
    """
    Process data for the delay category visualizations.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    return aggregated_data["delay_categories"]


def get_category_delay_data() -> pd.DataFrame:
    """
    Process data for the train category delay visualizations.
    
    Returns:
        pd.DataFrame: Processed data for visualization
    """
    aggregated_data = get_pre_aggregated_data()
    return aggregated_data["train_categories"]


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