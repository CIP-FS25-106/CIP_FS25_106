"""
historical_df_intermediate.py - Module for filtering and processing historical train data

This module handles reading, filtering, and processing historical train data
from multiple years and creates an intermediate filtered dataset.
"""

import pandas as pd
import os
import glob
import logging
from pathlib import Path
from typing import List, Dict, Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("historical_df_intermediate.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Define constants
CHUNK_SIZE = 100000  # Size of chunks for processing large CSV files


def get_project_root() -> Path:
    """
    Get the path to the project root directory.
    
    Returns:
        Path: Project root directory path
    """
    return Path(__file__).parent.parent.parent


def get_columns_to_keep() -> List[str]:
    """
    Define columns to keep in the filtered dataset.
    
    Returns:
        List[str]: Column names to retain
    """
    return [
        "BETRIEBSTAG", 
        "FAHRT_BEZEICHNER",
        "BETREIBER_ABK",
        "PRODUKT_ID", 
        "LINIEN_TEXT",
        "VERKEHRSMITTEL_TEXT",
        "ZUSATZFAHRT_TF",
        "FAELLT_AUS_TF",
        "HALTESTELLEN_NAME",
        "ANKUNFTSZEIT",
        "AN_PROGNOSE",
        "AN_PROGNOSE_STATUS",
        "DURCHFAHRT_TF"
    ]


def get_target_stations() -> List[str]:
    """
    Define target stations to filter for.
    
    Returns:
        List[str]: Names of stations to include
    """
    return ["Zürich HB", "Luzern", "Genève"]


def ensure_output_directory(output_dir: Path) -> None:
    """
    Ensure the output directory exists.
    
    Args:
        output_dir: Path to the output directory
    """
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Output directory confirmed: {output_dir}")


def get_csv_files_for_year(year: str, project_root: Path) -> List[str]:
    """
    Get all CSV files for a specific year.
    
    Args:
        year: Year as string
        project_root: Project root path
        
    Returns:
        List[str]: List of CSV file paths
    """
    folder_path = project_root / "data" / "historical" / f"downloads_{year}"
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files for year {year}")
    return csv_files


def filter_chunk(chunk: pd.DataFrame, stations: List[str]) -> pd.DataFrame:
    """
    Filter a chunk of data based on specified criteria.
    
    Args:
        chunk: DataFrame chunk to filter
        stations: List of station names to include
        
    Returns:
        pd.DataFrame: Filtered chunk
    """
    filtered_chunk = chunk[
        (chunk["BETREIBER_ABK"] == "SBB") &
        (chunk["PRODUKT_ID"] == "Zug") &
        (chunk["ZUSATZFAHRT_TF"] == False) &
        (chunk["DURCHFAHRT_TF"] == False) &
        (chunk["HALTESTELLEN_NAME"].isin(stations)) &
        (chunk["AN_PROGNOSE_STATUS"].notna())
    ]
    
    return filtered_chunk


def process_file(file_path: str, columns_to_keep: List[str], stations: List[str]) -> List[pd.DataFrame]:
    """
    Process a single CSV file and return filtered chunks.
    
    Args:
        file_path: Path to CSV file
        columns_to_keep: List of columns to retain
        stations: List of station names to include
        
    Returns:
        List[pd.DataFrame]: List of filtered data chunks
    """
    logger.info(f"Processing file: {os.path.basename(file_path)}")
    
    filtered_chunks = []
    
    try:
        # Create a CSV reader that processes in chunks
        csv_reader = pd.read_csv(
            file_path,
            sep=";",
            chunksize=CHUNK_SIZE,  # Process in chunks for large files
            usecols=columns_to_keep,
            low_memory=False
        )
        
        # Process each chunk
        for i, chunk in enumerate(csv_reader):
            filtered_chunk = filter_chunk(chunk, stations)
            
            if not filtered_chunk.empty:
                filtered_chunks.append(filtered_chunk)
                
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1} chunks from {os.path.basename(file_path)}")
        
        logger.info(f"Finished processing {os.path.basename(file_path)}")
        return filtered_chunks
    
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return []


def process_year_data(year: str, project_root: Path, columns_to_keep: List[str], 
                     stations: List[str]) -> List[pd.DataFrame]:
    """
    Process all CSV files for a specific year.
    
    Args:
        year: Year as string
        project_root: Project root path
        columns_to_keep: List of columns to retain
        stations: List of station names to include
        
    Returns:
        List[pd.DataFrame]: List of filtered data chunks from all files
    """
    logger.info(f"Processing data for year {year}...")
    
    csv_files = get_csv_files_for_year(year, project_root)
    year_chunks = []
    
    for file_path in csv_files:
        file_chunks = process_file(file_path, columns_to_keep, stations)
        year_chunks.extend(file_chunks)
    
    logger.info(f"Completed processing for year {year}, collected {len(year_chunks)} filtered chunks")
    return year_chunks


def combine_and_save_data(all_filtered_chunks: List[pd.DataFrame], output_file: Path) -> None:
    """
    Combine all filtered chunks and save to CSV.
    
    Args:
        all_filtered_chunks: List of filtered DataFrame chunks
        output_file: Path to output file
    """
    if not all_filtered_chunks:
        logger.warning("No filtered data to save")
        return
    
    logger.info("Combining all filtered chunks...")
    
    try:
        combined = pd.concat(all_filtered_chunks, ignore_index=True)
        logger.info(f"Combined data shape: {combined.shape}")
        
        combined.to_csv(output_file, index=False, sep=",")
        logger.info(f"Filtered data saved to {output_file}")
    
    except Exception as e:
        logger.error(f"Error combining and saving filtered data: {e}")


def main():
    """Main function to execute the data filtering process."""
    try:
        # Get project root and define paths
        project_root = get_project_root()
        
        # Get columns to keep and target stations
        columns_to_keep = get_columns_to_keep()
        stations = get_target_stations()
        
        # Define output directory and ensure it exists
        output_dir = project_root / "data" / "historical" / "processed"
        ensure_output_directory(output_dir)
        
        # Define years to process
        years = ["2022", "2023", "2024"]
        
        # Initialize list to store filtered chunks
        all_filtered_chunks = []
        
        # Process each year
        for year in years:
            year_chunks = process_year_data(year, project_root, columns_to_keep, stations)
            all_filtered_chunks.extend(year_chunks)
        
        # Combine and save filtered data
        intermediate_file = output_dir / "intermediate_filtered_data.csv"
        combine_and_save_data(all_filtered_chunks, intermediate_file)
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
