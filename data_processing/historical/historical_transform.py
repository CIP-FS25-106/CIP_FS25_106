"""
historical_transform.py - Module for transforming historical train data

This module handles reading, processing, and saving historical train arrival data,
including delay calculations and standardizing column names.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("transform_historical.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_project_root() -> Path:
    """
    Get the path to the project root directory.
    
    Returns:
        Path: Project root directory path
    """
    return Path(__file__).parent.parent.parent


def load_historical_data(file_path: Path) -> pd.DataFrame:
    """
    Load historical train data from CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        pd.DataFrame: Loaded data
    """
    logger.info(f"Loading data from {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully loaded {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


def convert_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert arrival time columns to datetime format.
    
    Args:
        df: DataFrame with arrival time columns
        
    Returns:
        pd.DataFrame: DataFrame with converted datetime columns
    """
    logger.info("Converting datetime columns")
    try:
        # Convert to datetime using mixed format, handling missing values and format inconsistencies
        df["ANKUNFTSZEIT_DT"] = pd.to_datetime(
            df["ANKUNFTSZEIT"], 
            format="mixed", 
            dayfirst=True, 
            errors="coerce"
        )
        
        df["AN_PROGNOSE_DT"] = pd.to_datetime(
            df["AN_PROGNOSE"], 
            format="mixed", 
            dayfirst=True, 
            errors="coerce"
        )
        
        # Log summary of conversion success
        scheduled_null = df["ANKUNFTSZEIT_DT"].isnull().sum()
        prognosis_null = df["AN_PROGNOSE_DT"].isnull().sum()
        logger.info(f"Datetime conversion complete. Null values: scheduled={scheduled_null}, prognosis={prognosis_null}")
        
        return df
    except Exception as e:
        logger.error(f"Error converting datetime columns: {e}")
        raise


def calculate_delay(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate delay in minutes between scheduled and actual arrival times.
    
    Args:
        df: DataFrame with datetime columns
        
    Returns:
        pd.DataFrame: DataFrame with added delay column
    """
    logger.info("Calculating delay in minutes")
    try:
        # Compute delay in minutes
        df["DELAY"] = (df["AN_PROGNOSE_DT"] - df["ANKUNFTSZEIT_DT"]).dt.total_seconds() / 60
        
        # Log delay statistics
        delay_avg = df["DELAY"].mean()
        delay_max = df["DELAY"].max()
        logger.info(f"Delay calculation complete. Average delay: {delay_avg:.2f} minutes, Maximum delay: {delay_max:.2f} minutes")
        
        return df
    except Exception as e:
        logger.error(f"Error calculating delay: {e}")
        raise


def categorize_delay(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize delay into defined buckets.
    
    Args:
        df: DataFrame with delay column
        
    Returns:
        pd.DataFrame: DataFrame with added delay category column
    """
    logger.info("Categorizing delay values")
    try:
        # Define conditions for delay categories
        conditions = [
            df["FAELLT_AUS_TF"] == True,
            (df["DELAY"] > 2) & (df["DELAY"] <= 5),
            (df["DELAY"] > 5) & (df["DELAY"] <= 15),
            df["DELAY"] > 15
        ]
        
        # Define category labels
        choices = [
            "Cancelled",
            "2 to 5minutes",
            "5 to 15minutes",
            "more than 15minutes"
        ]
        
        # Apply categorization
        df["DELAY_CAT"] = np.select(conditions, choices, default="On time")
        
        # Log category distribution
        category_counts = df["DELAY_CAT"].value_counts()
        logger.info(f"Delay categorization complete. Distribution: {category_counts.to_dict()}")
        
        return df
    except Exception as e:
        logger.error(f"Error categorizing delay: {e}")
        raise


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename columns to match API column names for easier concatenation.
    
    Args:
        df: DataFrame with original column names
        
    Returns:
        pd.DataFrame: DataFrame with standardized column names
    """
    logger.info("Standardizing column names")
    try:
        # Define mapping from original to new column names
        column_mapping = {
            "BETRIEBSTAG": "ride_day",
            "FAHRT_BEZEICHNER": "ride_description",
            "BETREIBER_ABK": "train_operator",
            "PRODUKT_ID": "product_id",
            "LINIEN_TEXT": "line_text",
            "VERKEHRSMITTEL_TEXT": "train_category",
            "ZUSATZFAHRT_TF": "extra_train",
            "FAELLT_AUS_TF": "cancelled",
            "HALTESTELLEN_NAME": "station_name",
            "ANKUNFTSZEIT_DT": "scheduled_arrival",
            "AN_PROGNOSE_DT": "arrival_prognosis",
            "AN_PROGNOSE_STATUS": "arrival_prognosis_status",
            "DURCHFAHRT_TF": "nostopping"
        }
        
        # Apply column renaming
        df = df.rename(columns=column_mapping)
        
        logger.info(f"Column standardization complete. New columns: {list(df.columns)}")
        return df
    except Exception as e:
        logger.error(f"Error standardizing column names: {e}")
        raise


def clean_and_transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all transformation steps to prepare historical data.
    
    Args:
        df: Raw DataFrame
        
    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    logger.info("Starting data transformation process")
    
    # Apply all transformation steps
    df = convert_datetime_columns(df)
    df = calculate_delay(df)
    df = categorize_delay(df)
    df = standardize_column_names(df)
    
    # Drop original columns that are no longer needed
    df = df.drop(columns=["ANKUNFTSZEIT", "AN_PROGNOSE"])
    
    logger.info(f"Data transformation complete. Final shape: {df.shape}")
    return df


def main():
    """Main function to execute the data transformation process."""
    try:
        # Get project root and define file paths
        project_root = get_project_root()
        input_file = project_root / "data" / "historical" / "processed" / "intermediate_filtered_data.csv"
        output_dir = project_root / "data" / "historical" / "processed"
        output_file = output_dir / "historical_transformed.csv"
        
        # Ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load data
        df = load_historical_data(input_file)
        
        # Transform data
        df = clean_and_transform_data(df)
        
        # Save transformed data
        logger.info(f"Saving transformed data to {output_file}")
        df.to_csv(output_file, index=False, sep=",")
        logger.info(f"Successfully saved {len(df)} records to {output_file}")
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        raise


if __name__ == "__main__":
    main()