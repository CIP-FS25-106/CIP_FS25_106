"""
data_processing.py - Module for loading and preparing train delay data.
Handles the data preprocessing steps for the SBB Train Delays Dashboard.
"""

import pandas as pd
import numpy as np
import logging
import io
import gzip
import requests
from typing import List, Dict, Optional, Tuple, Union

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_processing.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DELAY_THRESHOLD = 2  # Minutes threshold for considering a train delayed
DEFAULT_STATIONS = ["Zürich HB", "Luzern", "Genève"]

def load_historical_data(urls=None, urls_file=None, local_file=None):
    """
    Load historical data from either Cloudinary URLs or a local file.
    
    Args:
        urls: List of Cloudinary URLs to fetch data from
        urls_file: JSON file containing Cloudinary URLs
        local_file: Path to a local CSV file
    
    Returns:
        pandas.DataFrame: Loaded and combined data
    """
    try:
        if local_file:
            logger.info(f"Loading data from local file: {local_file}")
            df = pd.read_csv(local_file)
            logger.info(f"Loaded {len(df)} records from local file")
            return df
            
        if urls is None and urls_file:
            try:
                import json
                with open(urls_file, 'r') as f:
                    urls = json.load(f)
                logger.info(f"Loaded {len(urls)} URLs from {urls_file}")
            except Exception as e:
                logger.error(f"Error loading URLs from file: {e}")
                urls = []
        
        if not urls:
            logger.error("No URLs provided for data loading")
            return pd.DataFrame()
            
        # Load and combine data from all URLs
        dfs = []
        for i, url in enumerate(urls):
            try:
                logger.info(f"Fetching data from URL {i+1}/{len(urls)}")
                response = requests.get(url)
                response.raise_for_status()
                
                # Check if the file is gzipped
                if url.endswith('.gz'):
                    content = gzip.decompress(response.content)
                    df_part = pd.read_csv(io.BytesIO(content))
                else:
                    df_part = pd.read_csv(io.BytesIO(response.content))
                
                dfs.append(df_part)
                logger.info(f"Loaded {len(df_part)} records from URL {i+1}")
            except Exception as e:
                logger.error(f"Error loading data from URL {i+1}: {e}")
        
        if not dfs:
            logger.error("No data was successfully loaded")
            return pd.DataFrame()
            
        # Combine all data parts
        df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined data: {len(df)} records total")
        
        # Standardize column names to match those used in the visualization functions
        column_mapping = {
            'DELAY': 'delay',
            'DELAY_CAT': 'delay_category',
            'station_name': 'station_name',
            'train_category': 'train_category',
            'ride_day': 'ride_day',
            'scheduled_arrival': 'scheduled_arrival',
            'arrival_planned': 'scheduled_arrival'
        }
        
        # Rename columns if they exist
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns and old_col != new_col:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # Ensure we have delay column
        if 'delay' not in df.columns and 'DELAY' in df.columns:
            df.rename(columns={'DELAY': 'delay'}, inplace=True)
            
        # Create the delay_category column if it doesn't exist
        if 'delay_category' not in df.columns and 'delay' in df.columns:
            conditions = [
                (df['delay'] <= DELAY_THRESHOLD),
                (df['delay'] > DELAY_THRESHOLD) & (df['delay'] <= 5),
                (df['delay'] > 5) & (df['delay'] <= 15),
                (df['delay'] > 15)
            ]
            choices = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes']
            df['delay_category'] = np.select(conditions, choices, default='Cancelled')
            
        # Make sure all necessary date columns are datetime objects
        if 'ride_day' in df.columns:
            df['ride_day'] = pd.to_datetime(df['ride_day'], errors='coerce')
            
        if 'scheduled_arrival' in df.columns:
            df['scheduled_arrival'] = pd.to_datetime(df['scheduled_arrival'], errors='coerce')
            
        # Remove extreme negative delays (as done in historical_data_analysis.py)
        if 'delay' in df.columns:
            df_filtered = df[df['delay'] >= -500]
            removed_count = len(df) - len(df_filtered)
            if removed_count > 0:
                logger.info(f"Removed {removed_count} records with extreme negative delays")
            df = df_filtered
            
        return df
    except Exception as e:
        logger.error(f"Error in load_historical_data: {e}")
        return pd.DataFrame()


def filter_data(df, stations=None, categories=None, start_date=None, end_date=None):
    """
    Filter the data based on user-selected criteria.
    
    Args:
        df: Input DataFrame
        stations: List of station names to include
        categories: List of train categories to include
        start_date: Start date for filtering
        end_date: End date for filtering
        
    Returns:
        pandas.DataFrame: Filtered DataFrame
    """
    try:
        if df.empty:
            return df
            
        filtered_df = df.copy()
        
        # Filter by station
        if stations and 'station_name' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['station_name'].isin(stations)]
            logger.info(f"Filtered for stations: {stations}, {len(filtered_df)} records remaining")
        
        # Filter by train category
        if categories and 'train_category' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['train_category'].isin(categories)]
            logger.info(f"Filtered for train categories: {categories}, {len(filtered_df)} records remaining")
        
        # Filter by date range
        date_col = None
        for col in ['ride_day', 'scheduled_arrival', 'arrival_planned']:
            if col in filtered_df.columns:
                date_col = col
                break
                
        if date_col:
            # Ensure the column is datetime
            if not pd.api.types.is_datetime64_any_dtype(filtered_df[date_col]):
                filtered_df[date_col] = pd.to_datetime(filtered_df[date_col], errors='coerce')
            
            # Apply date filters
            if start_date:
                filtered_df = filtered_df[filtered_df[date_col] >= start_date]
                logger.info(f"Filtered for dates >= {start_date}, {len(filtered_df)} records remaining")
                
            if end_date:
                filtered_df = filtered_df[filtered_df[date_col] <= end_date]
                logger.info(f"Filtered for dates <= {end_date}, {len(filtered_df)} records remaining")
        
        return filtered_df
    except Exception as e:
        logger.error(f"Error in filter_data: {e}")
        return df


def calculate_delay_stats(df):
    """
    Calculate key statistics about train delays.
    
    Args:
        df: Input DataFrame
        
    Returns:
        dict: Dictionary containing calculated statistics
    """
    try:
        if df.empty:
            return {
                'total_trains': 0,
                'avg_delay': 0.0,
                'pct_on_time': 0.0,
                'pct_delayed': 0.0
            }
            
        delay_col = 'delay' if 'delay' in df.columns else 'DELAY'
        
        # Make sure we have the column
        if delay_col not in df.columns:
            return {
                'total_trains': len(df),
                'avg_delay': 0.0,
                'pct_on_time': 0.0,
                'pct_delayed': 0.0
            }
            
        # Calculate statistics
        total_trains = len(df)
        avg_delay = df[delay_col].mean()
        
        # Count on-time and delayed trains
        if 'delay_category' in df.columns:
            on_time_count = len(df[df['delay_category'] == 'On time'])
        else:
            on_time_count = len(df[df[delay_col] <= DELAY_THRESHOLD])
            
        delayed_count = total_trains - on_time_count
        
        # Calculate percentages
        pct_on_time = 100 * on_time_count / total_trains if total_trains > 0 else 0
        pct_delayed = 100 * delayed_count / total_trains if total_trains > 0 else 0
        
        return {
            'total_trains': total_trains,
            'avg_delay': avg_delay,
            'pct_on_time': pct_on_time,
            'pct_delayed': pct_delayed
        }
    except Exception as e:
        logger.error(f"Error in calculate_delay_stats: {e}")
        return {
            'total_trains': 0,
            'avg_delay': 0.0,
            'pct_on_time': 0.0,
            'pct_delayed': 0.0
        }


def get_delay_by_time(df, by='hour'):
    """
    Calculate delay statistics by time (hour or day of week).
    
    Args:
        df: Input DataFrame
        by: 'hour' or 'day_of_week' - how to group the data
        
    Returns:
        pandas.DataFrame: DataFrame with delay statistics by time
    """
    try:
        if df.empty:
            return pd.DataFrame()
            
        # Make sure we have the necessary columns
        delay_col = 'delay' if 'delay' in df.columns else 'DELAY'
        if delay_col not in df.columns:
            return pd.DataFrame()
            
        time_df = df.copy()
        
        # Create time columns if needed
        if by == 'hour':
            for col in ['scheduled_arrival', 'arrival_planned']:
                if col in time_df.columns:
                    time_df['hour'] = pd.to_datetime(time_df[col]).dt.hour
                    break
                    
            if 'hour' not in time_df.columns:
                return pd.DataFrame()
                
            group_col = 'hour'
        else:  # day_of_week
            for col in ['ride_day', 'scheduled_arrival', 'arrival_planned']:
                if col in time_df.columns:
                    time_df['day_of_week'] = pd.to_datetime(time_df[col]).dt.day_name()
                    break
                    
            if 'day_of_week' not in time_df.columns:
                return pd.DataFrame()
                
            # Order the days of the week
            weekday_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            time_df['day_of_week'] = pd.Categorical(time_df['day_of_week'], categories=weekday_order, ordered=True)
            
            group_col = 'day_of_week'
        
        # Define delayed trains
        time_df['is_delayed'] = time_df[delay_col] > DELAY_THRESHOLD
        
        # Group and calculate statistics
        if 'station_name' in time_df.columns:
            delay_by_time = time_df.groupby([group_col, 'station_name']).agg(
                total=('is_delayed', 'count'),
                delayed=('is_delayed', 'sum')
            ).reset_index()
        else:
            delay_by_time = time_df.groupby([group_col]).agg(
                total=('is_delayed', 'count'),
                delayed=('is_delayed', 'sum')
            ).reset_index()
        
        # Calculate percentage
        delay_by_time['pct_delayed'] = 100 * delay_by_time['delayed'] / delay_by_time['total']
        
        return delay_by_time
    except Exception as e:
        logger.error(f"Error in get_delay_by_time: {e}")
        return pd.DataFrame()


def get_delay_by_station_and_category(df):
    """
    Calculate average delay by station and train category.
    
    Args:
        df: Input DataFrame
        
    Returns:
        pandas.DataFrame: DataFrame with delay statistics by station and category
    """
    try:
        if df.empty:
            return pd.DataFrame()
            
        # Make sure we have the necessary columns
        delay_col = 'delay' if 'delay' in df.columns else 'DELAY'
        if delay_col not in df.columns or 'station_name' not in df.columns or 'train_category' not in df.columns:
            return pd.DataFrame()
            
        # Calculate average delay by station and category
        avg_delay = df.groupby(['station_name', 'train_category'])[delay_col].agg(['mean', 'count']).reset_index()
        avg_delay.columns = ['station_name', 'train_category', 'avg_delay', 'count']
        
        return avg_delay
    except Exception as e:
        logger.error(f"Error in get_delay_by_station_and_category: {e}")
        return pd.DataFrame()


def get_delay_categories_distribution(df):
    """
    Calculate the distribution of trains across delay categories.
    
    Args:
        df: Input DataFrame
        
    Returns:
        pandas.DataFrame: DataFrame with counts and percentages for each delay category
    """
    try:
        if df.empty:
            return pd.DataFrame()
            
        # Check if we have delay_category column
        if 'delay_category' in df.columns:
            delay_cat_col = 'delay_category'
        elif 'DELAY_CAT' in df.columns:
            delay_cat_col = 'DELAY_CAT'
        else:
            # Create delay category based on delay
            delay_col = 'delay' if 'delay' in df.columns else 'DELAY'
            if delay_col not in df.columns:
                return pd.DataFrame()
                
            conditions = [
                (df[delay_col] <= DELAY_THRESHOLD),
                (df[delay_col] > DELAY_THRESHOLD) & (df[delay_col] <= 5),
                (df[delay_col] > 5) & (df[delay_col] <= 15),
                (df[delay_col] > 15)
            ]
            choices = ['On time', '2 to 5minutes', '5 to 15minutes', 'more than 15minutes']
            df['delay_category'] = np.select(conditions, choices, default='Cancelled')
            delay_cat_col = 'delay_category'
        
        # Count occurrences for each category
        category_counts = df[delay_cat_col].value_counts().reset_index()
        category_counts.columns = ['category', 'count']
        
        # Calculate percentages
        total = category_counts['count'].sum()
        category_counts['percentage'] = 100 * category_counts['count'] / total if total > 0 else 0
        
        # Ensure categories are in expected order
        category_order = ["On time", "2 to 5minutes", "5 to 15minutes", "more than 15minutes", "Cancelled"]
        category_counts['category'] = pd.Categorical(category_counts['category'], categories=category_order, ordered=True)
        category_counts = category_counts.sort_values('category')
        
        return category_counts
    except Exception as e:
        logger.error(f"Error in get_delay_categories_distribution: {e}")
        return pd.DataFrame()