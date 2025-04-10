"""
delay_analysis.py - Module for analyzing train delays from previously collected data

This module handles analyzing delay data from previously collected connection and station board data,
avoiding additional API calls if data is already available.
"""

import os
import glob
import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any

# Import configuration
from config import (
    CONNECTION_PAIRS, TARGET_STATIONS, DEFAULT_RAW_DIR, DEFAULT_ANALYSIS_DIR,
    DEFAULT_TIME_SLOTS, DELAY_THRESHOLD_MINUTES
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("delay_analysis.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_day_time_slots() -> List[str]:
    """
    Generate time slots throughout the day for data collection.
    Returns a list of times in HH:MM format at 2-hour intervals.
    """
    return DEFAULT_TIME_SLOTS


def generate_dates_for_month(year: int, month: int) -> List[str]:
    """
    Generate a list of dates in YYYY-MM-DD format for a given month.
    
    Args:
        year: The year (e.g., 2025)
        month: The month (1-12)
        
    Returns:
        List[str]: List of dates in YYYY-MM-DD format
    """
    # Determine month start date
    start_date = datetime(year, month, 1)
    
    # Determine month end date
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    # Generate all dates in the month
    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    
    return dates


def find_connection_files(year: int, month: int, from_station: str, to_station: str, date: Optional[str] = None, raw_data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Find existing connection CSV files for the specified parameters.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        from_station: Origin station name
        to_station: Destination station name
        date: Specific date to find (optional)
        raw_data_dir: Directory containing the raw data
        
    Returns:
        List[str]: List of matching CSV file paths
    """
    month_str = f"{year}-{month:02d}"
    month_dir = os.path.join(raw_data_dir, month_str)
    
    if not os.path.exists(month_dir):
        logger.warning(f"No data directory found for {month_str}")
        return []
    
    from_name = from_station.replace(' ', '_')
    to_name = to_station.replace(' ', '_')
    
    # Build the pattern to match connection files
    if date:
        pattern = f"connection_{from_name}_to_{to_name}_{date}.csv"
    else:
        pattern = f"connection_{from_name}_to_{to_name}_*.csv"
    
    search_pattern = os.path.join(month_dir, pattern)
    return glob.glob(search_pattern)


def find_station_files(year: int, month: int, station_name: str, date: Optional[str] = None, raw_data_dir: str = DEFAULT_RAW_DIR) -> List[str]:
    """
    Find existing station board CSV files for the specified parameters.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        station_name: Station name
        date: Specific date to find (optional)
        raw_data_dir: Directory containing the raw data
        
    Returns:
        List[str]: List of matching CSV file paths
    """
    month_str = f"{year}-{month:02d}"
    month_dir = os.path.join(raw_data_dir, month_str)
    
    if not os.path.exists(month_dir):
        logger.warning(f"No data directory found for {month_str}")
        return []
    
    station_name_sanitized = station_name.replace(' ', '_')
    
    # Build the pattern to match station files
    if date:
        pattern = f"{station_name_sanitized}_{date}.csv"
    else:
        pattern = f"{station_name_sanitized}_*.csv"
    
    search_pattern = os.path.join(month_dir, pattern)
    return glob.glob(search_pattern)


def load_connection_data(file_path: str) -> pd.DataFrame:
    """
    Load connection data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame with connection data
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error loading connection data from {file_path}: {e}")
        return pd.DataFrame()


def load_station_data(file_path: str) -> pd.DataFrame:
    """
    Load station board data from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        pd.DataFrame: DataFrame with station board data
    """
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logger.error(f"Error loading station data from {file_path}: {e}")
        return pd.DataFrame()


def match_connection_with_station_board(
    connection_row: pd.Series,
    from_station_df: pd.DataFrame,
    to_station_df: pd.DataFrame
) -> Dict:
    """
    Match a connection with corresponding entries in station board DataFrames.
    
    Args:
        connection_row: A row from the connections DataFrame
        from_station_df: DataFrame with station board entries for the origin station
        to_station_df: DataFrame with station board entries for the destination station
        
    Returns:
        Dict: Enhanced connection with matched station board information
    """
    enhanced = connection_row.to_dict()
    
    # Extract train information for the first section
    section_1_category = enhanced.get('section_1_category')
    section_1_number = enhanced.get('section_1_number')
    
    # Find the last section (for multi-leg journeys)
    last_section_idx = 1
    while f'section_{last_section_idx+1}_category' in enhanced:
        last_section_idx += 1
    
    last_section_category = enhanced.get(f'section_{last_section_idx}_category')
    last_section_number = enhanced.get(f'section_{last_section_idx}_number')
    
    # Match with departure station board - filter for departure entries
    departure_df = from_station_df[from_station_df['board_type'] == 'departure']
    matched_departure = None
    
    if len(departure_df) > 0 and section_1_category and section_1_number:
        departure_matches = departure_df[
            (departure_df['train_category'] == section_1_category) & 
            (departure_df['train_number'] == section_1_number)
        ]
        
        if len(departure_matches) > 0:
            # Use the row with the closest scheduled departure time
            if 'departure_datetime' in enhanced and enhanced['departure_datetime']:
                conn_departure = pd.to_datetime(enhanced['departure_datetime'])
                departure_matches['time_diff'] = abs(
                    pd.to_datetime(departure_matches['scheduled_departure']) - conn_departure
                )
                matched_departure = departure_matches.sort_values('time_diff').iloc[0]
            else:
                # If no departure time, just use the first match
                matched_departure = departure_matches.iloc[0]
    
    # Match with arrival station board - filter for arrival entries
    arrival_df = to_station_df[to_station_df['board_type'] == 'arrival']
    matched_arrival = None
    
    if len(arrival_df) > 0 and last_section_category and last_section_number:
        arrival_matches = arrival_df[
            (arrival_df['train_category'] == last_section_category) & 
            (arrival_df['train_number'] == last_section_number)
        ]
        
        if len(arrival_matches) > 0:
            # Use the row with the closest scheduled arrival time
            if 'arrival_datetime' in enhanced and enhanced['arrival_datetime']:
                conn_arrival = pd.to_datetime(enhanced['arrival_datetime'])
                arrival_matches['time_diff'] = abs(
                    pd.to_datetime(arrival_matches['scheduled_arrival']) - conn_arrival
                )
                matched_arrival = arrival_matches.sort_values('time_diff').iloc[0]
            else:
                # If no arrival time, just use the first match
                matched_arrival = arrival_matches.iloc[0]
    
    # Extract station board delays
    if matched_departure is not None:
        enhanced['sb_departure_delay'] = matched_departure.get('departure_delay')
        enhanced['sb_departure_platform'] = matched_departure.get('platform')
    
    if matched_arrival is not None:
        enhanced['sb_arrival_delay'] = matched_arrival.get('arrival_delay')
        enhanced['sb_arrival_platform'] = matched_arrival.get('platform')
    
    # Calculate delay difference (station board vs. connection API)
    if 'departure_delay' in enhanced and 'sb_departure_delay' in enhanced:
        conn_delay = enhanced['departure_delay']
        sb_delay = enhanced['sb_departure_delay']
        if pd.notna(conn_delay) and pd.notna(sb_delay):
            enhanced['departure_delay_diff'] = sb_delay - conn_delay
    
    if 'arrival_delay' in enhanced and 'sb_arrival_delay' in enhanced:
        conn_delay = enhanced['arrival_delay']
        sb_delay = enhanced['sb_arrival_delay']
        if pd.notna(conn_delay) and pd.notna(sb_delay):
            enhanced['arrival_delay_diff'] = sb_delay - conn_delay
    
    # Determine total delay for the journey (how much delay was added during the journey)
    dep_delay = enhanced.get('departure_delay', 0) if pd.notna(enhanced.get('departure_delay', 0)) else 0
    arr_delay = enhanced.get('arrival_delay', 0) if pd.notna(enhanced.get('arrival_delay', 0)) else 0
    enhanced['journey_added_delay'] = arr_delay - dep_delay
    
    return enhanced


def analyze_connections_for_day(
    from_station: str, 
    to_station: str, 
    date: str, 
    raw_data_dir: str = DEFAULT_RAW_DIR,
    output_dir: str = DEFAULT_ANALYSIS_DIR
) -> str:
    """
    Analyze connections and station boards for a specific day and station pair using saved data.
    
    Args:
        from_station: Origin station name
        to_station: Destination station name
        date: Date in format YYYY-MM-DD
        raw_data_dir: Directory where raw data is stored
        output_dir: Directory to save results
        
    Returns:
        str: Path to the saved CSV file
    """
    # Parse date to get year and month
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    year = date_obj.year
    month = date_obj.month
    month_str = date_obj.strftime('%Y-%m')
    
    logger.info(f"Analyzing connections from {from_station} to {to_station} on {date}")
    
    # Find connection files for this day
    connection_files = find_connection_files(year, month, from_station, to_station, date, raw_data_dir)
    if not connection_files:
        logger.warning(f"No connection data found for {from_station} to {to_station} on {date}")
        return ""
    
    # Find station files for both stations on this day
    from_station_files = find_station_files(year, month, from_station, date, raw_data_dir)
    to_station_files = find_station_files(year, month, to_station, date, raw_data_dir)
    
    if not from_station_files:
        logger.warning(f"No station board data found for {from_station} on {date}")
        return ""
    
    if not to_station_files:
        logger.warning(f"No station board data found for {to_station} on {date}")
        return ""
    
    # Load connection data
    connection_df = pd.concat([load_connection_data(file) for file in connection_files])
    
    # Load station board data
    from_station_df = pd.concat([load_station_data(file) for file in from_station_files])
    to_station_df = pd.concat([load_station_data(file) for file in to_station_files])
    
    if len(connection_df) == 0:
        logger.warning(f"No valid connection data found for {from_station} to {to_station} on {date}")
        return ""
    
    if len(from_station_df) == 0 or len(to_station_df) == 0:
        logger.warning(f"No valid station board data found for analysis on {date}")
        return ""
    
    # Process each connection to match with station board data
    enhanced_connections = []
    
    for _, connection in connection_df.iterrows():
        try:
            enhanced = match_connection_with_station_board(
                connection, from_station_df, to_station_df
            )
            enhanced_connections.append(enhanced)
        except Exception as e:
            logger.error(f"Error processing connection {connection.get('collection_time', 'unknown')}: {e}")
            continue
    
    # If we have processed connections, save to CSV
    if enhanced_connections:
        # Create output directory structure
        output_month_dir = os.path.join(output_dir, month_str)
        os.makedirs(output_month_dir, exist_ok=True)
        
        # Create sanitized names for the file
        from_name = from_station.replace(' ', '_')
        to_name = to_station.replace(' ', '_')
        
        # Define output file path
        filename = f"delay_analysis_{from_name}_to_{to_name}_{date}.csv"
        output_path = os.path.join(output_month_dir, filename)
        
        # Save to CSV
        df = pd.DataFrame(enhanced_connections)
        df.to_csv(output_path, index=False)
        
        logger.info(f"Saved {len(enhanced_connections)} analyzed connections to {output_path}")
        return output_path
    else:
        logger.warning(f"No connections could be analyzed for {from_station} to {to_station} on {date}")
        return ""


def analyze_connections_for_month(
    year: int,
    month: int,
    raw_data_dir: str = DEFAULT_RAW_DIR,
    output_dir: str = DEFAULT_ANALYSIS_DIR
) -> List[str]:
    """
    Analyze connections and station boards for all station pairs for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        raw_data_dir: Directory where raw data is stored
        output_dir: Directory to save results
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    # Generate all dates for the month
    dates = generate_dates_for_month(year, month)
    
    output_files = []
    
    # Check if the data directory for this month exists
    month_str = f"{year}-{month:02d}"
    month_dir = os.path.join(raw_data_dir, month_str)
    
    if not os.path.exists(month_dir):
        logger.warning(f"No data directory found for {month_str}. Skipping analysis.")
        return []
    
    # For each station pair, analyze connections for each day
    for from_station, to_station in CONNECTION_PAIRS:
        for date in dates:
            try:
                file_path = analyze_connections_for_day(
                    from_station, to_station, date, raw_data_dir, output_dir
                )
                if file_path:
                    output_files.append(file_path)
            except Exception as e:
                logger.error(f"Error analyzing connections for {from_station} to {to_station} on {date}: {e}")
                # Continue with next pair/date
    
    return output_files


def create_monthly_summary(
    year: int,
    month: int,
    output_dir: str = DEFAULT_ANALYSIS_DIR
) -> str:
    """
    Create a summary of all analyzed connections for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        output_dir: Directory where analysis results are stored
        
    Returns:
        str: Path to the saved CSV file
    """
    # Determine the month directory
    month_str = f"{year}-{month:02d}"
    month_dir = os.path.join(output_dir, month_str)
    
    # Define output summary file
    summary_file = os.path.join(output_dir, f"summary_{month_str}.csv")
    
    # Check if the directory exists
    if not os.path.exists(month_dir):
        logger.warning(f"No analysis data found for {month_str}")
        return ""
    
    # Find all CSV files in the month directory
    csv_files = [os.path.join(month_dir, f) for f in os.listdir(month_dir) if f.endswith('.csv')]
    
    if not csv_files:
        logger.warning(f"No CSV files found in {month_dir}")
        return ""
    
    # Read and concatenate all CSVs
    all_data = []
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            all_data.append(df)
        except Exception as e:
            logger.error(f"Error reading {csv_file}: {e}")
    
    if not all_data:
        logger.warning("No data could be read from CSV files")
        return ""
    
    # Combine all data
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Create summary statistics
    summary = combined_df.groupby(['from_station_name', 'to_station_name']).agg({
        'departure_delay': ['mean', 'median', 'max', 'count'],
        'arrival_delay': ['mean', 'median', 'max', 'count'],
        'journey_added_delay': ['mean', 'median', 'max', 'min'],
        'duration_minutes': ['mean', 'median', 'min', 'max'],
        'transfers': ['mean', 'median', 'min', 'max']
    }).reset_index()
    
    # Flatten the column hierarchy
    summary.columns = ['_'.join(col).strip('_') for col in summary.columns.values]
    
    # Add a calculated field for on-time performance (percentage of trains with minimal delay)
    if 'arrival_delay' in combined_df:
        on_time = combined_df.groupby(['from_station_name', 'to_station_name']).apply(
            lambda x: 100 * (x['arrival_delay'] <= DELAY_THRESHOLD_MINUTES).sum() / len(x)
        ).reset_index(name='on_time_percentage')
        
        summary = pd.merge(summary, on_time, on=['from_station_name', 'to_station_name'])
    
    # Save summary to CSV
    summary.to_csv(summary_file, index=False)
    logger.info(f"Saved monthly summary to {summary_file}")
    
    return summary_file


if __name__ == "__main__":
    # Example usage: Analyze connections for January 2025
    year = 2025
    month = 1
    
    # Analyze connections for the month
    output_files = analyze_connections_for_month(year, month)
    print(f"Analyzed connections saved to {len(output_files)} files.")
    
    # Create monthly summary
    summary_file = create_monthly_summary(year, month)
    if summary_file:
        print(f"Monthly summary saved to {summary_file}")