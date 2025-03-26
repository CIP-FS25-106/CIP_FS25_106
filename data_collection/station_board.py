"""
station_board.py - Module for collecting and processing station board data

This module handles retrieving, processing, and saving data about
departures and arrivals at train stations.
"""

import os
import csv
import logging
from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Optional
from pathlib import Path

# Import from the same package
from data_collection.api_client import get_station_board

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("station_board.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Stations to focus on initially
TARGET_STATIONS = {
    "Luzern": "8505000",
    "Zürich HB": "8503000",
    "Genève": "8501008"
}


def process_stationboard_entry(entry: Dict) -> Dict:
    """
    Process a single stationboard entry to extract relevant information.
    
    Args:
        entry: Raw stationboard entry from the API
        
    Returns:
        Dict: Processed entry with only the relevant fields
    """
    # Extract stop data
    stop = entry.get('stop', {})
    station = stop.get('station', {})
    
    # Check if departure and arrival times exist
    departure_timestamp = stop.get('departure', None)
    scheduled_departure = None
    if departure_timestamp:
        scheduled_departure = datetime.fromisoformat(departure_timestamp.replace('Z', '+00:00'))
    
    arrival_timestamp = stop.get('arrival', None)
    scheduled_arrival = None
    if arrival_timestamp:
        scheduled_arrival = datetime.fromisoformat(arrival_timestamp.replace('Z', '+00:00'))
    
    # Process delay information
    departure_delay = stop.get('departureDelay', None)
    if departure_delay:
        try:
            departure_delay = int(departure_delay)
        except (ValueError, TypeError):
            departure_delay = None
    
    arrival_delay = stop.get('arrivalDelay', None)
    if arrival_delay:
        try:
            arrival_delay = int(arrival_delay)
        except (ValueError, TypeError):
            arrival_delay = None
    
    # Create processed entry
    processed_entry = {
        'collection_date': datetime.now().strftime('%Y-%m-%d'),
        'collection_time': datetime.now().strftime('%H:%M:%S'),
        'station_id': station.get('id', ''),
        'station_name': station.get('name', ''),
        'train_category': entry.get('category', ''),
        'train_number': entry.get('number', ''),
        'train_operator': entry.get('operator', ''),
        'train_to': entry.get('to', ''),
        'scheduled_departure': scheduled_departure.strftime('%Y-%m-%d %H:%M:%S') if scheduled_departure else None,
        'scheduled_arrival': scheduled_arrival.strftime('%Y-%m-%d %H:%M:%S') if scheduled_arrival else None,
        'departure_delay': departure_delay,
        'arrival_delay': arrival_delay,
        'platform': stop.get('platform', ''),
        'departure_prognosis': stop.get('departureTimestamp', None),
        'arrival_prognosis': stop.get('arrivalTimestamp', None),
    }
    
    return processed_entry


def collect_station_data(station_name: str, station_id: str, date: Optional[str] = None, 
                        time_window_days: int = 1, data_dir: str = "data/raw") -> str:
    """
    Collect station board data for a given station over a time period.
    
    Args:
        station_name: Name of the station
        station_id: ID of the station
        date: Starting date in format YYYY-MM-DD
        time_window_days: Number of days to collect data for
        data_dir: Directory to save data
        
    Returns:
        str: Path to the saved CSV file
    """
    if date is None:
        # Default to today
        date = datetime.now().strftime('%Y-%m-%d')
    
    # Create date object and determine month
    start_date = datetime.strptime(date, '%Y-%m-%d')
    month_str = start_date.strftime('%Y-%m')
    
    # Create folder structure
    month_dir = os.path.join(data_dir, month_str)
    os.makedirs(month_dir, exist_ok=True)
    
    # Define output file path
    filename = f"{station_name.replace(' ', '_')}_{start_date.strftime('%Y-%m-%d')}.csv"
    output_path = os.path.join(month_dir, filename)
    
    all_entries = []
    
    # Collect data for the specified number of days
    current_date = start_date
    for _ in range(time_window_days):
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Collect departures
        logger.info(f"Collecting departure data for {station_name} on {date_str}")
        departures = get_station_board(station_id, date=date_str, type_="departure")
        for entry in departures:
            processed = process_stationboard_entry(entry)
            processed['board_type'] = 'departure'
            all_entries.append(processed)
        
        # Collect arrivals
        logger.info(f"Collecting arrival data for {station_name} on {date_str}")
        arrivals = get_station_board(station_id, date=date_str, type_="arrival")
        for entry in arrivals:
            processed = process_stationboard_entry(entry)
            processed['board_type'] = 'arrival'
            all_entries.append(processed)
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Save data to CSV
    if all_entries:
        df = pd.DataFrame(all_entries)
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(all_entries)} entries to {output_path}")
        return output_path
    else:
        logger.warning(f"No data collected for {station_name}")
        return ""


def collect_data_for_period(start_date: str, end_date: str, data_dir: str = "data/raw") -> List[str]:
    """
    Collect data for all target stations for a specific period.
    
    Args:
        start_date: Start date in format YYYY-MM-DD
        end_date: End date in format YYYY-MM-DD
        data_dir: Directory to save data
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    # Convert dates to datetime
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate days between start and end
    delta = end - start
    days = delta.days + 1  # Include the end date
    
    output_files = []
    
    # Collect data for each station
    for station_name, station_id in TARGET_STATIONS.items():
        logger.info(f"Collecting data for {station_name} from {start_date} to {end_date}")
        
        # Process data month by month
        current_date = start
        while current_date <= end:
            # Determine the end of the current month or the end date, whichever comes first
            month_end = min(
                datetime(current_date.year, current_date.month, 1) + timedelta(days=32),
                end + timedelta(days=1)
            ).replace(day=1) - timedelta(days=1)
            
            # Calculate days in this segment
            segment_days = (month_end - current_date).days + 1
            
            # Collect data for this month
            file_path = collect_station_data(
                station_name, 
                station_id, 
                current_date.strftime('%Y-%m-%d'),
                segment_days, 
                data_dir
            )
            
            if file_path:
                output_files.append(file_path)
            
            # Move to the next month
            current_date = month_end + timedelta(days=1)
    
    return output_files


def collect_monthly_data(year: int, month: int, data_dir: str = "data/raw") -> List[str]:
    """
    Collect data for all target stations for a specific month.
    
    Args:
        year: Year (e.g., 2025)
        month: Month (1-12)
        data_dir: Directory to save data
        
    Returns:
        List[str]: Paths to saved CSV files
    """
    # Determine start and end dates for the month
    start_date = datetime(year, month, 1)
    
    # Get the last day of the month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    
    return collect_data_for_period(
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d'),
        data_dir
    )


if __name__ == "__main__":
    # Example usage
    # Collect data for January 2025
    files = collect_monthly_data(2025, 1)
    print(f"Collected data saved to {len(files)} files:")